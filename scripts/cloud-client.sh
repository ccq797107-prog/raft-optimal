#!/usr/bin/env bash
# 云上版 docker/client.sh：SSH 到对应地域的机器，用已部署的 cdraft-client
# 在那台机器上发请求，让请求真正从那个域发起（付出真实跨地域 RTT）。
#
#   ./scripts/cloud-client.sh <a|b|c|d> read  [count] [key]
#   ./scripts/cloud-client.sh <a|b|c|d> write [count] [key] [value]
#   ./scripts/cloud-client.sh gl                # 查看当前 Global Leader
#
# 例子:
#   ./scripts/cloud-client.sh a write 100         # 从北京发 100 次写 (key=foo value=bar)
#   ./scripts/cloud-client.sh b read 10           # 从上海发 10 次读
#   ./scripts/cloud-client.sh c write 5 mykey hi  # 从广州写 5 次 mykey=hi
#   ./scripts/cloud-client.sh d write 10          # 从贵阳(漂流域)发 10 次写
#
# 域 -> 地域: a=北京 b=上海 c=广州（共识域）, d=贵阳（漂流域，只有 client）
#
# 漂流域 d 没有本地共识节点，流程与 a/b/c 不同：先向集群发现拓扑并上报到各共识域
# 的实测延迟（-report-floating-latency），GL 才能据此挑选最近的 responder 域做
# Fast Return；写请求直接打 GL 的 EIP，并把 -reply-route 指到贵阳自己的 EIP:9100，
# GL 选中的 responder DL 才能从远端回调把 Fast Return 结果送回贵阳。
#
# hairpin 规避：云主机访问自己机器的 EIP 通常不通（NAT 不回环）。所以先问本机
# 节点 GL 在哪：GL 在本域（同机）就直接打 GL 的本地端口，避免被重定向到自己
# 的 EIP；GL 在别的域则正常打本域节点，让它重定向到远端 EIP（可达）。
#
# 可用环境变量覆盖: SSH_USER=root  SSH_KEY=~/.ssh/id_rsa
set -euo pipefail
cd "$(dirname "$0")/.."

SSH_USER="${SSH_USER:-root}"
SSH_KEY="${SSH_KEY:-}"
REMOTE_DIR=/opt/cd-raft

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10)
[ -n "$SSH_KEY" ] && SSH_OPTS+=(-i "$SSH_KEY")

eip() {
  case "$1" in
    a) echo 1.92.115.7 ;;     # 北京
    b) echo 123.60.74.154 ;;  # 上海
    c) echo 110.41.85.62 ;;   # 广州
    d) echo 101.245.65.31 ;;  # 贵阳（漂流域）
    *) echo "未知域 $1" >&2; exit 2 ;;
  esac
}

# 漂流域 d 的私网/回调 EIP，写请求的 -reply-route 用它（GL responder 远端回调）。
FLOATING_REPLY_EIP=101.245.65.31

usage() {
  echo "usage: $0 <a|b|c|d> <read|write> [count] [key] [value]" >&2
  echo "       $0 gl" >&2
  exit 2
}

# 在某域机器上问本机任一活着的节点要 status（依次试 7101/7102/7103）
status_from() {
  local m=$1 port
  for port in 7101 7102 7103; do
    out=$(ssh "${SSH_OPTS[@]}" "$SSH_USER@$(eip "$m")" \
      "$REMOTE_DIR/cdraft-client-linux -status -target 127.0.0.1:$port -timeout 3s" 2>/dev/null) || continue
    [ -n "$out" ] && { echo "$out"; return 0; }
  done
  return 1
}

gl_of() { # 从 status 输出解析 globalLeader= 节点 id（如 b2）
  tr ' ' '\n' | awk -F= '/^globalLeader=/{print $2}'
}

# 问 a/b/c 任一机器要 GL 节点 id，换算成 "EIP:端口"（漂流域 d 从远端直打 GL 用）。
# 节点 id 形如 b2 -> 域字母 b、端口 710 + 2 = 7102、EIP = eip(b)。
gl_endpoint() {
  local m gl letter num
  for m in a b c; do
    out=$(status_from "$m" 2>/dev/null) || continue
    gl=$(echo "$out" | gl_of)
    [ -n "$gl" ] && [ "$gl" != "-" ] || continue
    letter=${gl%%[0-9]*}
    num=${gl#?}
    echo "$(eip "$letter"):710${num}"
    return 0
  done
  return 1
}

region="${1:-}"

if [ "$region" = "gl" ]; then
  for m in a b c; do
    out=$(status_from "$m") || continue
    echo "$out"
    gl=$(echo "$out" | gl_of)
    [ -n "$gl" ] && [ "$gl" != "-" ] && echo "Global Leader: $gl (domain-${gl%%[0-9]*})"
    exit 0
  done
  echo "没有节点应答（集群没起来？）" >&2
  exit 1
fi

mode="${2:-}"
count="${3:-1}"
key="${4:-foo}"
value="${5:-bar}"

case "$region" in a|b|c|d) ;; *) usage ;; esac
case "$mode" in read|write) ;; *) usage ;; esac

# 漂流域 d：没有本地共识节点，单独走"发现 GL -> 上报延迟 -> 直打 GL EIP"流程。
if [ "$region" = "d" ]; then
  gl_ep=$(gl_endpoint) || { echo "找不到 Global Leader（共识集群没起来？）" >&2; exit 1; }
  echo "(漂流域 d=贵阳 -> Global Leader $gl_ep)"
  d_host="$SSH_USER@$(eip d)"

  case "$mode" in
    read)
      ssh "${SSH_OPTS[@]}" "$d_host" \
        "$REMOTE_DIR/cdraft-client-linux \
          -target $gl_ep -origin-domain domain-d -key $key -count $count -timeout 15s"
      ;;
    write)
      # 上报延迟 + 写在【同一个 SSH 会话】里完成：漂流域 telemetry 有 TTL(默认 5s)，
      # 且若分成两次 SSH，第二次握手偶发 "Connection closed port 22" 会导致 GL 没拿到
      # 新 telemetry → 不选 responder → 整批回退成 global-leader。合并后保证 telemetry 新鲜。
      ssh "${SSH_OPTS[@]}" "$d_host" "
        $REMOTE_DIR/cdraft-client-linux -report-floating-latency \
          -target $gl_ep -origin-domain domain-d -timeout 10s \
          || echo '(上报延迟失败，仍尝试写——可能回退成 global-leader)'
        $REMOTE_DIR/cdraft-client-linux \
          -target $gl_ep -origin-domain domain-d -key $key -value $value -count $count \
          -timeout 15s -request-id cli-d-\$(date +%s) \
          -callback-listen 0.0.0.0:9100 -reply-route $FLOATING_REPLY_EIP:9100
      "
      ;;
  esac
  exit 0
fi

# hairpin 规避：GL 与客户端同机时直接打 GL 的本地端口
target=127.0.0.1:7101
if out=$(status_from "$region"); then
  gl=$(echo "$out" | gl_of)
  if [ -n "$gl" ] && [ "$gl" != "-" ] && [ "${gl%%[0-9]*}" = "$region" ]; then
    target="127.0.0.1:710${gl#?}"
  fi
fi

common="-target $target -origin-domain domain-$region -key $key -count $count -timeout 12s"

case "$mode" in
  read)
    ssh "${SSH_OPTS[@]}" "$SSH_USER@$(eip "$region")" \
      "$REMOTE_DIR/cdraft-client-linux $common"
    ;;
  write)
    ssh "${SSH_OPTS[@]}" "$SSH_USER@$(eip "$region")" \
      "$REMOTE_DIR/cdraft-client-linux $common \
        -value $value -request-id cli-\$(date +%s) \
        -callback-listen 0.0.0.0:9100 -reply-route 127.0.0.1:9100"
    ;;
esac
