#!/usr/bin/env bash
# 云上版 docker/gl.sh：查看 / 迁移 Global Leader（华为云，本机走 EIP + GL-only Move RPC）。
#
#   ./scripts/cloud-gl.sh show            # 打印当前 Global Leader 节点 + 域
#   ./scripts/cloud-gl.sh move <a|b|c>    # 通过 Move RPC 安全交接 GL 到该共识域
#
# 与 docker/gl.sh 的区别：
#   - docker/gl.sh 靠"驱逐当前 GL 域 → 剩余两域重选 → 拉回"，会停启节点；
#   - 本脚本在本机直接通过各域 EIP 访问节点（与 cdraft-mover 同款），move 走
#     GL-only 的 Move RPC 做安全交接（catch-up + 高任期选举 + fencing），不停任何
#     节点、不 SSH、无停机。
#
# 前提：各域 EIP 的客户端端口（默认 7101）对本机可达（安全组放通），与 cdraft-mover
# 在本机直连 EIP 的用法一致。
#
# 可用环境变量覆盖：CLIENT_BIN=bin/cdraft-client  PORT=7101
set -euo pipefail
cd "$(dirname "$0")/.."

PORT="${PORT:-7101}"
CLIENT_BIN="${CLIENT_BIN:-bin/cdraft-client}"

eip() { # 共识域 -> 弹性公网 IP
  case "$1" in
    a) echo 1.92.115.7 ;;     # 北京
    b) echo 123.60.74.154 ;;  # 上海
    c) echo 110.41.85.62 ;;   # 广州
    *) echo "未知共识域 $1（只支持 a|b|c）" >&2; exit 2 ;;
  esac
}

# 没有现成二进制就地构建（仅本机用，本机架构即可）。
ensure_bin() {
  [ -x "$CLIENT_BIN" ] && return 0
  echo "==> 构建 $CLIENT_BIN" >&2
  mkdir -p "$(dirname "$CLIENT_BIN")"
  go build -o "$CLIENT_BIN" ./cmd/cdraft-client
}

# 依次拿各域 EIP 当种子，发现当前 GL（-discover-topology 直接给出 GL 地址/域）。
discover() {
  local m out
  for m in a b c; do
    out=$("$CLIENT_BIN" -discover-topology -target "$(eip "$m"):$PORT" \
      -origin-domain "domain-$m" -timeout 5s 2>/dev/null) || continue
    echo "$out" | grep -q '^globalLeader=' && { echo "$out" | head -1; return 0; }
  done
  return 1
}

cmd="${1:-}"
ensure_bin

case "$cmd" in
  show)
    line=$(discover) || { echo "没有节点应答（集群没起来 / EIP:$PORT 不可达？）" >&2; exit 1; }
    echo "$line"
    ;;
  move)
    target="${2:-}"
    case "$target" in a|b|c) ;; *) echo "usage: $0 move <a|b|c>" >&2; exit 2 ;; esac
    # 任一可达种子即可：client 内部发现当前 GL，并把 Move 打到 GL 的 EIP。
    for m in a b c; do
      seed="$(eip "$m"):$PORT"
      if out=$("$CLIENT_BIN" -move "domain-$target" -target "$seed" -timeout 25s 2>&1); then
        echo "$out"
        exit 0
      fi
      # 种子可达但 Move 被拒（非超时/连接错误）时无需再换种子重试。
      case "$out" in
        *"move rejected"*|*"already in"*) echo "$out"; exit 1 ;;
      esac
    done
    echo "move 失败：没有可达种子（检查 EIP:$PORT 放通 / 集群状态）。" >&2
    exit 1
    ;;
  *)
    echo "usage: $0 {show | move <a|b|c>}" >&2; exit 2 ;;
esac
