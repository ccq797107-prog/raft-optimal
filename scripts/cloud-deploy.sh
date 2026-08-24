#!/usr/bin/env bash
# 华为云 3 地域一键部署：北京/上海/广州各 1 台机器，每台跑本域 3 个节点
# （a1-a3 北京、b1-b3 上海、c1-c3 广州），与 docker 拓扑一致。
# 在本地开发机（Mac）上运行，通过 SSH 控制三台云服务器。
#
# 用法:
#   ./scripts/cloud-deploy.sh deploy   # 交叉编译 + 分发二进制和配置到三台机器
#   ./scripts/cloud-deploy.sh start    # 启动 9 个节点（每台机器 3 个, nohup 后台）
#   ./scripts/cloud-deploy.sh stop     # 停止所有节点
#   ./scripts/cloud-deploy.sh restart  # 重启所有节点
#   ./scripts/cloud-deploy.sh status   # 查看进程 + 各节点角色
#   ./scripts/cloud-deploy.sh logs a1  # 跟踪某个节点的日志（a1..c3）
#   ./scripts/cloud-deploy.sh test     # 跨域写 + 读，验证集群
#   ./scripts/cloud-deploy.sh rtt           # 拉取集群实测的跨域 RTT 时延矩阵（a/b/c）
#   ./scripts/cloud-deploy.sh rtt --with-d  # 先触发漂流域 d 上报，再拉矩阵（含 d 行）
#   ./scripts/cloud-deploy.sh clean    # 停止并清空数据目录（回到全新集群）
#   ./scripts/cloud-deploy.sh all      # deploy + start + 等待选举 + test
#
# 可用环境变量覆盖:
#   SSH_USER=root  SSH_KEY=~/.ssh/id_rsa  GOARCH=amd64

set -euo pipefail
cd "$(dirname "$0")/.."

SSH_USER="${SSH_USER:-root}"
SSH_KEY="${SSH_KEY:-}"
GOARCH="${GOARCH:-amd64}"

REMOTE_DIR=/opt/cd-raft
CONFIG=config/cluster.huawei.json

# 机器（域）列表。macOS 自带 bash 3.2 不支持关联数组，所以用 case 查表。
# MACHINES        共识域：每台跑 3 个节点（参与选举/复制）。
# CLIENT_MACHINES 漂流域：只装 client 二进制、不跑共识进程（start/stop/status/clean 都不碰）。
MACHINES=(a b c)
CLIENT_MACHINES=(d)

eip() { # 域 -> 弹性公网 IP（SSH 也走这个）
  case "$1" in
    a) echo 1.92.115.7 ;;     # 北京
    b) echo 123.60.74.154 ;;  # 上海
    c) echo 110.41.85.62 ;;   # 广州
    d) echo 101.245.65.31 ;;  # 贵阳（漂流域，client-only）
    *) echo "未知域 $1" >&2; exit 1 ;;
  esac
}

region_name() {
  case "$1" in
    a) echo 北京 ;; b) echo 上海 ;; c) echo 广州 ;; d) echo 贵阳 ;;
  esac
}

nodes_of() { # 域 -> 该机器上跑的 3 个节点
  echo "${1}1 ${1}2 ${1}3"
}

machine_of() { # 节点 id (a1..c3) -> 域字母
  echo "${1%%[0-9]*}"
}

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10)
[ -n "$SSH_KEY" ] && SSH_OPTS+=(-i "$SSH_KEY")

run() { # run <域字母> <command...>
  local m=$1; shift
  ssh "${SSH_OPTS[@]}" "$SSH_USER@$(eip "$m")" "$@"
}

each() { # 在三台机器上各执行一次
  for m in "${MACHINES[@]}"; do
    echo "--- $(region_name "$m") ($(eip "$m")) ---"
    run "$m" "$@" || echo "[$m] FAILED"
  done
}

cmd_deploy() {
  echo "==> 交叉编译 linux/$GOARCH"
  GOOS=linux GOARCH="$GOARCH" go build -o bin/cdraft-linux "./cmd/cdraft"
  GOOS=linux GOARCH="$GOARCH" go build -o bin/cdraft-client-linux "./cmd/cdraft-client"

  for m in "${MACHINES[@]}"; do
    echo "==> 分发到 $(region_name "$m") ($(eip "$m"))"
    run "$m" "mkdir -p $REMOTE_DIR/data $REMOTE_DIR/logs $REMOTE_DIR/.staging"
    # 先传到 .staging 再 mv 覆盖：直接覆写正在运行的二进制会报
    # "Text file busy"，而 rename 不动旧 inode，正在跑的进程不受影响
    # （注意进程要重启后才会用上新二进制）。
    scp "${SSH_OPTS[@]}" bin/cdraft-linux bin/cdraft-client-linux "$CONFIG" \
      "$SSH_USER@$(eip "$m"):$REMOTE_DIR/.staging/"
    run "$m" "cd $REMOTE_DIR && mv -f .staging/* . && chmod +x cdraft-linux cdraft-client-linux"
  done

  # 漂流域机器只需要 client 二进制（+ 配置，供参考），没有共识进程。
  for m in "${CLIENT_MACHINES[@]:-}"; do
    [ -z "$m" ] && continue
    echo "==> 分发 client 到 $(region_name "$m") ($(eip "$m"))（漂流域，仅 client）"
    run "$m" "mkdir -p $REMOTE_DIR/logs $REMOTE_DIR/.staging"
    scp "${SSH_OPTS[@]}" bin/cdraft-client-linux "$CONFIG" \
      "$SSH_USER@$(eip "$m"):$REMOTE_DIR/.staging/"
    run "$m" "cd $REMOTE_DIR && mv -f .staging/* . && chmod +x cdraft-client-linux"
  done
  echo "==> 部署完成（运行中的节点需 restart 才会使用新二进制/新配置）"
}

cmd_start() {
  for m in "${MACHINES[@]}"; do
    echo "==> 启动 $(region_name "$m") ($(eip "$m")) 节点: $(nodes_of "$m")"
    run "$m" "cd $REMOTE_DIR && for node in $(nodes_of "$m"); do \
      if pgrep -f \"cdraft-linux -config cluster.huawei.json -node \$node \" >/dev/null; then \
        echo \"\$node 已在运行, 跳过\"; \
      else \
        nohup ./cdraft-linux -config cluster.huawei.json -node \$node -data data \
          >> logs/\$node.log 2>&1 & \
        echo \"\$node 已启动\"; \
      fi; \
    done; sleep 1; pgrep -af 'cdraft-linux -config' | wc -l | xargs echo '运行中的节点进程数:'"
  done
}

cmd_stop() {
  each "pkill -f 'cdraft-linux -config' && echo '已停止' || echo '本来就没在运行'"
}

cmd_status() {
  for m in "${MACHINES[@]}"; do
    echo "--- $(region_name "$m") ($(eip "$m")) ---"
    run "$m" "pgrep -af 'cdraft-linux -config' | sed 's|.*-node |node |;s| -data.*||' || echo '未运行'; \
      for node in $(nodes_of "$m"); do \
        port=\$((7100 + \${node#?})); \
        $REMOTE_DIR/cdraft-client-linux -status -target 127.0.0.1:\$port -timeout 3s 2>/dev/null || echo \"\$node: 不可达\"; \
      done" || echo "[$m] FAILED"
  done
}

cmd_logs() {
  local node="${1:?用法: logs <a1|a2|a3|b1|...|c3>}"
  run "$(machine_of "$node")" "tail -f -n 100 $REMOTE_DIR/logs/$node.log"
}

cmd_clean() {
  cmd_stop
  each "rm -rf $REMOTE_DIR/data && mkdir -p $REMOTE_DIR/data && echo '数据已清空'"
}

cmd_test() {
  local key="probe-$(date +%s)"
  echo "==> 1) 从北京(domain-a)写入  key=$key value=hello-from-beijing"
  ./scripts/cloud-client.sh a write 1 "$key" hello-from-beijing
  echo
  echo "==> 2) 从上海(domain-b)读取（验证跨域复制与重定向）"
  ./scripts/cloud-client.sh b read 1 "$key"
  echo
  echo "==> 3) 从广州(domain-c)读取"
  ./scripts/cloud-client.sh c read 1 "$key"
  echo
  echo "==> 跨域读写验证通过"
}

# 拉取集群【自己实测】的跨域 RTT 矩阵：节点用心跳/通告测各域往返时延，GL 视图最全
# （含漂流域近期上报的延迟）。这正是代价模型/Fast Return responder 选择所用的数据。
# 本机直连 EIP 查询（与 cdraft-mover / cloud-gl.sh 同款），无需 SSH、无需先 deploy。
#
# 注意：漂流域 d 的延迟是【易逝】的——只有客户端 -report-floating-latency 上报，GL 端
# 按 floatingTelemetryTtlMillis(默认 5s) 过期。所以单独跑 rtt 时 d 行通常已过期不显示；
# 加 --with-d 会先 SSH 到 d 机器实测上报，再在 5s 内拉矩阵，这样能看到 d 行。
cmd_rtt() {
  local with_d=0
  case "${1:-}" in
    --with-d|-d) with_d=1 ;;
    "") ;;
    *) echo "用法: $0 rtt [--with-d]" >&2; exit 2 ;;
  esac

  local CLIENT_LOCAL="${CLIENT_BIN:-bin/cdraft-client}"
  if [ ! -x "$CLIENT_LOCAL" ]; then
    echo "==> 构建 $CLIENT_LOCAL" >&2
    go build -o "$CLIENT_LOCAL" ./cmd/cdraft-client
  fi

  # 发现当前 GL 的可达地址（-discover-topology 第一行带 address=EIP:port）。
  local gl_addr="" m out cm attempt ok
  for m in "${MACHINES[@]}"; do
    out=$("$CLIENT_LOCAL" -discover-topology -target "$(eip "$m"):7101" \
      -origin-domain "domain-$m" -timeout 5s 2>/dev/null) || continue
    gl_addr=$(echo "$out" | head -1 | tr ' ' '\n' | awk -F= '/^address=/{print $2}')
    [ -n "$gl_addr" ] && [ "$gl_addr" != "-" ] && break
  done
  [ -n "$gl_addr" ] && [ "$gl_addr" != "-" ] || {
    echo "找不到 Global Leader（集群没起来 / EIP:7101 不可达？）" >&2; exit 1; }

  # 可选：先让漂流域从各自机器实测并上报延迟（TTL 内有效），矩阵才能显示其行。
  # 漂流域机器的 sshd 偶发在握手阶段掐断连接（"Connection closed port 22"），重试几次基本能过。
  if [ "$with_d" = 1 ]; then
    for cm in "${CLIENT_MACHINES[@]:-}"; do
      [ -z "$cm" ] && continue
      echo "==> 触发漂流域 $(region_name "$cm")（domain-${cm}）上报延迟到 GL ($gl_addr)"
      attempt=1; ok=0
      while [ "$attempt" -le 3 ]; do
        if run "$cm" "$REMOTE_DIR/cdraft-client-linux -report-floating-latency \
          -target $gl_addr -origin-domain domain-${cm} -timeout 10s"; then
          ok=1; break
        fi
        echo "   第 ${attempt}/3 次上报失败（贵阳 SSH 偶发握手掐断），${attempt}s 后重试..." >&2
        sleep "$attempt"
        attempt=$((attempt + 1))
      done
      [ "$ok" = 1 ] || echo "[${cm}] 上报最终失败（domain-${cm} 行可能仍缺，可重跑一次）"
    done
  fi

  echo "==> 从 Global Leader ($gl_addr) 拉取实测 RTT 矩阵"
  out=$("$CLIENT_LOCAL" -metrics -target "$gl_addr" -timeout 5s) || {
    echo "拉取 metrics 失败" >&2; exit 1; }

  echo "$out" | grep '^metrics ' || true
  echo "$out" | awk '
    function short(d){ sub(/^domain-/,"",d); return d }
    /^rtt /{
      line=$0; sub(/^rtt /,"",line);
      eq=index(line,"="); pair=substr(line,1,eq-1); v=substr(line,eq+1);
      ar=index(pair,"->"); from=short(substr(pair,1,ar-1)); to=short(substr(pair,ar+2));
      rtt[from"|"to]=v; doms[from]=1; doms[to]=1;
    }
    END{
      n=0; for(d in doms) order[n++]=d;
      for(i=0;i<n;i++) for(j=i+1;j<n;j++) if(order[j]<order[i]){t=order[i];order[i]=order[j];order[j]=t}
      if(n==0){ print "(暂无 RTT 数据：集群刚起来还没测到，或没有跨域流量)"; exit }
      printf "%-9s","from\\to(ms)";
      for(i=0;i<n;i++) printf "%8s",order[i];
      printf "\n";
      for(i=0;i<n;i++){
        printf "%-9s",order[i];
        for(j=0;j<n;j++){
          if(order[i]==order[j]){ printf "%8s","~0"; continue }
          k=order[i]"|"order[j];
          printf "%8s",(k in rtt)?rtt[k]:"-";
        }
        printf "\n";
      }
      print "";
      print "(单位 ms，往返 RTT；- = 该方向暂无测量；漂流域 d 仅在近期 -report-floating-latency 上报后才有)";
    }'
}

cmd_all() {
  cmd_deploy
  cmd_start
  echo "==> 等待 20s 让真实选举收敛（各域 Domain Leader -> Global Leader）"
  sleep 20
  cmd_status
  cmd_test
}

case "${1:-}" in
  deploy)  cmd_deploy ;;
  start)   cmd_start ;;
  stop)    cmd_stop ;;
  restart) cmd_stop; cmd_start ;;
  status)  cmd_status ;;
  logs)    shift; cmd_logs "$@" ;;
  test)    cmd_test ;;
  rtt)     shift; cmd_rtt "$@" ;;
  clean)   cmd_clean ;;
  all)     cmd_all ;;
  *)
    grep '^#   ' "$0" | sed 's/^#   //'
    exit 1
    ;;
esac
