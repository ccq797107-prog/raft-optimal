#!/usr/bin/env bash
# End-to-end demo of DECOUPLED Global Leader migration ("切主").
#
# The consensus core does NOT decide where the leader should be. It only (a)
# measures per-domain W/R window statistics + the RTT matrix (exposed via the
# Metrics RPC) and (b) executes a safe handoff when the current GL receives a
# GL-only Move RPC. The DECISION is made by an external controller, cdraft-mover,
# which polls those statistics, runs the paper cost model, and issues Move.
#
# This script drives a sustained ASYMMETRIC read/write load from one domain and
# runs cdraft-mover; you should see the controller decide and the GL migrate
# toward the loaded domain.
#
# Prereqs (the image must contain the current code, which builds cdraft-mover):
#
#   ./cluster.sh build      # rebuild image with current code + config
#   ./cluster.sh up         # start the 9-node cluster + client, wait for a GL
#   ./auto-migration-demo.sh [durationSeconds] [loadDomain a|b|c]
#
# With no args it loads for 60s and auto-picks a domain that is NOT the current
# Global Leader, so you should see the GL move to the loaded domain.
set -euo pipefail
cd "$(dirname "$0")"

DURATION="${1:-60}"
LOAD_DOMAIN="${2:-}"

node_ip() {
  case "$1" in
    a1) echo 10.10.1.11 ;; a2) echo 10.10.1.12 ;; a3) echo 10.10.1.13 ;;
    b1) echo 10.10.2.11 ;; b2) echo 10.10.2.12 ;; b3) echo 10.10.2.13 ;;
    c1) echo 10.10.3.11 ;; c2) echo 10.10.3.12 ;; c3) echo 10.10.3.13 ;;
  esac
}
first_ip() { case "$1" in a) echo 10.10.1.11 ;; b) echo 10.10.2.11 ;; c) echo 10.10.3.11 ;; esac; }
svc_of()   { case "$1" in a) echo a1 ;; b) echo b1 ;; c) echo c1 ;; esac; }
dom_of()   { case "$1" in a) echo domain-a ;; b) echo domain-b ;; c) echo domain-c ;; esac; }

# Current Global Leader domain (e.g. "domain-a"), queried from any live node.
gl_domain() {
  for n in a1 b1 c1 a2 b2 c2 a3 b3 c3; do
    line=$(docker compose exec -T "$n" cdraft-client -status -target "$(node_ip "$n"):7101" -timeout 3s 2>/dev/null) || continue
    dom=$(echo "$line" | grep -o 'globalLeaderDomain=[^ ]*' | cut -d= -f2 || true)
    if [ -n "${dom:-}" ] && [ "$dom" != "-" ]; then echo "$dom"; return 0; fi
  done
  echo ""
}

start_gl=$(gl_domain)
if [ -z "$start_gl" ]; then
  echo "No Global Leader yet. Start the cluster first: ./cluster.sh up" >&2
  exit 1
fi
if [ -z "$LOAD_DOMAIN" ]; then
  case "$start_gl" in
    domain-c) LOAD_DOMAIN=a ;;
    *)        LOAD_DOMAIN=c ;;
  esac
fi
want="domain-$LOAD_DOMAIN"
echo "start GL = $start_gl ; driving load from $want for ${DURATION}s ; expecting GL -> $want"
if [ "$start_gl" = "$want" ]; then
  echo "(current GL is already $want; pass a different loadDomain to see a move)"
fi

ip=$(first_ip "$LOAD_DOMAIN"); svc=$(svc_of "$LOAD_DOMAIN"); dom=$(dom_of "$LOAD_DOMAIN")
end=$(( $(date +%s) + DURATION ))

# Background load generator: a steady stream of cross-domain writes + reads from
# the load domain, so its W/R counts dominate the GL's sliding window.
(
  i=0
  while [ "$(date +%s)" -lt "$end" ]; do
    i=$((i + 1))
    docker compose exec -T "$svc" cdraft-client -target "${ip}:7101" -origin-domain "$dom" \
      -key demo -value "v$i" -count 5 -request-id "demo-$(date +%s%N)" \
      -callback-listen 0.0.0.0:9100 -reply-route "${ip}:9100" -timeout 12s >/dev/null 2>&1 || true
    docker compose exec -T "$svc" cdraft-client -target "${ip}:7101" -origin-domain "$dom" \
      -key demo -count 10 -timeout 12s >/dev/null 2>&1 || true
  done
) &
load_pid=$!

# External migration controller: polls the GL's window statistics, runs the cost
# model, and issues the GL-only Move RPC. It is the ONLY thing that decides to
# migrate; the consensus core never does. Short cooldown/confirmations make the
# demo move promptly. Its decision/move log lines are tagged "cdraft-mover:".
docker compose exec -T a1 cdraft-mover \
  -config /etc/cd-raft/cluster.json \
  -poll 2s -confirmations 2 -cooldown 8s -min-improvement 0.1 \
  > /tmp/cdraft-mover.log 2>&1 &
mover_pid=$!

migrated=0
while [ "$(date +%s)" -lt "$end" ]; do
  now_gl=$(gl_domain)
  echo "[$(date +%T)] GL=$now_gl"
  if [ "$now_gl" = "$want" ] && [ "$now_gl" != "$start_gl" ]; then
    echo "✅ MIGRATION OBSERVED: Global Leader moved $start_gl -> $now_gl"
    migrated=1
    break
  fi
  sleep 3
done

kill "$load_pid" 2>/dev/null || true
wait "$load_pid" 2>/dev/null || true
kill "$mover_pid" 2>/dev/null || true
wait "$mover_pid" 2>/dev/null || true

echo "--- cdraft-mover decisions (external controller) ---"
grep -iE 'cdraft-mover:' /tmp/cdraft-mover.log | tail -40 || true
echo "--- node-side handoff log lines (recent) ---"
docker compose logs --since "$((DURATION + 10))s" 2>/dev/null \
  | grep -iE 'migration (START|OK|ABORT)' | tail -40 || true

if [ "$migrated" -ne 1 ]; then
  echo "No migration observed within ${DURATION}s. Try a longer duration or heavier load; check /tmp/cdraft-mover.log for the controller's decisions." >&2
fi
