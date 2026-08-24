#!/usr/bin/env bash
# Simple wrapper around cdraft-client so you don't have to remember IPs, ports
# and Fast Return reply-routes.
#
#   ./client.sh <a|b|c|d> read  [count] [key]
#   ./client.sh <a|b|c|d> write [count] [key] [value]
#
# Examples:
#   ./client.sh a write 10            # 10 writes from domain-a (key=foo value=bar)
#   ./client.sh b read 10             # 10 reads from domain-b
#   ./client.sh c write 5 mykey hello # 5 writes of mykey=hello from domain-c
#   ./client.sh d write 10            # 10 writes from floating domain-d
#
# For consensus domains it execs into that domain's first node (a1/b1/c1). For
# floating domain-d it execs into client-d, a client-only helper with no CD-Raft
# node. The request originates inside the selected domain and pays the real
# tc-shaped client<->GL RTT. The client follows the Global Leader redirect
# automatically and caches it, so only the first op pays redirect/cold-start cost.
set -euo pipefail
cd "$(dirname "$0")"

domain="${1:-}"
mode="${2:-}"
count="${3:-1}"
key="${4:-foo}"
value="${5:-bar}"

case "$domain" in
  a) svc=a1; ip=10.10.1.11; dom=domain-a ;;
  b) svc=b1; ip=10.10.2.11; dom=domain-b ;;
  c) svc=c1; ip=10.10.3.11; dom=domain-c ;;
  d) svc=client-d; ip=10.10.4.20; dom=domain-d ;;
  *) echo "usage: $0 <a|b|c|d> <read|write> [count] [key] [value]" >&2; exit 2 ;;
esac

case "$domain" in
  a|b|c) target="${ip}:7101" ;;
  d)     target="10.10.2.11:7101" ;;
esac

common=(-target "$target" -origin-domain "$dom" -key "$key" -count "$count" -timeout 12s)

start_floating_latency_report() {
	if [ "$domain" != "d" ]; then
		return 0
	fi
	docker compose exec -T "$svc" cdraft-client \
		-target "$target" -origin-domain "$dom" \
		-report-floating-latency -timeout 12s >/dev/null 2>&1 &
}

case "$mode" in
	read)
		docker compose exec -T "$svc" cdraft-client "${common[@]}"
		;;
	write)
		start_floating_latency_report
		docker compose exec -T "$svc" cdraft-client "${common[@]}" \
			-value "$value" \
			-request-id "cli-$(date +%s)-$$" \
			-callback-listen 0.0.0.0:9100 -reply-route "${ip}:9100"
		;;
	*)
		echo "usage: $0 <a|b|c|d> <read|write> [count] [key] [value]" >&2
		exit 2
		;;
esac
