#!/usr/bin/env bash
# Inspect or steer which domain holds the Global Leader.
#
#   ./gl.sh show            # print the current Global Leader
#   ./gl.sh move <a|b|c>    # force the GL into that domain
#
# Global election is intentionally symmetric (randomized timers, no preferred
# domain), so `move` works by repeatedly evicting the domain that currently holds
# the GL: it STOPS that domain's nodes, lets the remaining two re-elect a GL
# among themselves, then brings the stopped domain back as a follower. The target
# domain is never stopped, so the GL converges onto it within a few rounds. All
# domains are left running at the end.
set -euo pipefail
cd "$(dirname "$0")"

nodes_of() {
  case "$1" in
    a) echo "a1 a2 a3" ;;
    b) echo "b1 b2 b3" ;;
    c) echo "c1 c2 c3" ;;
  esac
}

# State lives in an embedded LevelDB now, so we ask a running node for its view
# of the Global Leader over gRPC instead of reading a state file. `move` may have
# the queried domain stopped, so try one node per domain until one answers.
gl_status() {
  for nd in a1:10.10.1.11 b1:10.10.2.11 c1:10.10.3.11; do
    out=$(docker compose exec -T "${nd%%:*}" cdraft-client -status -target "${nd##*:}:7101" -timeout 3s 2>/dev/null) || continue
    [ -n "$out" ] && { echo "$out"; return 0; }
  done
}

gl_domain() {
  gl_status | tr ' ' '\n' | awk -F= '/^globalLeaderDomain=/{
    if($2=="domain-a")print "a"; else if($2=="domain-b")print "b";
    else if($2=="domain-c")print "c"; else print "?"
  }'
}

gl_node() {
  gl_status | tr ' ' '\n' \
    | awk -F= '/^globalLeader=/{n=$2} /^globalLeaderDomain=/{d=$2} END{print "Global Leader:", (n?n:"-"), (d?d:"-")}'
}

cmd="${1:-}"
case "$cmd" in
  show)
    gl_node
    ;;
  move)
    target="${2:-}"
    case "$target" in a|b|c) ;; *) echo "usage: $0 move <a|b|c>" >&2; exit 2 ;; esac
    for attempt in $(seq 1 8); do
      cur="$(gl_domain)"
      echo "[attempt $attempt] current GL domain: ${cur:-none}, target: $target"
      if [ "$cur" = "$target" ]; then
        gl_node
        echo "done: GL is in domain-$target"
        exit 0
      fi
      if [ "$cur" = "?" ] || [ -z "$cur" ]; then
        echo "  no GL yet, waiting..."; sleep 8; continue
      fi
      echo "  evicting domain-$cur to force re-election..."
      docker compose stop $(nodes_of "$cur") >/dev/null
      sleep 12                      # remaining two domains re-elect a GL
      docker compose start $(nodes_of "$cur") >/dev/null
      sleep 5                       # evicted domain rejoins as follower
    done
    echo "gave up after 8 attempts; current GL:" >&2
    gl_node
    exit 1
    ;;
  *)
    echo "usage: $0 {show | move <a|b|c>}" >&2; exit 2 ;;
esac
