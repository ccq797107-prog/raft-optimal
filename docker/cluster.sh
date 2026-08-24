#!/usr/bin/env bash
# Simple cluster control so you don't have to remember the compose incantations.
#
#   ./cluster.sh build     # recompile + rebake the image after editing code
#   ./cluster.sh up        # build + start all 9 nodes + client helpers
#   ./cluster.sh staged    # rebuild, then bring domains up one at a time (a, b, c)
#   ./cluster.sh down      # stop everything and wipe state (-v)
#   ./cluster.sh status    # print each node's domain leader / global leader / term
#   ./cluster.sh gl        # print just the current Global Leader
#   ./cluster.sh logs [svc]# follow logs (default: a1)
set -euo pipefail
cd "$(dirname "$0")"

# node -> its own client address (the listener binds to the domain IP, not
# localhost, so each node must be queried via its own IP from inside itself).
ip_of() {
  case "$1" in
    a1) echo 10.10.1.11 ;; a2) echo 10.10.1.12 ;; a3) echo 10.10.1.13 ;;
    b1) echo 10.10.2.11 ;; b2) echo 10.10.2.12 ;; b3) echo 10.10.2.13 ;;
    c1) echo 10.10.3.11 ;; c2) echo 10.10.3.12 ;; c3) echo 10.10.3.13 ;;
  esac
}

# State now lives in an embedded LevelDB (single-process lock), so it can no
# longer be read by cat-ing a file. Query the running node over gRPC instead.
node_status() {
  docker compose exec -T "$1" cdraft-client -status -target "$(ip_of "$1"):7101" -timeout 3s 2>/dev/null \
    || echo "(down)"
}

print_status() {
  for f in a1 a2 a3 b1 b2 b3 c1 c2 c3; do
    echo -n "$f: "; node_status "$f"
  done
}

global_status() {
  for f in a1 b1 c1 a2 b2 c2 a3 b3 c3; do
    out="$(node_status "$f")"
    case "$out" in
      "(down)") continue ;;
    esac
    echo "$out"
    return 0
  done
  return 1
}

cmd="${1:-}"
case "$cmd" in
  build)
    # Recompile the Go binaries and rebake the shared cd-raft:local image. Run
    # this after editing code OUTSIDE the container; the binaries are baked into
    # the image (not bind-mounted), so a plain restart will NOT pick up changes.
    docker compose build
    echo "image rebuilt; recreate containers with: $0 up   (or $0 staged for a clean staged start)"
    ;;
  up)
    # --build rebuilds the image first (only service a1 declares build, so it is
    # produced once), then up -d recreates every container off the fresh image.
    docker compose up -d --build
    echo "started; give it ~8s to elect a Global Leader, then: $0 gl"
    ;;
  staged)
    # Always rebuild first so a staged run reflects the current code, then start
    # from a clean slate (down -v wipes the LevelDB volume).
    docker compose build
    docker compose down -v
    echo "== phase 1: domain-a =="; docker compose up -d a1 a2 a3; sleep 6
    echo "== phase 2: + domain-b =="; docker compose up -d b1 b2 b3; sleep 8
    echo "== phase 3: + domain-c + clients =="; docker compose up -d c1 c2 c3 client client-d; sleep 8
    print_status
    ;;
  down)
    docker compose down -v
    ;;
  status)
    print_status
    ;;
  gl)
    global_status | tr ' ' '\n' \
      | awk -F= '/^globalLeader=/{n=$2} /^globalLeaderDomain=/{d=$2} END{print "Global Leader:", (n?n:"-"), (d?d:"-")}'
    ;;
  logs)
    docker compose logs -f "${2:-a1}"
    ;;
  *)
    echo "usage: $0 {build|up|staged|down|status|gl|logs [svc]}" >&2; exit 2 ;;
esac
