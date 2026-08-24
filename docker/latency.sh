#!/usr/bin/env bash
# Change the inter-domain one-way latency live, on the running containers, with
# tc/netem. No rebuild and no restart needed.
#
#   ./latency.sh show                 # print each domain's egress shaping
#   ./latency.sh set <ab> <ac> <bc> [ad bd cd]
#   ./latency.sh pair <x> <y> <ms>    # set one link (x,y in a|b|c|d), keep the rest
#   ./latency.sh reset                # restore defaults
#
# Values are ONE-WAY ms; the RTT a link shows is the sum of both sides (e.g.
# ab=50 => a<->b RTT 100ms). Domain d is a floating client-only subnet. The
# current matrix is remembered in docker/.latency.
set -euo pipefail
cd "$(dirname "$0")"

STATE=".latency"
DEFAULT_AB=12 DEFAULT_AC=20 DEFAULT_BC=12
DEFAULT_AD=120 DEFAULT_BD=25 DEFAULT_CD=90

read_state() {
  if [ -f "$STATE" ]; then
    read -r AB AC BC AD BD CD < "$STATE"
    AD="${AD:-$DEFAULT_AD}"
    BD="${BD:-$DEFAULT_BD}"
    CD="${CD:-$DEFAULT_CD}"
  else
    AB=$DEFAULT_AB AC=$DEFAULT_AC BC=$DEFAULT_BC
    AD=$DEFAULT_AD BD=$DEFAULT_BD CD=$DEFAULT_CD
  fi
}

write_state() { echo "$AB $AC $BC $AD $BD $CD" > "$STATE"; }

# Re-apply the full tc tree (mirrors entrypoint.sh) inside one container.
apply_one() {
  local svc="$1" rules="$2"
  docker compose exec -T "$svc" env RULES="$rules" sh -s <<'EOF'
set -e
IFACE="${CD_IFACE:-eth0}"
tc qdisc del dev "$IFACE" root 2>/dev/null || true
tc qdisc add dev "$IFACE" root handle 1: htb default 99
tc class add dev "$IFACE" parent 1: classid 1:99 htb rate 10gbit ceil 10gbit quantum 100000
cid=10
for rule in $RULES; do
  subnet="${rule%%:*}"; delay="${rule##*:}"
  tc class add dev "$IFACE" parent 1: classid 1:${cid} htb rate 10gbit ceil 10gbit quantum 100000
  tc qdisc add dev "$IFACE" parent 1:${cid} handle ${cid}: netem delay "${delay}ms"
  tc filter add dev "$IFACE" protocol ip parent 1: prio 1 u32 match ip dst "$subnet" flowid 1:${cid}
  cid=$((cid + 10))
done
EOF
}

apply_all() {
  local a_rules="10.10.2.0/24:${AB} 10.10.3.0/24:${AC} 10.10.4.0/24:${AD}"
  local b_rules="10.10.1.0/24:${AB} 10.10.3.0/24:${BC} 10.10.4.0/24:${BD}"
  local c_rules="10.10.1.0/24:${AC} 10.10.2.0/24:${BC} 10.10.4.0/24:${CD}"
  local d_rules="10.10.1.0/24:${AD} 10.10.2.0/24:${BD} 10.10.3.0/24:${CD}"
  for s in a1 a2 a3 client; do apply_one "$s" "$a_rules"; done
  for s in b1 b2 b3;        do apply_one "$s" "$b_rules"; done
  for s in c1 c2 c3;        do apply_one "$s" "$c_rules"; done
  for s in client-d;         do apply_one "$s" "$d_rules"; done
  write_state
  echo "applied one-way matrix: ab=${AB}ms ac=${AC}ms bc=${BC}ms ad=${AD}ms bd=${BD}ms cd=${CD}ms"
  echo "  => RTT a<->b=$((AB*2))ms  a<->c=$((AC*2))ms  b<->c=$((BC*2))ms"
  echo "  => RTT a<->d=$((AD*2))ms  b<->d=$((BD*2))ms  c<->d=$((CD*2))ms"
}

cmd="${1:-}"
case "$cmd" in
  show)
    for s in a1 b1 c1 client-d; do
      echo "== $s =="
      docker compose exec -T "$s" tc qdisc show dev eth0 | sed 's/^/  /'
    done
    ;;
  set)
    AB="${2:?ab}" AC="${3:?ac}" BC="${4:?bc}"
    AD="${5:-$DEFAULT_AD}" BD="${6:-$DEFAULT_BD}" CD="${7:-$DEFAULT_CD}"
    apply_all
    ;;
  pair)
    read_state
    x="${2:?x}"; y="${3:?y}"; ms="${4:?ms}"
    link="$(printf '%s\n' "$x$y" | tr 'ABC' 'abc')"
    case "$link" in
      ab|ba) AB="$ms" ;;
      ac|ca) AC="$ms" ;;
      bc|cb) BC="$ms" ;;
      ad|da) AD="$ms" ;;
      bd|db) BD="$ms" ;;
      cd|dc) CD="$ms" ;;
      *) echo "pair must be two of a|b|c|d (e.g. a d)" >&2; exit 2 ;;
    esac
    apply_all
    ;;
  reset)
    AB=$DEFAULT_AB AC=$DEFAULT_AC BC=$DEFAULT_BC
    AD=$DEFAULT_AD BD=$DEFAULT_BD CD=$DEFAULT_CD
    apply_all
    ;;
  *)
    echo "usage: $0 {show | set <ab> <ac> <bc> [ad bd cd] | pair <x> <y> <ms> | reset}" >&2
    exit 2 ;;
esac
