#!/bin/sh
# Self-inject real one-way network latency with tc/netem, then run the node.
#
# Each container shapes its own EGRESS toward the OTHER domains' subnets, so the
# per-link round trip is the sum of both sides' one-way delays (e.g. a->b 50ms +
# b->a 50ms = 100ms RTT). Same-domain and client/gateway traffic falls into the
# default class with no added delay. This is real kernel packet delay on the
# real gRPC path, so the app-level networkSimulation MUST stay disabled.
#
# Requires NET_ADMIN (granted via cap_add in docker-compose.yml).
set -e

IFACE="${CD_IFACE:-eth0}"

# One-way egress matrix per domain: "<dst-subnet>:<delay-ms> ...".
# Domains map to /24s: domain-a=10.10.1.0/24, domain-b=10.10.2.0/24,
# domain-c=10.10.3.0/24, floating domain-d=10.10.4.0/24. Matrix:
# a<->b 50ms, a<->c 80ms, b<->c 60ms, a<->d 120ms, b<->d 25ms,
# c<->d 90ms.
case "$CD_DOMAIN" in
  domain-a) RULES="10.10.2.0/24:50 10.10.3.0/24:80 10.10.4.0/24:120" ;;
  domain-b) RULES="10.10.1.0/24:50 10.10.3.0/24:60 10.10.4.0/24:25" ;;
  domain-c) RULES="10.10.1.0/24:80 10.10.2.0/24:60 10.10.4.0/24:90" ;;
  domain-d) RULES="10.10.1.0/24:120 10.10.2.0/24:25 10.10.3.0/24:90" ;;
  *)        RULES="" ;;
esac

if [ -n "$RULES" ]; then
  # Clear any prior shaping (idempotent on container restart).
  tc qdisc del dev "$IFACE" root 2>/dev/null || true
  # Classful root; default class 99 = no added delay (same-domain, gateway).
  tc qdisc add dev "$IFACE" root handle 1: htb default 99
  tc class add dev "$IFACE" parent 1: classid 1:99 htb rate 10gbit ceil 10gbit quantum 100000
  cid=10
  for rule in $RULES; do
    subnet="${rule%%:*}"
    delay="${rule##*:}"
    tc class add dev "$IFACE" parent 1: classid 1:${cid} htb rate 10gbit ceil 10gbit quantum 100000
    tc qdisc add dev "$IFACE" parent 1:${cid} handle ${cid}: netem delay "${delay}ms"
    tc filter add dev "$IFACE" protocol ip parent 1: prio 1 u32 \
      match ip dst "$subnet" flowid 1:${cid}
    echo "tc: egress to ${subnet} delayed ${delay}ms"
    cid=$((cid + 10))
  done
fi

# The client helper is not a node: it just needs the same egress shaping as a
# member of its CD_DOMAIN so that cdraft-client reads/writes run over real
# (tc-shaped) cross-domain links. After applying tc it idles; exec into it to
# send requests.
if [ "${CD_CLIENT_HELPER:-}" = "true" ] || [ "$CD_NODE" = "client" ]; then
  echo "client helper ready (egress shaped as ${CD_DOMAIN}); idling"
  exec sleep infinity
fi

exec /usr/local/bin/cdraft -config /etc/cd-raft/cluster.json -node "$CD_NODE" -data /var/lib/cd-raft
