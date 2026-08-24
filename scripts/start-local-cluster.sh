#!/usr/bin/env bash
set -euo pipefail

config="${1:-config/cluster.json}"
data_dir="${2:-/tmp/cd-raft-data}"
pids=()

cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

for node in a1 a2 a3 b1 b2 b3 c1 c2 c3; do
  go run ./cmd/cdraft -config "$config" -node "$node" -data "$data_dir" &
  pids+=("$!")
done

echo "CD-Raft nodes started. Press Ctrl-C to stop."
wait
