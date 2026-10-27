#!/usr/bin/env bash
# Head-to-head: aozumi-db against libSQL's server, same client, same machine.
# Each run starts a fresh server on an empty data directory. Rounds alternate
# the order so machine load drifts evenly across contenders.
#
#   bench/head-to-head.sh <path to sqld> <output dir> [rounds]
set -euo pipefail
SQLD=$1
OUT=$2
ROUNDS=${3:-3}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
ADB=$ROOT/target/release/aozumi-db
BENCH=$ROOT/target/release/examples/bench
WORK=$(mktemp -d)
PORT=18090
mkdir -p "$OUT"

SERVER=
trap 'if [[ -n $SERVER ]]; then kill "$SERVER" 2>/dev/null || true; fi; rm -rf "$WORK"' EXIT

start() { # name dir
  if curl -s -o /dev/null "http://127.0.0.1:$PORT/"; then
    echo "port $PORT is already in use; stop that server first" >&2; exit 1
  fi
  case $1 in
    libsql) "$SQLD" --db-path "$2/data.sqld" --http-listen-addr 127.0.0.1:$PORT >"$2/server.log" 2>&1 & ;;
    aozumi-full) AOZUMI_DB_DEV=1 AOZUMI_DB_LISTEN=127.0.0.1:$PORT AOZUMI_DB_DATA_DIR="$2" "$ADB" >"$2/server.log" 2>&1 & ;;
    aozumi-normal) AOZUMI_DB_DEV=1 AOZUMI_DB_SYNCHRONOUS=normal AOZUMI_DB_LISTEN=127.0.0.1:$PORT AOZUMI_DB_DATA_DIR="$2" "$ADB" >"$2/server.log" 2>&1 & ;;
  esac
  SERVER=$!
  for _ in $(seq 100); do curl -s -o /dev/null -X POST "http://127.0.0.1:$PORT/v2/pipeline" -d '{"baton":null,"requests":[{"type":"close"}]}' && return; sleep 0.1; done
  echo "server $1 did not start" >&2; exit 1
}
stop() { kill "$SERVER"; wait "$SERVER" 2>/dev/null || true; SERVER=; }

for round in $(seq "$ROUNDS"); do
  if (( round % 2 )); then order="libsql aozumi-full aozumi-normal"; else order="aozumi-normal aozumi-full libsql"; fi
  for name in $order; do
    for mode in ycsb latency; do
      dir=$WORK/$name-$mode-$round; mkdir -p "$dir"
      start "$name" "$dir"
      echo "### round $round $name $mode load: $(uptime | sed 's/.*averages*: //')" | tee -a "$OUT/raw.md"
      if [[ $mode == ycsb ]]; then
        "$BENCH" ycsb ABCDEF --records 100000 --clients 32 --seconds 10 --url "http://127.0.0.1:$PORT" | tee -a "$OUT/raw.md" || echo "bench failed (exit $?)" | tee -a "$OUT/raw.md"
      else
        "$BENCH" latency --rate 1000 --clients 32 --seconds 10 --url "http://127.0.0.1:$PORT" | tee -a "$OUT/raw.md" || echo "bench failed (exit $?)" | tee -a "$OUT/raw.md"
      fi
      stop
      rm -rf "$dir"
    done
  done
done
