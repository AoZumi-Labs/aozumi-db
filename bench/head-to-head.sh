#!/usr/bin/env bash
# Head-to-head: aozumi-db against libSQL's server and embedded SQLite, same
# workloads, same machine. Each server run starts a fresh server on an empty
# data directory; each embedded run a fresh file. Rounds rotate the order so
# machine load drifts evenly across contenders.
#
#   bench/head-to-head.sh <path to sqld> <output dir> [rounds] [seconds per workload]
#
# Contenders:
#   libsql          libSQL's server (sqld), its defaults (synchronous = NORMAL)
#   aozumi-full     aozumi-db, its default durability (synchronous = FULL)
#   aozumi-normal   aozumi-db with AOZUMI_DB_SYNCHRONOUS=normal, as sqld
#   sqlite-full     SQLite's C library in the benchmark process (no network), FULL
#   sqlite-normal   the same, NORMAL
# The servers are reached over HTTP with libSQL's protocol, the way apps reach
# them; embedded SQLite is the ceiling an app gets without a server. YCSB runs
# on all five, the open-loop write latency on the servers.
set -euo pipefail
SQLD=$1
OUT=$2
ROUNDS=${3:-3}
SECONDS_PER=${4:-10}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
ADB=$ROOT/target/release/aozumi-db
BENCH=$ROOT/target/release/examples/bench
WORK=${BENCH_WORK:-$(mktemp -d)}
PORT=18090
mkdir -p "$OUT" "$WORK"

SERVER=
trap 'if [[ -n $SERVER ]]; then kill "$SERVER" 2>/dev/null || true; fi' EXIT

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

CONTENDERS=(libsql aozumi-full aozumi-normal sqlite-full sqlite-normal)
for round in $(seq "$ROUNDS"); do
  # Rotate the order by one each round.
  shift_by=$(( (round - 1) % ${#CONTENDERS[@]} ))
  order=("${CONTENDERS[@]:$shift_by}" "${CONTENDERS[@]:0:$shift_by}")
  for name in "${order[@]}"; do
    case $name in
      sqlite-*)
        dir=$WORK/$name-ycsb-$round; mkdir -p "$dir"
        echo "### round $round $name ycsb load: $(uptime | sed 's/.*averages*: //')" | tee -a "$OUT/raw.md"
        TMPDIR=$dir "$BENCH" ycsb-sqlite ABCDEF --records 100000 --clients 32 --seconds "$SECONDS_PER" --synchronous "${name#sqlite-}" | tee -a "$OUT/raw.md" || echo "bench failed (exit $?)" | tee -a "$OUT/raw.md"
        rm -rf "$dir"
        continue
        ;;
    esac
    for mode in ycsb latency; do
      dir=$WORK/$name-$mode-$round; mkdir -p "$dir"
      start "$name" "$dir"
      echo "### round $round $name $mode load: $(uptime | sed 's/.*averages*: //')" | tee -a "$OUT/raw.md"
      if [[ $mode == ycsb ]]; then
        "$BENCH" ycsb ABCDEF --records 100000 --clients 32 --seconds "$SECONDS_PER" --url "http://127.0.0.1:$PORT" | tee -a "$OUT/raw.md" || echo "bench failed (exit $?)" | tee -a "$OUT/raw.md"
      else
        "$BENCH" latency --rate 1000 --clients 32 --seconds "$SECONDS_PER" --url "http://127.0.0.1:$PORT" | tee -a "$OUT/raw.md" || echo "bench failed (exit $?)" | tee -a "$OUT/raw.md"
      fi
      stop
      rm -rf "$dir"
    done
  done
done
echo "### done $(date -u +%FT%TZ)" | tee -a "$OUT/raw.md"
