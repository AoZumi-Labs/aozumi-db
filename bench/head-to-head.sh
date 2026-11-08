#!/usr/bin/env bash
# Head-to-head: aozumi-db against libSQL's server and embedded SQLite, same
# workloads, same machine. Each server run starts a fresh server on an empty
# data directory; each embedded run a fresh file. Rounds rotate the order so
# machine load drifts evenly across contenders.
#
#   bench/head-to-head.sh <path to sqld> <output dir> [rounds] [seconds per workload]
#   ROUND_FROM=3 bench/head-to-head.sh ...   # resume: run rounds 3.. only, appending
#
# A server that dies during a run is recorded as crashed, with the end of its
# log; one that stops answering is killed after a deadline and recorded as
# stalled. Either way the run goes on to the next contender.
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
ROUND_FROM=${ROUND_FROM:-1}
# Deadlines: YCSB loads 100k records and runs 6 workloads; the latency run is one.
YCSB_DEADLINE=$(( SECONDS_PER * 6 + 300 ))
LATENCY_DEADLINE=$(( SECONDS_PER + 120 ))
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
stop() { # dir
  if ! kill -0 "$SERVER" 2>/dev/null; then
    echo "**server exited during the run (crashed)**; the end of its log:" | tee -a "$OUT/raw.md"
    tail -n 5 "$1/server.log" | sed 's/^/    /' | tee -a "$OUT/raw.md"
  else
    kill "$SERVER" 2>/dev/null || true
    for _ in $(seq 50); do kill -0 "$SERVER" 2>/dev/null || break; sleep 0.1; done
    kill -9 "$SERVER" 2>/dev/null || true
  fi
  wait "$SERVER" 2>/dev/null || true
  SERVER=
}

# Runs the benchmark client with a deadline; a client still running then is
# killed and the run recorded as stalled.
bench() { # deadline args...
  local deadline=$1; shift
  local status=0
  timeout --signal=KILL "$deadline" "$BENCH" "$@" | tee -a "$OUT/raw.md" || status=${PIPESTATUS[0]}
  case $status in
    0) ;;
    137) echo "**stalled: no result within ${deadline}s, killed**" | tee -a "$OUT/raw.md" ;;
    *) echo "bench failed (exit $status)" | tee -a "$OUT/raw.md" ;;
  esac
}

CONTENDERS=(libsql aozumi-full aozumi-normal sqlite-full sqlite-normal)
for round in $(seq "$ROUND_FROM" "$ROUNDS"); do
  # Rotate the order by one each round.
  shift_by=$(( (round - 1) % ${#CONTENDERS[@]} ))
  order=("${CONTENDERS[@]:$shift_by}" "${CONTENDERS[@]:0:$shift_by}")
  for name in "${order[@]}"; do
    case $name in
      sqlite-*)
        dir=$WORK/$name-ycsb-$round; mkdir -p "$dir"
        echo "### round $round $name ycsb load: $(uptime | sed 's/.*averages*: //')" | tee -a "$OUT/raw.md"
        # The embedded database file goes in $dir, on the benchmark's volume.
        export TMPDIR=$dir
        bench "$YCSB_DEADLINE" ycsb-sqlite ABCDEF --records 100000 --clients 32 --seconds "$SECONDS_PER" --synchronous "${name#sqlite-}"
        unset TMPDIR
        rm -rf "$dir"
        continue
        ;;
    esac
    for mode in ycsb latency; do
      dir=$WORK/$name-$mode-$round; mkdir -p "$dir"
      start "$name" "$dir"
      echo "### round $round $name $mode load: $(uptime | sed 's/.*averages*: //')" | tee -a "$OUT/raw.md"
      if [[ $mode == ycsb ]]; then
        bench "$YCSB_DEADLINE" ycsb ABCDEF --records 100000 --clients 32 --seconds "$SECONDS_PER" --url "http://127.0.0.1:$PORT"
      else
        bench "$LATENCY_DEADLINE" latency --rate 1000 --clients 32 --seconds "$SECONDS_PER" --url "http://127.0.0.1:$PORT"
      fi
      stop "$dir"
      rm -rf "$dir"
    done
  done
done
echo "### done $(date -u +%FT%TZ)" | tee -a "$OUT/raw.md"
