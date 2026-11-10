#!/bin/bash
# Group commit against one flush per commit, same machine, same disks.
# For each disk and repeat, both modes run back to back, in alternating order.
set -u
export HOME=/root
. /root/.cargo/env
cd /bench/aozumi-db
R=/bench/results; mkdir -p $R
B=target/release/aozumi-db; X=target/release/examples/bench
{ echo "server: $(nproc) vCPU, $(lscpu | awk -F: '/Model name/ {gsub(/^ +/,"",$2); print $2}'), kernel $(uname -r), aozumi-db $(git rev-parse --short HEAD)"
  for m in /nvme /gp3; do echo "$m: $(findmnt -no SOURCE,FSTYPE,OPTIONS $m)"; done; } > $R/machine.txt
start() { # mode dir
  AOZUMI_DB_DEV=1 AOZUMI_DB_SYNCHRONOUS=$1 AOZUMI_DB_LISTEN=127.0.0.1:18091 AOZUMI_DB_DATA_DIR=$2 $B > $2.log 2>&1 &
  PID=$!
  for _ in $(seq 100); do curl -s -o /dev/null http://127.0.0.1:18091/health && return; sleep 0.1; done
  echo "server did not start" >> $R/errors.txt
}
stop() { kill $PID; wait $PID 2>/dev/null; }
for disk in nvme gp3; do
  for rep in 1 2; do
    if (( rep % 2 )); then modes="full group"; else modes="group full"; fi
    for mode in $modes; do
      out=$R/$disk-$mode-$rep.md
      dir=/$disk/w; rm -rf $dir $dir.log; mkdir -p $dir
      start $mode $dir
      echo "### $disk $mode repeat $rep load: $(cut -d' ' -f1-3 /proc/loadavg)" > $out
      timeout --signal=KILL 600 $X writes --sweep 1,4,16,64 --seconds 20 --url http://127.0.0.1:18091 >> $out 2>&1 || echo "bench exit $?" >> $out
      stop; rm -rf $dir
      mkdir -p $dir; start $mode $dir
      timeout --signal=KILL 900 $X ycsb ABC --records 100000 --clients 32 --seconds 20 --url http://127.0.0.1:18091 >> $out 2>&1 || echo "bench exit $?" >> $out
      stop; rm -rf $dir $dir.log
      sync; echo 3 > /proc/sys/vm/drop_caches; sleep 5
    done
  done
done
date -u +%FT%TZ > $R/DONE
