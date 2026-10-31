#!/usr/bin/env bash
# Runs the UNCHANGED original cloud.Benchmark from the tagged worktree. No trace, no fixes.
# Usage: run_original.sh <layout path> <out dir> <iterations> <threshold> <repeats> <quantityToMax> <discountToMax>
set -u
layout=$1; out=$2; iterations=$3; threshold=$4; repeats=$5; qty=$6; disc=$7
mkdir -p "$out"
printf '%s\n' "layout=$layout iterations=$iterations threshold=$threshold repeats=$repeats quantityToMax=$qty discountToMax=$disc" > "$out/args.txt"
timeout 900 spark-submit --master 'local[6]' --driver-memory 8g --conf spark.ui.enabled=false \
  --class cloud.Benchmark /work-original-build/caching-1.0-SNAPSHOT.jar \
  "$layout" "$iterations" "$threshold" "$repeats" "$qty" "$disc" > "$out/stdout.log" 2> "$out/stderr.log"
code=$?
echo "$code" > "$out/exit_code.txt"
echo "exit code $code"
