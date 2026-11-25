#!/usr/bin/env bash
# Benchmark with a regression guardrail: release build, run, compare with bench/baseline.json.
# Usage: scripts/bench.sh [b1|b2|b3|all] [--accept]
#   --accept  make this run the new baseline before comparing (after an intentional change)
# Exit code: that of `compare` (1 = a tracked metric regressed).
set -euo pipefail
cd "$(dirname "$0")/.."

which=all
accept=0
for arg in "$@"; do
  case "$arg" in
    b1|b2|b3|all) which=$arg ;;
    --accept) accept=1 ;;
    *) echo "usage: scripts/bench.sh [b1|b2|b3|all] [--accept]" >&2; exit 2 ;;
  esac
done

docker compose up -d --wait
cargo build --release -p walrus-bench
bin=target/release/walrus-bench

"$bin" run "$which"
# `run` prints where it wrote the result; take the newest by timestamp.
if [ "$accept" = 1 ]; then
  "$bin" accept
fi

status=0
"$bin" compare || status=$?
echo
echo "report:   $(ls -t bench/results/*.md | head -1)"
echo "history:  bench/HISTORY.md"
echo "baseline: bench/baseline.json"
exit "$status"
