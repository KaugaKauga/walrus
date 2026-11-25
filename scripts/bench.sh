#!/usr/bin/env bash
# Benchmarks with a regression guardrail against bench/baseline.json.
#
#   scripts/bench.sh [b1|b2|b3|all]   While developing: run and compare with the baseline.
#                                     Writes nothing under bench/ (scratch output: target/bench/).
#   scripts/bench.sh --record         When the work is done, before pushing: full run of the
#                                     committed tree. Unless a metric regressed, it is saved to
#                                     bench/results, becomes the baseline, and bench/HISTORY.md
#                                     is regenerated. Then commit bench/.
#   scripts/bench.sh --record --force Record even though a metric regressed (intended trade-off).
#
# Exit code: 1 if a tracked metric regressed (and, with --record, nothing was saved).
set -euo pipefail
cd "$(dirname "$0")/.."

usage() {
  echo "usage: scripts/bench.sh [b1|b2|b3|all] | --record [--force]" >&2
  exit 2
}

which=all
record=0
force=0
for arg in "$@"; do
  case "$arg" in
    b1|b2|b3|all) which=$arg ;;
    --record) record=1 ;;
    --force) force=1 ;;
    *) usage ;;
  esac
done
if [ "$record" = 1 ] && [ "$which" != all ]; then
  echo "--record always runs every scenario (the baseline must cover all metrics)" >&2
  exit 2
fi
if [ "$force" = 1 ] && [ "$record" = 0 ]; then
  usage
fi

docker compose up -d --wait
cargo build --release -p walrus-bench
bin=target/release/walrus-bench

if [ "$record" = 1 ]; then
  if [ "$force" = 1 ]; then
    exec "$bin" record --force
  fi
  exec "$bin" record
fi

"$bin" run "$which"
status=0
"$bin" compare || status=$?
echo
echo "report: $(ls -t target/bench/*.md | head -1)   (dev run; nothing under bench/ changed)"
exit "$status"
