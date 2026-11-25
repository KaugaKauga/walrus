#!/usr/bin/env bash
# The regular tests (what CI runs): fmt, clippy -D warnings, unit + integration tests against
# Postgres. Extra arguments go to `cargo test`, e.g. `scripts/test.sh -p walrus-engine catchup`.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose up -d --wait
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace "$@"
