#!/usr/bin/env bash
# Everything CI runs. Needs `docker compose up -d` for the integration tests.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
