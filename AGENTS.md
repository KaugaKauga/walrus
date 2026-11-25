# AGENTS.md

How to work in this repository, for people and coding agents alike.

What walrus is and why it's built this way lives in [PLAN.md](PLAN.md) (design, invariants,
protocol) and [docs/decisions/](docs/decisions/) (architecture decisions). Read the relevant
section before changing behavior. Neither is restated here.

## Commands

```sh
scripts/test.sh               # fmt check, clippy -D warnings, all tests (starts Postgres via docker compose)
scripts/bench.sh [b1|b2|b3]   # while developing: benchmark vs the baseline; writes nothing under bench/
scripts/bench.sh --record     # work done and committed: record the run as baseline + history
```

A change is done when `scripts/test.sh` passes. When a change can affect performance, also run
`scripts/bench.sh`. Before pushing finished work, commit it, run `scripts/bench.sh --record` and
commit `bench/`.

## Code

- **Simple and concrete.**
  - No traits with a single implementation.
  - No speculative extension points.
  - No configuration for values we tune with benchmarks; those are constants.
  - Each invariant has one owner (PLAN.md §3). Don't re-check or work around it elsewhere.
- **Errors.**
  - Use `anyhow` with context at boundaries. Use typed errors only where a caller branches on them.
  - Invariant violations stop the engine with a clear message.
- **Safety.**
  - No `unsafe`.
  - No `unwrap()` outside tests. Use `expect("why this can't fail")` only for real invariants.
- **Performance is a feature.**
  - Prefer the proven patterns in PLAN.md §3.
  - Measure before optimizing, and keep the benchmark guardrail green.

## Comments

The reader is an experienced developer. Code should explain itself through names, types and
structure. A comment is the exception, not the default.

**Write a comment only when it tells the reader something the code can't:**

- why a non-obvious choice was made, especially when the obvious alternative is wrong;
- an invariant being enforced, cited by its PLAN.md id, e.g. `// I7: subscribe before reading P0`;
- a Postgres, protocol or library behavior the code depends on;
- a measured performance reason;
- a short summary of genuinely complex logic: the join, group commit, wire formats.

**Don't write:**

- comments that restate what the next line does;
- doc comments that repeat the item's name or signature (`/// Creates a new parser.` on `new`);
- step-by-step narration, or section banners;
- design text copied from PLAN.md. Link the section instead (`PLAN.md §7.1`).

**Doc comments (`///`)** belong on public items only when they carry a contract the signature
can't express: units, error conditions, ordering or concurrency rules, or what `None` means.

**Module docs (`//!`)**: one or two lines saying what the module owns, plus a PLAN.md reference
where one exists.

**Tests** are named for the behavior they prove. Comment a test only for setup that isn't obvious,
such as why a timing or a specific Postgres state is needed.

## Tests

- Integration tests run against the compose Postgres (`localhost:54329`).
  - Use the harness in `crates/walrus-engine/tests/common/`. Each test gets its own database,
    slot and data directory, so tests run in parallel. Never change server-wide settings in a
    test that shares a binary with others.
  - Expected state always comes from Postgres itself (`SELECT *`), never from the engine.
- A bug fix comes with the test that reproduces it.
- The model test (`tests/model.rs`) prints its seed. Replay it with `WALRUS_MODEL_SEED=<seed>`.

## Decisions and docs

- A design change updates PLAN.md in the same change.
- Record it in PLAN.md §17 (decision log).
- A defining decision gets an ADR in `docs/decisions/`.
- Keep README.md's quickstart working.
