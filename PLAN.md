# Walrus — MVP plan

Walrus (WAL + Rust) is a sync engine. It:

- reads Postgres's logical replication stream;
- fans changes out to clients over Server-Sent Events (SSE);
- remembers which rows changed when, so a returning client gets only the rows it is missing,
  read straight from Postgres.

Clients (eventually Leptos apps) keep a local copy that converges to the latest state.

The engine keeps no copy of the data: Postgres remains the only source of rows. Why we chose this
over a stateless engine or a full row store: [ADR 0001](docs/decisions/0001-state-model.md).

This document is the plan for the first version:

- what it does;
- how it is built;
- how we prove it is correct and fast;
- the order we build it in.

## Status (2026-11-25)

**MVP milestones M0–M6 are done.**

- `scripts/test.sh` is green: fmt, clippy `-D warnings`, 146 unit and integration tests against
  Postgres 17. The seeded model test is part of the suite.
- Benchmark history and the current baseline: `bench/HISTORY.md` (`scripts/bench.sh --record`).

| Scenario | Result | Target |
|---|---|---|
| Commit → client latency | p50 2.6 ms, p99 7.2 ms | p50 25 ms, p99 100 ms |
| Sustained ingest | ≈ 22.5k changes/s | 10k changes/s |
| Delta resume of 100 changed rows in a 100k-row table | 2.4 ms, 20 KiB | — |
| Full catch-up of the same table | 84 ms, 20.7 MiB | — |

Known follow-ups, smallest first:

1. Measure replication lag against `pg_current_wal_lsn()`.
2. Add the missing §9 histograms.
3. Coalesce live commits for clients that are behind.
4. Benchmark on Linux with real fsync.
5. The two residual join risks in §14.

Next: Phase 1b (§10.3).

---

## 1. What the MVP must prove

1. **Correctness.** A client always ends up with exactly the rows `SELECT *` returns, under
   concurrent writes. That holds whether it connects, disconnects, reconnects, or comes back after
   the engine restarts.
2. **Performance.** We know the engine's envelope from measurements, not guesses, and it meets the
   targets in §10. The envelope covers latency, ingest rate, client count, memory and catch-up
   cost. Catch-up cost must scale with what changed, not with table size.

Everything in the MVP serves one of these two goals.

### In scope

- Whole configured tables (all rows, all columns), Postgres 15–18 (CI runs 17 for the MVP).
- Read path only: Postgres → engine → clients.
- A durable **change index**: the commit LSN of every key's latest change. No row data and no
  per-transaction log.
- Catch-ups read from Postgres: full tables for new clients, only the changed rows for returning
  ones, nothing at all when nothing changed.
- One SSE stream per client covering any subset of the configured tables.
- Resume after disconnects and engine restarts using offsets.
- Correctness test suite against a real Postgres in Docker, and a benchmark harness.

### Out of scope (deliberately)

- Auth, row filtering (WHERE-clause shapes), column selection.
- Writes and optimistic updates (the offset design already supports them later, see §16).
- More than one engine instance, high availability.
- Finer handling of schema and table-set changes. For now either change starts a new epoch, so
  every client refetches its tables (§4.5).
- TLS to Postgres, CORS, CDN caching, HTTP GET catch-up.
- The browser/WASM client and Leptos bindings (next phases).

---

## 2. Vocabulary

| Term | Meaning |
|---|---|
| **WAL** | Postgres's write-ahead log: its append-only journal of every change. |
| **LSN** | A position in the WAL (a 64-bit number, shown like `0/16B3740`). Only goes up. |
| **Slot** | A replication slot: Postgres's bookmark of how far we have confirmed. Postgres keeps WAL from there on. |
| **Publication** | The list of tables whose changes Postgres decodes for us. |
| **Commit LSN** | The LSN of a transaction's commit record, known from its `Begin` message. Orders transactions; used in the index and in offsets. |
| **Change index** | The engine's durable map `(table, primary key) → commit LSN of the key's latest change`. Deletes are changes too. No row data. |
| **Epoch** | A random 64-bit id created with the change index. A new epoch (new index, new slot) invalidates every offset. |
| **Offset** | `epoch` + commit LSN: a client's "I am up to date as of here". Opaque to clients. |
| **Floor** | Per table: the LSN from which the index is complete. Below it, the client gets a full table. |
| **Catch-up** | What a connecting client receives before live updates: full tables, changed rows, or nothing. |
| **Delta keys** | For a returning client: the keys whose latest change is after its offset. |
| **Join** | Splicing a catch-up read from a Postgres snapshot with the live stream, so no change is lost or applied twice. |

---

## 3. Engineering principles

### Simplicity

- **Concrete types, no speculative abstraction.** No traits with one implementation, no plugin
  points, no config for values we tune with benchmarks (those are constants).
- **One owner per invariant.** Each module owns specific invariants (listed below). No other
  module re-checks or works around them.
- **Synchronous core, async edges.** The index store and the writer are plain blocking code on
  their own threads. Async (tokio) is used only for network I/O: Postgres connections and HTTP.
- **Errors:** `anyhow` with context at boundaries. Typed errors only where a caller branches on
  them. Invariant violations stop the engine with a clear message; they are never papered over.
- **Tooling:** `rustfmt`, `clippy -D warnings`, no `unsafe`, no `unwrap()` outside tests (use
  `expect("why this cannot fail")` for real invariants).
- **Comments only where they add something** the code can't say: see [AGENTS.md](AGENTS.md).

### Proven performance patterns we commit to

| Pattern | Where | Why / precedent |
|---|---|---|
| **Single writer** | One thread owns all index writes | No write locks or contention. The WAL stream is serial anyway (LMAX, most databases' log writers). |
| **Opportunistic group commit** | Writer | Each store commit applies every transaction already queued. There is no linger timer, so no extra latency at low load and high throughput at high load. |
| **Ack after durable commit** | Writer → replication | Postgres discards WAL we confirm, so we only confirm what is fsynced (the same rule as Postgres's own WAL). |
| **Idempotent apply by LSN** | Writer | Replication is at-least-once after a restart. Skipping `commit_lsn <= last_lsn` makes it effectively once. |
| **Encode once, share bytes** | Fan-out | Each transaction is serialized once into SSE frames (`bytes::Bytes`) that every session shares. Per-client work is a memory copy into the socket. |
| **Ordered index range scans** | Catch-up | "Which keys changed since X" is a B-tree range scan on `(table, lsn)`, so its cost is proportional to the delta. |
| **Batched primary-key reads** | Delta catch-up | Postgres's cheapest query: one round trip per batch, cost proportional to the delta. |
| **Skip Postgres when nothing changed** | Catch-up | An empty delta is answered from the index alone. This is the common case for clients that reconnect often. |
| **MVCC snapshots** | Catch-up | Rows come from a Postgres `REPEATABLE READ` snapshot and keys from a store read transaction. Both stay consistent while writes continue. |
| **Bounded queues everywhere + drop slow consumers** | Replication → writer, writer → sessions, catch-up → socket | Memory stays bounded. A slow client is disconnected and recovers with a cheap catch-up instead of holding buffers. |
| **Admission control** | Catch-ups | A fixed pool of catch-up connections caps concurrent Postgres reads, so a reconnect storm can neither overload Postgres nor starve live updates. |
| **COPY for reads** | Catch-ups | Postgres's fastest read path. One text parser serves full and delta reads (Debezium and Supabase ETL also use COPY). |
| **Zero-copy decoding** | pgoutput decoder | Slice the incoming `Bytes` instead of allocating per field. |

### Invariants and who owns them

| # | Invariant | Owner |
|---|---|---|
| I1 | Postgres is only acknowledged up to LSNs whose index commit is durable. | writer |
| I2 | `last_lsn` is written in the same store transaction as the index entries it covers. | store |
| I3 | Transactions with `commit_lsn <= last_lsn` are skipped. | writer |
| I4 | Sessions only see durable state. Broadcast happens after commit, so every offset a client holds is ≤ the durable `last_lsn`. | writer |
| I5 | Above a table's floor, the index holds the commit LSN of every key's latest change. | writer + store |
| I6 | When the engine cannot guarantee I5 for a table (truncate, retention), it raises that table's floor. It never guesses; a full catch-up is always correct. | writer |
| I7 | A session subscribes to the live stream before it reads the index at `P0` and before it takes a Postgres snapshot. It skips live transactions `<= P0`. | session |
| I8 | A catch-up snapshot is used only if it can see every transaction the engine applied at or before `P0` (§7.2). During the join, a streamed transaction visible in the snapshot is dropped for exactly the rows the catch-up read, and sent for all others. | session + pg/xid |
| I9 | Catch-up reads (COPY) and the stream render values identically: same session display settings, same columns. | pg/setup |
| I10 | New-epoch setup is all-or-nothing: `initialized` is set in the same store transaction that records the epoch and floors, after the slot exists. | pg/setup + store |

---

## 4. Postgres integration

### 4.1 Requirements and startup checks

The engine checks these at startup and exits with a clear, actionable error if any fails:

- `wal_level = logical`.
- Free capacity in `max_replication_slots`, `max_wal_senders`, and `max_connections` (for the
  catch-up pool).
- The role has `REPLICATION` (or is superuser), can create/alter the publication, and can
  `SELECT` the configured tables.
- Every configured table exists and has a primary key.
- **Every primary-key column has a type whose equal values always render the same text**:
  integer types, `uuid`, `text`/`varchar` with a deterministic collation, `date`, `timestamp`,
  `timestamptz`, `bytea`. Clients and the engine identify rows by the text of their key. With
  `numeric` (`1.0` = `1.00`) or `float8` (`0` = `-0`), equal keys can render differently.
- No configured table has `REPLICA IDENTITY NOTHING`, because that would make users' own UPDATE
  and DELETE statements fail once the table is published. The default identity (primary key) is
  all we need; we never require `FULL` (§6.3).

If a store exists, the engine also compares it with Postgres. Any of the following makes the
engine start a **new epoch** (§4.5) and log a warning:

- the slot is missing, or invalidated (`pg_replication_slots.wal_status = lost`);
- the configured table set differs from the stored one;
- a table's columns differ from the stored schema.

### 4.2 Connections and session settings

Every connection gets the same pinned settings through the startup `options` parameter (I9):

```
-c bytea_output=hex -c DateStyle=ISO,DMY -c TimeZone=UTC -c extra_float_digits=1
-c IntervalStyle=iso_8601 -c standard_conforming_strings=on
```

Electric pins the same display settings for the same reason: values arrive as text and must
render identically in catch-ups and in the stream, whatever the server's defaults are.
`standard_conforming_strings` is pinned because we build key literals (§4.7).

- **Replication connection:** `pgwire-replication`, in a "gated" session so we can run
  `CREATE_REPLICATION_SLOT` on it before `START_REPLICATION`.
  - It streams with `proto_version '1'` and no in-progress transaction streaming, so each
    transaction arrives whole, at commit, in commit order.
  - It hands us `Begin`/`Commit` as typed events and every other pgoutput message as raw bytes for
    our decoder.
- **Setup connection:** `tokio-postgres`, for checks, publication setup and catalog reads.
- **Catch-up pool:** `WALRUS_CATCHUP_CONNECTIONS` connections (`deadpool-postgres`). The pool
  size is the admission-control limit: the most catch-up load walrus puts on Postgres at once.

### 4.3 Publication

`CREATE PUBLICATION walrus FOR TABLE <configured tables>` if it does not exist. If it exists with
a different table set, `ALTER PUBLICATION ... SET TABLE` happens as part of a new epoch (§4.5).

Electric needs a `LOCK TABLE ... IN SHARE MODE` trick when it adds tables to a publication while
a slot is already streaming. We don't: a table-set change starts a new epoch, which recreates the
slot after the publication is updated. The trick becomes relevant when we add tables without a
new epoch.

### 4.4 Slot

- **Permanent** logical slot, default name `walrus`, created on the gated replication session:
  `CREATE_REPLICATION_SLOT walrus LOGICAL pgoutput (SNAPSHOT 'nothing')`.
- `START_REPLICATION ... LOGICAL 0/0`: Postgres resumes from the slot's confirmed position.
- A lost (invalidated) slot starts a new epoch automatically. Clients then refetch; nothing needs
  to be rebuilt.
- Resetting the engine by hand: stop it and delete the data directory. The next start begins a
  new epoch.
- A permanent slot makes Postgres keep WAL while the engine is down. So the docs and the Docker
  setup set `max_slot_wal_keep_size`, and the engine exports its replication lag (§9).

### 4.5 New epoch (first boot and recovery) (I10)

This runs when:

- there is no initialized store;
- the slot is missing or lost;
- the table set or a table's schema changed (§4.1).

```
1. Drop the slot if it exists; delete the store if it exists.
2. Create the publication, or set its table list (§4.3).
3. Create the slot → consistent point C.
   Postgres waits for all transactions running at this moment to finish before it returns. So:
   - every transaction that commits after C is in the stream;
   - every transaction that committed before C is visible to any snapshot taken later.
4. In one store transaction: create the store with a new random epoch and the table schemas from
   the catalog; set every table's floor = C and last_lsn = C; set initialized = true.
5. START_REPLICATION; serve sessions.
```

No data is copied. Setup takes the same time whatever the database size, because the only wait is
step 3. Every client's first connect in a new epoch is a full catch-up from Postgres (§7.1).

Operational gotcha: step 3 blocks behind long-running or idle-in-transaction sessions. The engine
logs what it is waiting on (from `pg_stat_activity`) every few seconds.

### 4.6 Normal start (store initialized)

1. Open the store and read `last_lsn`.
2. `START_REPLICATION` at `0/0`.
3. Skip every transaction with `commit_lsn <= last_lsn` (I3) and apply the rest.
4. Report ready and serve sessions once the stream is up.

Clients that were connected before the restart resume with offsets close to `last_lsn`, so their
catch-ups are empty or tiny.

### 4.7 Reading rows for catch-ups

Every catch-up read is a `COPY ... TO STDOUT` in text format, inside the session's snapshot
transaction (§7.1):

```sql
-- full table
COPY (SELECT <columns> FROM <table>) TO STDOUT

-- delta, single-column key
COPY (SELECT <columns> FROM <table> WHERE id = ANY('<array literal>'::<type>[])) TO STDOUT

-- delta, composite key
COPY (SELECT <columns> FROM <table> t
      WHERE (t.list_id, t.position) IN
            (SELECT * FROM unnest('<array literal>'::bigint[], '<array literal>'::int[])))
TO STDOUT
```

- **Columns:** `attnum > 0`, not dropped, not generated (pgoutput does not publish generated
  columns), in `attnum` order, so catch-up rows and streamed rows have the same shape. Each
  column's type is stored as `format_type(...)` plus its type OID.
- **Same rendering as the stream:** COPY text format uses the same type output functions as
  pgoutput's text format, so values are identical given I9. We parse COPY's escapes (`\\`, `\t`,
  `\n`, `\r`, `\b`, `\f`, `\v`, `\N` = NULL).
- **Key literals:** COPY takes no bind parameters, so delta keys are inlined as array literals.
  - The keys are the text pgoutput gave us.
  - The literal builder (pure, in `pg/literal.rs`) escapes array-element syntax (`"`, `\`) and SQL
    string syntax (`'` doubled, with `standard_conforming_strings` pinned on).
  - It is unit-tested, and round-trip-tested against Postgres's own parser (§11.3 #13).
- **Batching:** delta keys are read in batches of a constant size (set by benchmark). A row that
  comes back is an upsert. A key that doesn't come back is a delete.
- **Index use:** tests check with `EXPLAIN` that delta reads use the primary-key index, for both
  key shapes.

### 4.8 Decoding pgoutput

`Begin` and `Commit` come from `pgwire-replication` as typed events. Our decoder handles the
rest from raw bytes. In the table below, "index" is what the writer records and "stream" is what
live sessions receive (§6.3):

| Message | Handling |
|---|---|
| `R` Relation | Map relation id → configured table. Compare column names and type OIDs with the stored schema. If they differ, **exit** with "schema of `<table>` changed"; the next start begins a new epoch (§4.1). An unknown relation also exits (it should be impossible given the publication). |
| `I` Insert | Index: key → commit LSN. Stream: upsert. |
| `U` Update | Index: new key → commit LSN. If an old key (`K`) is present, the primary key changed, so the old key → commit LSN as well. Stream: upsert. If any column is marked `u` (unchanged large value, "TOAST"), send a patch instead. On a primary-key change, send the new key's upsert or patch (a patch names the old key as `from`), then a delete of the old key. |
| `D` Delete | Index: key → commit LSN. Stream: delete. |
| `T` Truncate | Per truncated table: raise its floor to this LSN and drop its index entries (I6). Stream: `table-reset` with no rows. |
| `Y` Type, `O` Origin, `M` Message | Ignore. |

The decoder is pure (bytes in, change values out) and unit-tested on captured fixtures.

---

## 5. Change index

### 5.1 Choosing the store (spike, first task of M0)

Candidates:

- **SQLite** (`rusqlite`, bundled, WAL mode, `synchronous=FULL`). This is the default bet: proven,
  inspectable with the `sqlite3` CLI, concurrent readers with one writer.
- **redb** (pure Rust, copy-on-write B-tree, MVCC readers).

Records are small (a key and an LSN). The spike benchmarks our exact workload, nothing else:

1. Apply batches of 1, 10, 100 and 1000 key upserts (with the by-LSN index), fsync per commit:
   commits/s and p99 commit latency.
2. Range scan "keys changed since X" while the writer runs.
3. Retention GC: delete 1M entries in bounded slices while writing.
4. Bytes per entry on disk for uuid, bigint and composite keys; steady-state memory.

The decision and the numbers are recorded in §17. Both candidates expose the same small set of
operations, and the rest of the engine is written against the concrete winner.

**Outcome (M0, 2026-11-24):** we took the default bet, SQLite, without a redb comparison. Quick
numbers on a MacBook (release build):

| Measurement | Result |
|---|---|
| Batches of 1 key per commit | ≈ 7,100 commits/s |
| Batches of 1,000 keys per commit | ≈ 449,000 keys/s |
| Disk per uuid key, both indexes | ≈ 131 bytes |

macOS SQLite does not use `F_FULLFSYNC` by default, so the batch-of-1 figure overstates true
flush throughput there. Production targets Linux. The redb comparison is deferred to Phase 1b,
and only happens if the index store ever shows up as a bottleneck.

Durability note: in WAL mode, SQLite's `synchronous=NORMAL` can lose the last commits on power
failure. Combined with I1, that would lose index entries permanently (Postgres would not resend
them) and break I5, leaving clients stale. So we use `synchronous=FULL` and rely on group commit
for throughput.

### 5.2 Data model (SQLite reference layout)

```sql
CREATE TABLE meta (            -- epoch, last_lsn, initialized, format_version
  key   TEXT PRIMARY KEY,
  value BLOB NOT NULL
) WITHOUT ROWID;

CREATE TABLE synced_tables (
  id        INTEGER PRIMARY KEY,
  name      TEXT NOT NULL UNIQUE,       -- 'public.todos'
  columns   TEXT NOT NULL,              -- JSON: [{name, type, type_oid, pk}]
  floor_lsn INTEGER NOT NULL
);

CREATE TABLE changes (
  table_id INTEGER NOT NULL,
  pk       BLOB    NOT NULL,            -- primary-key values as Postgres text, length-prefixed
  lsn      INTEGER NOT NULL,            -- commit LSN of the key's latest insert, update or delete
  PRIMARY KEY (table_id, pk)
) WITHOUT ROWID;

CREATE INDEX changes_by_lsn ON changes (table_id, lsn);

CREATE TABLE lsn_clock (                -- one sample per minute, for time-based retention
  time_us INTEGER PRIMARY KEY,
  lsn     INTEGER NOT NULL
);
```

There is no row data and no "deleted" flag. A catch-up reads every delta key from Postgres, and a
key Postgres doesn't return is a delete. That is also correct for a row inserted and deleted while
the client was away: deleting a key the client doesn't have is a no-op. (redb equivalent:
`changes: (table_id, pk) → lsn` and `by_lsn: (table_id, lsn, pk) → ()`.)

Delta keys for a table: `SELECT pk FROM changes WHERE table_id = ? AND lsn > ?`, a range scan in
a read transaction where `last_lsn` = `P0`.

### 5.3 Writer

One OS thread. Its input is a bounded channel of decoded events (`Begin`, changes, `Commit`) from
the replication task. Each change is applied as it arrives, using the commit LSN from `Begin`, so
a huge transaction is never held in memory as a whole.

```
loop:
  open store transaction
  repeat:
    take next event (block only if this batch is empty)
    skip the whole transaction if commit_lsn <= last_lsn            -- I3
    per change: upsert (table, key) → commit_lsn; encode its stream op
    on Commit: record last_lsn, close this transaction's encoded frames
  until the channel is empty at a commit boundary, or the batch hits its size cap
  commit (fsync)                                                    -- I1, I2
  acknowledge the last transaction's end LSN to the replication task
  publish each committed transaction's frames in order              -- I4
  record an lsn_clock sample and run a bounded slice of retention GC, if due
```

Per-transaction encoded frames are capped (a constant, e.g. 8 MB). A transaction above the cap
is published as a "catch up" marker instead of its frames: live sessions close, and clients
reconnect and receive it through a catch-up from Postgres, which has backpressure.

### 5.4 Lookups

Delta-key lookups are short read transactions on tokio's blocking pool. A table's delta-key set
is capped (a constant). Above the cap, or above a constant fraction of the table's estimated size
(`pg_class.reltuples`), the table falls back to a full read. That bounds session memory, and is
cheaper anyway once most rows have changed. Both constants come from benchmark S7. The MVP
implements the absolute cap only (`DELTA_CAP`); the fraction threshold waits for S7's numbers.

### 5.5 Retention and floors

- Every minute (a constant), the writer records `(now, last_lsn)` in `lsn_clock`.
- Retention GC (config, default 7 days):
  1. Take the newest sample older than `now − retention`; call its LSN R.
  2. Raise every table's floor to at least R.
  3. Then delete index entries with `lsn <= R` in bounded slices, and prune old samples.

  Raising the floor first keeps I5 true at every step.
- Truncate also raises the floor (§4.8).
- A client whose offset is below a table's floor gets that table in full (§6.4).

---

## 6. Wire protocol (v0)

### 6.1 Request

```
GET /v1/stream?tables=public.todos,public.lists[&offset=<offset>]
Last-Event-ID: <offset>          (optional; browsers send it automatically on reconnect)
```

- The offset comes from the `Last-Event-ID` header or the `offset` query parameter. The header
  wins: a browser's `EventSource` sends it on automatic reconnects. The query parameter is how a
  persisted client resumes on a fresh page load, where `EventSource` can't set headers.

- `tables`: comma-separated, schema-qualified, must be a subset of the configured tables.
- Response headers: `Content-Type: text/event-stream`, `Cache-Control: no-store`,
  `X-Accel-Buffering: no` (stops nginx-style proxies from buffering the stream).
- Errors before the stream starts:
  - `400` for an unknown table or a malformed offset;
  - `503` + `Retry-After` while the engine is not ready (new epoch being set up, stream not up
    yet).

### 6.2 Offsets

`<epoch as 16 hex digits>-<LSN as 16 hex digits>`, e.g. `9f2c4e1a7b3d5f60-00000000016b3740`.
Clients treat it as opaque. An unparsable offset is a `400`. An epoch mismatch, or an LSN above
the store's `last_lsn`, is treated as "no offset".

### 6.3 Events

| Event | Data | Meaning |
|---|---|---|
| `retry: 1000` | (field) | Sent first: reconnect after 1 s. |
| `table-reset` | `{"table", "columns": [{"name", "type", "pk"}]}` | Drop all local rows of this table; here is its schema. |
| `rows` | `{"table", "ops": [op, ...]}` | Row operations, applied in order (below). Chunked (a constant, e.g. ≤ 1000 ops). |
| `commit` | `{"offset"}`, `id: <offset>` | Apply everything since the previous `commit` atomically; you are now at `offset`. |
| `: keepalive` | (comment) | Every 15 s, so proxies don't close idle streams. |

Row operations. Rows are arrays aligned with the schema's column order, with values as Postgres
text or `null`. Keys are arrays of the primary-key values in key-column order.

| Op | Meaning |
|---|---|
| `["u", row]` | Upsert: insert the row, or replace it. |
| `["d", key]` | Delete the row with this key. Deleting an unknown key is a no-op. |
| `["p", row, keep]` | Patch: replace the row, except that the columns whose indexes are in `keep` keep their current values (`row` has `null` there). Used when an update left large values unchanged. |
| `["p", row, keep, from]` | Same, for an update that also changed the primary key: the kept values come from the row at key `from`, which the next op deletes. |

Patches appear only on the live stream, where a session always holds the row: it has received
the table's full state, and an update implies the row exists. Catch-ups read whole rows from
Postgres, so they contain only `u` and `d`. This replaces `REPLICA IDENTITY FULL`, which would
make Postgres write the whole old row to the WAL on every update and delete (ADR 0001).

`commit` carries a `data` line because browsers do not dispatch SSE events with empty data.

Example (fresh client, then one live transaction):

```
retry: 1000

event: table-reset
data: {"table":"public.todos","columns":[{"name":"id","type":"bigint","pk":true},{"name":"title","type":"text","pk":false},{"name":"done","type":"boolean","pk":false}]}

event: rows
data: {"table":"public.todos","ops":[["u",["1","milk","f"]],["u",["2","eggs","t"]]]}

event: commit
id: 9f2c4e1a7b3d5f60-00000000016b3740
data: {"offset":"9f2c4e1a7b3d5f60-00000000016b3740"}

event: rows
data: {"table":"public.todos","ops":[["u",["2","eggs","f"]],["d",["1"]]]}

event: commit
id: 9f2c4e1a7b3d5f60-00000000016b3790
data: {"offset":"9f2c4e1a7b3d5f60-00000000016b3790"}
```

A patch that keeps an unchanged large `title` (column 1): `["p",["2",null,"t"],[1]]`.

### 6.4 Catch-up rules (per requested table)

| Client offset | Mode | Sends |
|---|---|---|
| None / invalid / other epoch | full | `table-reset` + every row |
| Below the table's floor | full | `table-reset` + every row |
| At or above the floor, no keys changed since | none | nothing (Postgres is not queried) |
| At or above the floor, keys changed | delta | `u` for each changed row Postgres returns, `d` for each changed key it doesn't |
| At or above the floor, delta above the cap or threshold (§5.4) | full | `table-reset` + every row |

Then one `commit` at `P0` (§7.1), then live transactions.

### 6.5 Client obligations

- Buffer `table-reset` and `rows` until `commit`, then apply atomically and remember the offset.
- Apply ops in order, as defined in §6.3.
- A patch for a row the client doesn't hold means the client's state is broken. The client drops
  its offset and reconnects fresh. This is defensive; the server guarantees it can't happen.
- A closed stream means "reconnect with your offset". The server closes streams on purpose for
  slow clients, oversized transactions and shutdown. Recovery is always the same catch-up.

---

## 7. Sessions and fan-out

### 7.1 Session algorithm (I7, I8)

Postgres is queried on behalf of a client only to read rows the client is missing. It is never
queried when nothing changed, and always goes through the catch-up pool.

```
1. Validate tables and offset; pick each table's mode (§6.4).
2. rx = live.subscribe()                                  -- before anything else (I7)
3. Store read transaction: P0 = last_lsn; per delta table, K_t = keys with lsn > offset.
   Tables over the cap or threshold (§5.4) switch to full.
4. If there are no full tables and every K_t is empty:
     commit(P0); go to 6.                                 -- Postgres not queried
5. Take a catch-up connection (waits while all are busy):
     BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
     SELECT pg_current_snapshot();                        -- snapshot S
     S can't see an applied transaction with lsn <= P0 (§7.2)
       → ROLLBACK, back off briefly, retry (bounded)
     full tables:  table-reset, COPY the whole table
     delta tables: COPY K_t in batches → "u" for rows returned, "d" for keys not returned
     COMMIT; return the connection.
     commit(P0)
6. Live loop, starting with the join filter on (if step 5 ran):
     transaction from rx:
       lsn <= P0                → skip
       "catch up" marker        → close
       filtering and xid visible in S
                                → drop its ops on full tables and on keys in K_t; send the rest
       filtering and xid >= S.xmax
                                → filtering off (this and later transactions began after S)
       otherwise                → write its frames for the subscribed tables + commit
     rx lagged                  → close
     15 s idle                  → keepalive
```

If `rx` lags while step 5 is still reading (for example a large full catch-up under heavy
writes), the session still finishes the catch-up and sends `commit(P0)`, then closes. The client
resumes from `P0` with a small delta instead of starting over, so a big catch-up can't livelock.

**Why this is correct.** Every transaction falls into exactly one case:

- **Committed at or before `P0`:**
  - If it changed a key after the client's offset, that key is in `K_t` and its row comes from
    snapshot S.
  - If it is at or before the offset, the client already has it.
- **Committed after `P0`:** it arrives on `rx`.
  - **Visible in S:** S already holds its effect on the rows the catch-up read, so those ops are
    dropped. Its other ops are sent, because those rows were not read.
  - **Not visible in S:** sent whole.

The one gap is a transaction at or before `P0` that is *not* visible in S: it would be in neither
the snapshot nor `rx`. §7.2 explains when that happens and how step 5 rules it out.

`commit(P0)` is a valid offset after a catch-up. The client has everything up to `P0`, plus
possibly some newer rows from S. A later reconnect from `P0` re-reads every key changed after
`P0`, so the newer rows are harmless.

Visibility uses a wraparound-aware 32-bit comparison, Postgres's own rule:
`precedes(a, b) = (a - b) as i32 < 0`. The streamed xid is 32-bit; the snapshot's xids are
64-bit and are truncated.

```
visible(xid) = precedes(xid, xmin) || (precedes(xid, xmax) && xid ∉ in_progress)
```

### 7.2 Commits that are streamed before they are visible

A backend commits in this order (`RecordTransactionCommit` in `xact.c`):

1. write and flush the commit record;
2. mark the transaction committed in the commit log (`pg_xact`);
3. with synchronous replication, wait for the standby;
4. leave the process array, which makes it visible to new snapshots.

The walsender can decode and stream a commit as soon as step 1 is done. So the engine can index
and broadcast a transaction that a snapshot taken just afterwards still can't see. With
synchronous replication the window lasts at least a standby round trip, and indefinitely if the
standby is down. Test §11.3 #4 reproduces this: with a missing synchronous standby, the engine
indexes a commit that other sessions cannot see yet.

**Postgres can't tell a session about such a transaction** (verified, 2026-11-25):

- Snapshot `xmax` is one past the latest *completed* xid. When the waiting transaction has the
  highest xid, it is at or above `xmax` and therefore not in the snapshot's in-progress list at
  all (observed: snapshot `19190:19190:` while xid 19190 waited).
- `pg_xact_status` reports a transaction that waits for its standby as `in progress`, not
  `committed`.

So the check is the engine's (`pg/xid.rs` `RecentXids`):

- The writer records every applied transaction as (xid, commit LSN). It widens the streamed
  32-bit xid to 64 bits against a reference: `pg_current_xact_id()` at startup, then the highest
  xid seen. It records each one before the index commit that covers it.
- After taking S, a session rejects S if any recorded transaction with commit LSN `<= P0` is not
  visible in S, comparing 64-bit xids exactly. It then retries with a later snapshot.
- A usable S prunes every entry below its `xmin`: those transactions have finished, so they are
  visible to every later snapshot. A cap bounds memory when no catch-ups run for a long time.

Retries are counted in the metrics (§9).

The check now covers the window from step 1, so no part of it is left open. What remains is
narrower:

- **Restart:** a transaction applied before an engine restart and still invisible after it isn't
  recorded. That requires a standby stall that outlasts the restart.
- **Cap:** an entry dropped by the cap is no longer checked. That requires more than 2^18 newer
  transactions while one stays invisible.

Both are listed in §14.

### 7.3 Live fan-out

- `tokio::sync::broadcast` ring of `Arc<EncodedTxn>` (constant capacity). The writer publishes
  after commit. Receivers that fall too far behind get `Lagged` and are closed, which bounds memory
  without growing per-client queues.
- `EncodedTxn` holds, per table it touched:
  - ready-to-send SSE frame bytes, plus the `commit` frame;
  - its ops' keys, which sessions need only while the join filter is on.
- A session writes the frames for its tables, with no per-client serialization. Only during the
  join does a session re-encode a transaction's frames, and only when it drops some of its ops.
- The HTTP response is a streaming body of `Bytes` frames written directly (SSE framing is
  trivial), not per-client event objects.

### 7.4 Backpressure

- **Replication → writer:** a bounded channel. When the writer is behind, the replication task
  waits. `pgwire-replication` keeps sending keepalives so Postgres doesn't time us out, and TCP
  flow control slows Postgres's sender.
- **Catch-up → socket:** each session has a bounded channel, so COPY follows the client's pace. A
  catch-up has a time limit (a constant), so a slow client cannot hold a catch-up connection and a
  Postgres snapshot open indefinitely. Clients that hit it are closed and resume.
- **Live → socket:** the broadcast ring. Lagging clients are closed and recover through a
  catch-up.

### 7.5 HTTP endpoints

| Route | Purpose |
|---|---|
| `GET /v1/stream` | §6 |
| `GET /health` | Process alive. |
| `GET /ready` | Store initialized and replication streaming. |
| `GET /metrics` | §9 |

### 7.6 Shutdown

1. Stop accepting connections.
2. Close sessions.
3. The writer commits its current batch and acknowledges it.
4. Stop replication. The slot stays (it is permanent).

---

## 8. Configuration

Environment variables, nothing else:

| Variable | Default | |
|---|---|---|
| `WALRUS_DATABASE_URL` | (required) | Postgres connection string. |
| `WALRUS_TABLES` | (required) | `public.todos,public.lists` |
| `WALRUS_DATA_DIR` | `./data` | Change index location. |
| `WALRUS_LISTEN_ADDR` | `127.0.0.1:4000` | HTTP. |
| `WALRUS_SLOT` | `walrus` | Slot names are cluster-wide, so tests need unique ones. |
| `WALRUS_PUBLICATION` | `walrus` | |
| `WALRUS_CATCHUP_CONNECTIONS` | `4` | Postgres connections used for catch-ups, i.e. the most catch-up load walrus puts on your database at once. |
| `WALRUS_RETENTION` | `7d` | How long a client can be offline and still get a delta instead of full tables. |

Buffer sizes, chunk sizes, batch caps, delta caps and thresholds, time limits and intervals are
constants in code, set from benchmark results.

---

## 9. Observability

`tracing` for logs. `/metrics` serves Prometheus text format from hand-rolled atomic counters and
fixed-bucket histograms (no metrics framework needed):

- **Replication:** server WAL end (from keepalives), applied LSN, **lag in bytes**,
  transactions/s, row changes/s, skipped (replayed) transactions.
- **Writer:** batch size, store commit latency histogram, queue depth, index entries, GC
  deletions.
- **Catch-ups:**
  - count by mode (none / delta / full) and full fallbacks;
  - delta keys per table (histogram);
  - rows read from Postgres and Postgres read time (histogram);
  - join retries (§7.2);
  - waits for a catch-up connection (queue length, wait time), time-limit closes.
- **Sessions:** active, closes by reason (lagged, oversized, time limit, client gone).
- **Process:** RSS, index size on disk.

---

## 10. Performance

The MVP measures a **baseline** on the test fixture (§11.1), so performance is tracked from the
first working version. The full performance and data-variety suite is a substantial piece of work
of its own, so it is a separate phase right after the MVP (Phase 1b, §16).

### 10.1 Targets (proposed; finalised in Phase 1b)

| Metric | Target (single node, local Postgres) |
|---|---|
| Postgres commit → client receives event | p50 ≤ 25 ms, p99 ≤ 100 ms at steady load |
| Sustained ingest | ≥ 10k row changes/s without growing lag |
| Concurrent SSE clients | ≥ 10k, idle session ≤ 64 KB memory |
| Reconnect with nothing changed | no Postgres query |
| Delta catch-up | cost proportional to changed rows (report bytes and Postgres time per changed row) |
| Full catch-up to one client | report time, bytes and Postgres time for 1M rows |
| New-epoch setup | independent of database size |
| Memory | flat regardless of table size and offline duration |

### 10.2 MVP baseline (`walrus-bench`)

`walrus-bench` is a load generator plus simulated clients, built on `walrus-client`. Latency is
measured end to end, on the same machine and clock:

1. The writer stores its own send timestamp (µs) in a column just before commit.
2. Each client computes `received − sent`.

| # | Scenario | Measures |
|---|---|---|
| B1 | Steady 100 txn/s against the fixture, 10 clients | Commit → client p50/p95/p99 |
| B2 | Writes as fast as possible for 30 s | Sustained change rate, replication lag, store commit latency |
| B3 | Clients reconnect after N writes, delta vs forced full | Bytes, Postgres time, time until up to date |

Recorded runs go to `bench/results/<date>-<commit>.{md,json}` with hardware details, the latest is the baseline (`bench/baseline.json`) and `bench/HISTORY.md` tracks them over time; dev runs (`scripts/bench.sh`) compare against it without recording. Micro-benchmarks
(`criterion`) cover the decoder, the COPY parser, the key-literal builder and the frame encoder.

### 10.3 Phase 1b suite (planned, not part of the MVP)

Datasets:

- small and large tables, up to 10M rows;
- wide rows;
- hot rows updated at high frequency;
- many tables;
- long-lived connections (hours to days).

Scenarios:

| # | Scenario | Measures |
|---|---|---|
| S1 | Ingest ramp: small-row writes from 1k to 50k changes/s | Max sustainable rate, lag, commit latency |
| S2 | Steady 1k txn/s, 100 clients | Commit → client p50/p95/p99 |
| S3 | 1k and 10k clients, steady writes | RSS, CPU, latency under fan-out |
| S4 | Reconnect storm: 5k clients reconnect at once after N writes | Time until all caught up; Postgres load; live latency for others meanwhile |
| S5 | Full catch-up of 1M rows to one client | Time, bytes, Postgres time |
| S6 | One transaction updating 1M rows | Peak memory, apply time, client recovery |
| S7 | **State-model validation (ADR 0001)**, matrix below | See below |

S7 measures delta against forced full on the real engine, and estimates the full row store from
the spike. It sets the delta caps and thresholds (§5.4).

| Dimension | Values |
|---|---|
| Table size | 1k, 100k, 1M rows |
| Churn | 0.1%, 10%, 100% |
| Clients reconnecting at once | 1, 1,000 |

For each combination it measures:

- bytes sent;
- Postgres CPU and query time (`pg_stat_statements`);
- engine CPU, memory and disk;
- time until up to date;
- live latency for other clients.

Each scenario's worst case is profiled (`samply`/flamegraph) and its top bottlenecks fixed.
Postgres 15, 16 and 18 join the test matrix in this phase.

---

## 11. Testing

### 11.1 Environment and fixture

`docker-compose.yml` runs `postgres:17` on port `54329` with data on `tmpfs` and these settings:

- `wal_level=logical`;
- `max_replication_slots=64`, `max_wal_senders=64`;
- `max_slot_wal_keep_size=2GB`;
- `shared_preload_libraries=pg_stat_statements`.

Each integration test creates its own database, slot name and temp data directory, so tests run
in parallel. Leftovers disappear with the container.

Every test starts from the same small fixture: three tables, 50 rows each, seeded
deterministically. No foreign keys, so tests can delete in any order. The columns cover the types
and shapes the engine must handle:

- uuid, composite and bigint keys;
- text with tabs, newlines, backslashes and unicode;
- NULLs, NaN and ±Infinity;
- arrays and json;
- large values.

```sql
CREATE TABLE users (
  id         uuid PRIMARY KEY,
  email      text NOT NULL,
  name       text,
  created_at timestamptz NOT NULL,
  avatar     bytea
);

CREATE TABLE lists (
  id       bigint PRIMARY KEY,
  owner_id uuid NOT NULL,
  title    text NOT NULL,
  archived boolean NOT NULL DEFAULT false,
  settings jsonb
);

CREATE TABLE todos (
  list_id  bigint NOT NULL,
  position int    NOT NULL,
  title    text   NOT NULL,
  done     boolean NOT NULL,
  due      date,
  estimate interval,
  priority numeric(5,2),
  score    double precision,
  tags     text[],
  notes    text,                 -- some rows get large values (see below)
  PRIMARY KEY (list_id, position) -- composite key
);

-- Store large notes out of line without compression, so the "unchanged large value" (TOAST)
-- path is exercised deterministically.
ALTER TABLE todos ALTER COLUMN notes SET STORAGE EXTERNAL;
```

### 11.2 Unit tests

- decoder: captured fixtures from a real Postgres, plus hand-built edge cases;
- COPY text parser: every escape;
- key-literal builder;
- xid visibility, including wraparound;
- offset parsing;
- frame encoding;
- store operations (temp directory).

### 11.3 Integration tests (real Postgres)

1. **New epoch, then live:** inserts, updates and deletes reach the client.
2. **Join on first connect:**
   - writers run throughout while clients connect (full catch-ups);
   - the workload includes a transaction that stays open across the snapshot;
   - repeated many times, and every client must equal `SELECT *`.
3. **Join on reconnect:** the same, with clients resuming from offsets (delta catch-ups). The test
   also asserts that only the delta keys were read from Postgres.
4. **Commit streamed before visible (§7.2):**
   - Set `synchronous_standby_names` to a standby that doesn't exist, so commits wait after
     flushing.
   - The engine streams such a commit. A session connecting meanwhile must not complete its
     catch-up.
   - After the wait is cancelled, the commit becomes visible and the client converges.
   - This also proves the Postgres behavior §7.2 relies on.
5. **Slot creation waits for in-flight transactions:** proves the property §4.5 step 3 relies on.
6. **Restart:** stop the engine, write, start it again. Reconnecting clients get only the delta.
7. **Crash during new-epoch setup:** the next start redoes it.
8. **Nothing changed:** a reconnect makes no Postgres query (catch-up mode `none`).
9. **Floors:** an offset below the floor (after retention GC, after truncate) gets `table-reset`
   for that table only. An offset from another epoch gets everything in full.
10. **Deletes while offline:** deleted, re-inserted, and inserted-then-deleted rows all converge.
11. **Large values and keys:**
    - an update leaving a large value unchanged arrives as a patch;
    - a primary-key change with an unchanged large value arrives as a patch with `from`;
    - a multi-table transaction is applied atomically.
12. **Type rendering:** every fixture column renders identically through a full COPY, a delta COPY
    and the stream.
13. **Key literals:** random strings round-trip through the literal builder and Postgres
    unchanged. The strings include quotes, backslashes, braces, commas, unicode and the empty
    string. `EXPLAIN` shows delta reads use the primary-key index for single and composite keys.
14. **Slow client, and lag during a catch-up:** the session is closed, the client resumes from
    `P0` and converges, and engine memory stays bounded.
15. **Oversized transaction:** live clients are closed and converge through a catch-up.
16. **Large delta:** falls back to a full read.
17. **Startup and lifecycle:**
    - a table without a primary key, an unsupported key type, or the wrong `wal_level` gives a
      clear error;
    - a schema change makes the engine exit, and its next start begins a new epoch;
    - a lost slot or a table-set change starts a new epoch.

### 11.4 Model test (the main correctness proof)

A seeded random workload over the fixture: a few hundred operations, with the seed printed on
failure for replay. It mixes:

- inserts, updates (including primary-key changes and large values), deletes and truncates;
- multi-table transactions;
- random client connects, disconnects and resumes;
- random engine restarts;
- retention GC with a short retention.

At the end the test waits until every client is at the engine's `last_lsn`. It then compares every
client's state with `SELECT *`, rendered with the pinned settings. CI runs it across many seeds.

---

## 12. Repository layout

```
walrus/
  Cargo.toml                workspace
  PLAN.md
  docs/decisions/           architecture decision records (0001: state model)
  docker-compose.yml
  crates/
    walrus-protocol/        wire types, offsets, event payloads (serde only; WASM-friendly,
                            shared with future clients)
    walrus-engine/          library + `walrus` binary
      src/
        main.rs             load config, run
        lib.rs              run(config, listener, shutdown)
        config.rs
        pg/
          setup.rs          checks, publication, catalog, session settings, new epoch
          replication.rs    slot, streaming, ack forwarding
          decode.rs         pgoutput → changes (pure)
          read.rs           full and delta COPY reads, snapshot check, catch-up pool
          copy_text.rs      COPY text parser (pure)
          literal.rs        key array literals (pure)
          xid.rs            visibility (pure)
        change_index.rs     schema, apply, lookups, retention GC
        writer.rs           single-writer loop, group commit, ack, publish
        encode.rs           changes → SSE frames
        session.rs          catch-up, join, live per client
        http.rs             routes
        metrics.rs
      tests/                integration + model tests
    walrus-client/          minimal native client: SSE parsing, apply to in-memory tables,
                            offsets. Used by tests and benchmarks; seed of the Phase 2 client.
    walrus-bench/           load generator, simulated clients, scenarios, reports
```

### Dependencies (and why)

| Crate | Why |
|---|---|
| `tokio` | Runtime for network I/O. |
| `axum` | HTTP routing and streaming response bodies. |
| `pgwire-replication` | Replication protocol, keepalives, acknowledgements. |
| `tokio-postgres` | Checks, catalog, COPY reads. |
| `deadpool-postgres` | Catch-up connection pool; its size is the admission-control limit. |
| `rusqlite` (bundled) or `redb` | Change index store, decided by the spike. |
| `serde`, `serde_json` | Wire payloads. |
| `bytes` | Zero-copy decoding and shared frames. |
| `tracing`, `tracing-subscriber` | Logs. |
| `anyhow` | Errors at boundaries. |
| `getrandom` | Epoch. |
| dev: `reqwest` (stream), `rand` (seeded), `criterion` | Client, model test, micro-benchmarks. |

Not used on purpose:

- ORMs;
- actor frameworks;
- metrics frameworks;
- testcontainers (compose is simpler and also serves manual runs).

---

## 13. Milestones

Each milestone ends with its tests passing. Nothing is "done" on manual checks alone.

| Milestone | Contents | Exit criteria |
|---|---|---|
| **M0 Foundations** | Workspace, compose, CI script (fmt, clippy, test), per-test database helpers, **index store spike** | Spike numbers and decision recorded in §17 |
| **M1 Replication & decoding** | Startup checks, settings, publication, slot, streaming, decoder for all messages | Decoder unit tests; an integration test receives correct decoded changes for insert/update/delete/truncate/TOAST/PK change |
| **M2 Change index & writer** | Index store, writer with group commit, ack after commit, idempotent skip, new-epoch setup | The index equals the set of keys the test changed after the floor, across restarts (§11.3 #5, #7) |
| **M3 Full catch-ups & live** | `walrus-protocol`, encoder (with patches), HTTP, sessions with full catch-ups, the join and the §7.2 check, fan-out, backpressure, `walrus-client` | Clients that connect under concurrent writes converge (#1, #2, #4, #11, and #12 for full COPY and the stream). This is option 1 of ADR 0001, our baseline. |
| **M4 Delta catch-ups** | Delta-key lookup, key literals, batched delta COPY, per-key join filter, empty-delta skip, full fallback | #3, #8, #13, #16; a resume reads only the delta keys from Postgres |
| **M5 Lifecycle & edge cases** | Restart, truncate, floors, retention GC, new-epoch triggers, readiness, metrics | §11.3 complete; model test green across many seeds |
| **M6 Baseline** | `walrus-bench` B1–B3, micro-benchmarks, report | Baseline recorded in `bench/results/`; obvious bottlenecks found by profiling fixed |

**MVP done** = M0–M6 exit criteria met. Phase 1b (performance, data variety and the S7
validation) starts right after.

---

## 14. Risks

| Risk | Mitigation |
|---|---|
| Postgres load from catch-ups (new clients on large tables, reconnect storms) | Catch-up connection cap, empty-delta skip, measured in S4/S5/S7. Later: cache full catch-ups (ADR 0001). |
| Join correctness, now on every catch-up that reads Postgres | One owner (I7, I8); tests #2–#4; model test across many seeds. |
| A transaction applied before an engine restart and still invisible after it, or dropped from the applied-xid record by its cap (§7.2) | Accepted for the MVP. Fix if ever observed: persist the record, or prune it from a periodic `xmin` query instead of capping. |
| The join stops filtering at the first transaction with xid ≥ `xmax`. A later transaction can still be visible in S when an earlier commit waits for a synchronous standby while an asynchronous one completes. It is then re-sent: harmless for upserts and deletes, but a patch whose `from` row the catch-up removed breaks the client, which then resyncs | Rare (needs mixed synchronous and asynchronous commits right at a catch-up). Fix if it shows up: keep filtering by exact visibility for a bounded time. |
| Slow clients hold a catch-up connection and a snapshot open | Catch-up time limit; closed clients resume; measured. |
| Long catch-up snapshots hold back vacuum | Bounded by the time limit; measured in S5. |
| Delta reads not using the primary-key index (composite keys, planner estimates) | `EXPLAIN` test (#13); batch size set by benchmark. |
| Index store commit throughput with fsync | Group commit; measured first in the spike. |
| WAL piles up while the engine is down (permanent slot) | `max_slot_wal_keep_size`, lag metric; a lost slot starts a new epoch automatically. |
| Slot creation blocked by long-running transactions | Log the blocking sessions; documented. |
| Catch-up vs stream rendering differences | Pinned settings on every connection; type-rendering test. |
| Primary-key types with non-canonical text | Allowlist at startup (§4.1). |
| `pgwire-replication` is a young crate | Confined to `pg/replication.rs`; integration tests; small enough to fork if needed. |
| Unknown target scale | Measured in Phase 1b; targets are provisional until then. |

---

## 15. Open questions

1. **Retention default:** proposal 7 days, i.e. how long a client can be offline and still get a
   delta instead of full tables.
2. **Target scale** (largest table, writes per second, clients, offline duration): answered by
   the Phase 1b measurements. The targets in §10.1 stay provisional until then.
3. **Delta caps and thresholds** (§5.4): set by S7.

---

## 16. After the MVP (for context, not planned in detail)

- **Phase 1b:** performance, data variety and state-model validation (§10.3); Postgres 15, 16 and
  18 in the test matrix.
- **Phase 2:** client library (native + WASM over `EventSource`), local persistence, resume by
  offset.
- **Phase 3:** Leptos bindings and a demo app.
- **Then, roughly in order of need:**
  - per-table re-sync on schema changes, and adding tables at runtime, both without a new epoch;
  - WHERE-clause shapes and auth (see below);
  - caching full catch-ups (compressed snapshots, CDN);
  - optimistic writes confirmed by LSN: the write endpoint returns `pg_current_wal_insert_lsn()`
    after commit, and the engine advances its offset on idle keepalives;
  - TLS to Postgres, CORS, compressed GET catch-up;
  - multiple instances.

How WHERE-clause shapes and auth fit this design:

- Catch-up reads already apply filters in Postgres. A key that no longer matches comes back
  absent, which means delete.
- The live path must evaluate filters in the engine.
- A row moving *into* a shape with unchanged large values needs a read from Postgres.
- Primary keys of rows outside a client's scope must not leak.

---

## 17. Decision log

| Date | Decision |
|---|---|
| 2026-11-17 | Own protocol, all Rust, SSE only. |
| 2026-11-17 | ~~History model B: latest state per row + tombstones + floors.~~ Superseded 2026-11-24 by ADR 0001. |
| 2026-11-17 | Durable store from day one; permanent slot; acknowledge after commit. Still holds; the store now holds the change index. |
| 2026-11-17 | Snapshot/stream join via xid filtering (Electric's method); whole tables; default replica identity. Still holds; the join now runs on every catch-up that reads Postgres. |
| 2026-11-24 | Postgres 15–18 supported; MVP CI on 17. |
| 2026-11-24 | MVP tests use a small fixture (3 tables × 50 rows); the full performance suite is Phase 1b. |
| 2026-11-24 | **State model: change index in the engine, row data read from Postgres** ([ADR 0001](docs/decisions/0001-state-model.md)). |
| 2026-11-24 | Unchanged large values are sent as patches; `REPLICA IDENTITY FULL` is never required. |
| 2026-11-24 | A lost slot, a table-set change or a schema change starts a new epoch automatically. |
| 2026-11-24 | Change index store: SQLite (default bet), numbers in §5.1. |
| 2026-11-25 | The §7.2 snapshot check is the engine's (applied xids), not `pg_xact_status`: Postgres can't report a commit that is streamed but not yet visible. |
| 2026-11-25 | Offsets also accepted as the `offset` query parameter (header wins). |
