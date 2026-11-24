# ADR 0001: State model — a change index in the engine, row data in Postgres

- **Status:** Accepted
- **Date:** 2026-11-24
- **Supersedes:** "History model B" and the engine row store (PLAN.md decision log, 2026-11-17)

## Context

Walrus streams Postgres changes to clients. While a client is connected that part is simple:
forward each committed transaction. The decision that shapes the product is what happens when a
client **(re)connects**: how does it get from what it has to the current state, and what does the
engine keep to make that possible?

Five facts frame the decision:

1. **Clients need end state, not history.** A client that was away doesn't need to know which
   transactions ran, only what the rows look like now and which rows are gone.
2. **Local-first clients reconnect constantly.** They persist their data and come back many times
   a day: tab reloads, laptops waking, phones changing networks, engine deploys. Reconnecting is
   the common case.
3. **The primary Postgres is the scarce resource.** It serves the whole application, is hard to
   scale out, and is usually the most expensive machine in the system. Engine instances and their
   disks are cheap and replaceable.
4. **Egress is billed.** Most cloud providers charge per GB leaving their network, so every byte
   sent to clients is a running cost, not just latency.
5. **Postgres stores and serves rows better than we will.** Its storage, indexes, buffer cache
   and planner are decades of work. We should not build a weaker copy of them.

Every design needs three things, so they don't separate the options:

- the live stream: one replication slot, fanned out to sessions;
- a **full catch-up** (whole tables) for brand-new clients and clients whose offset is too old;
- a **join** that splices a catch-up with the live stream without losing or duplicating changes.

The options differ in how a *returning* client, one that already has data and an offset, is
caught up.

Notation: **S** = rows in the tables a client subscribes to. **D** = rows inserted, updated or
deleted since the client's offset. For a client that reconnects often, D ≪ S.

## Options

### Option 1: Stateless engine (full refetch)

The engine keeps no data. It forwards the live stream, and every connect, including every
reconnect, gets a full catch-up read from Postgres.

- **Reconnect cost:** S rows sent; Postgres reads S rows.
- **Engine storage:** none.
- **Strengths:**
  - simplest possible engine;
  - nothing to keep consistent or size.
- **Weaknesses:**
  - Reconnect cost grows with data size, not change size.
  - Persisted client data is thrown away and resent on every reconnect.
  - 1,000 clients reconnecting after an engine deploy means 1,000 full reads of the same tables
    from the primary.

### Option 2: Full row store in the engine

The engine keeps the latest state of every synced row, plus tombstones for deletes, in its own
durable store indexed by LSN, and serves every catch-up from it. This was the previous plan
("model B"). Zero (a full SQLite replica) and PowerSync (bucket data in a separate database) are in
this family. Electric keeps per-shape logs of row data on disk.

- **Reconnect cost:** D rows sent; no Postgres work.
- **Engine storage:** a second copy of every synced row, plus tombstones and indexes. It grows
  with the database.
- **Strengths:**
  - cheapest reconnect possible;
  - Postgres only ever sees the replication stream;
  - catch-ups keep working while Postgres is down.
- **Weaknesses:**
  - **It duplicates the database.** A 150 GB database means roughly 150 GB more next to the
    engine.
  - First boot and every recovery copy the whole database, which takes hours on large databases.
  - The copy can drift from Postgres through any decoding bug, and clients then converge to our
    copy instead of Postgres's.
  - It is the most code: in effect a storage engine of our own.

### Option 3: Change index in the engine, rows from Postgres

The engine keeps only **which keys changed when**: for each `(table, primary key)`, the commit
LSN of its latest change (insert, update or delete). Row data stays in Postgres only.

A client reconnecting with offset Y:

1. The engine looks up the keys whose LSN is > Y in the index (a range scan, cost ∝ D).
2. If there are none, nothing is sent and **Postgres is not queried**.
3. Otherwise the engine reads those keys from Postgres by primary key, in batches (index lookups,
   cost ∝ D). Rows that come back are sent as upserts. Keys that don't come back are deletes.

- **Reconnect cost:** D rows sent; Postgres reads D rows by primary key, or nothing if D = 0.
- **Engine storage:** a few dozen bytes per key changed within the retention window (key + LSN +
  index overhead, to be measured). It grows with write rate × retention, not with database size.
- **Strengths:**
  - delta-sized network and Postgres cost;
  - no copy of the data;
  - instant engine boot at any database size;
  - catch-ups always return Postgres's actual rows.
- **Weaknesses:**
  - Postgres is queried on catch-ups (bounded by a connection cap).
  - Every catch-up that reads Postgres needs a join with the live stream.
  - Catch-ups need Postgres to be up.

## Comparison

| | 1. Stateless | 2. Full row store | 3. Change index |
|---|---|---|---|
| Reconnect: bytes sent | S | D | D |
| Reconnect: Postgres work | Read S rows | None | Read D rows by key; none if D = 0 |
| New client | Full read from Postgres | Full read from engine store | Full read from Postgres |
| Engine disk | 0 | ≈ synced data + tombstones + indexes | ≈ tens of bytes × keys changed within retention |
| Engine first boot / recovery | Instant | Copy every synced table (hours on large DBs) | Instant (empty index, new epoch) |
| 1,000 clients reconnect after an engine deploy | 1,000 full reads | Almost free | Mostly no queries (D ≈ 0) |
| Copy of the data that can drift | None | The whole store | None (keys only) |
| Catch-ups while Postgres is down | No | Yes | No |
| Joins of a Postgres snapshot with the stream | Every connect | First boot only | Every catch-up that reads Postgres |
| Future WHERE shapes / per-user filtering | Postgres applies filters | Engine applies filters over its store | Postgres applies filters at read time |
| Implementation complexity | Lowest | Highest | Medium |

## Worked example (illustrative)

A client's tables hold 100k rows of ~200 bytes, so S = 20 MB. Between two sessions 0.1% of rows
changed, so D = 100 rows = 20 KB.

| | One reconnect | 1,000 clients reconnecting |
|---|---|---|
| Option 1 | 20 MB sent; Postgres reads 100k rows | 20 GB sent; Postgres reads 100M rows |
| Option 2 | 20 KB sent; no Postgres work | 20 MB sent; no Postgres work |
| Option 3 | 20 KB sent; Postgres reads 100 rows by key | 20 MB sent; Postgres reads 100k rows by key |

Storage for a 150 GB database (say 500M rows):

- **Option 2:** roughly another 150 GB next to the engine.
- **Option 3:** if 25M distinct rows change within a 7-day retention window, about
  25M × ~80 B ≈ 2 GB.

When churn approaches 100%, D approaches S and the delta advantage disappears. Option 3 then reads
that table in full instead of key by key. The thresholds come from benchmarks.

## Decision

**Option 3: the engine keeps a durable change index; row data is read from Postgres.**

## Rationale

1. **Cost scales with what changed.** Bytes sent and Postgres work both scale with D, not S.
   Reconnects are the common case for local-first clients, so this is the cost that dominates in
   production.
2. **Less egress.** Sending D instead of S is a direct, recurring saving on bandwidth bills.
3. **It protects the primary.**
   - A reconnect where nothing changed costs Postgres nothing.
   - A reconnect with changes costs D primary-key lookups, the cheapest query Postgres runs.
   - Option 1 makes every reconnect a full read.
4. **No second database.**
   - Engine storage follows the write rate within the retention window, not the size of the
     database.
   - Postgres stays the only copy of the data, so nothing can drift: catch-ups return Postgres's
     actual rows.
5. **Recovery is cheap and fast.**
   - Nothing is copied at boot: a new epoch starts with an empty index and serves immediately.
   - Losing the slot or the engine's disk costs clients one full refetch, not hours of
     rebuilding.
6. **It fits the most workloads.**
   - Options 1 and 2 each win at one extreme: tiny tables, or a Postgres that must never be
     touched.
   - Real workloads mostly sit in between. There, option 3 keeps nearly all of option 2's
     reconnect savings at a small fraction of its storage and complexity.
7. **It fits what comes next.**
   - The index is per table, not per shape, so future WHERE-clause shapes share it.
   - Postgres applies the filter when the rows are read, so a row that no longer matches comes
     back absent, which is a delete.
   - Per-user authorization can live in the same read.
8. **Option 1 comes with it.** Full catch-ups are needed anyway for new clients and old offsets.
   Option 3 is option 1 plus the index, so option 1 stays available as the fallback and as the
   benchmark baseline.

## Consequences

What we accept with this choice, and where the plan handles each point (details in `PLAN.md`):

- **Postgres serves catch-ups.**
  - New clients and old offsets read whole tables, as in option 1. Returning clients read D rows.
  - A fixed pool of catch-up connections caps the load we put on Postgres; extra catch-ups wait
    (PLAN §4.2, §7.1).
- **A join on every catch-up that reads Postgres.** Each one is spliced with the live stream using
  snapshot visibility. This is the hardest correctness problem in the engine, so it has its own
  tests and is the core of the model test (PLAN §7.1, §11).
- **A commit can be streamed before it is visible.**
  - Postgres flushes a commit record before the transaction becomes visible to new snapshots.
    With synchronous replication the gap lasts at least a standby round trip.
  - A catch-up must never use a snapshot that misses a commit the engine has already streamed.
  - The session detects this and retries (PLAN §7.2). An integration test proves both the
    behavior and the fix.
- **No stored rows to fill in unchanged large values.**
  - On update, Postgres does not resend large out-of-line ("TOAST") values that didn't change.
    Option 2 filled them from its store.
  - We send such updates as **patches**: the client keeps its current value (PLAN §6.3).
  - We don't require `REPLICA IDENTITY FULL`, which would make Postgres write the whole old row
    to the WAL on every update and delete.
- **A small store is still needed.**
  - The index is durable, so engine restarts don't force full refetches. That keeps the single
    writer, group commit, acknowledge-after-durable, the permanent slot, and retention GC.
  - What shrinks is the payload: keys instead of rows.
- **Catch-ups need Postgres up.** During a Postgres outage, clients keep their local data and
  resume when it is back.
- **Large deltas fall back to full reads.** When D is a large share of a table, one full read is
  cheaper than many key lookups, so the engine switches per table (thresholds from benchmarks).
- **Primary-key text must be canonical.** Clients and the engine match rows by the text of their
  primary key. Key types where equal values can render differently, such as `numeric` (`1.0` vs
  `1.00`), are rejected at startup (PLAN §4.1).

## Also considered

- **A bounded log of recent transactions (ring buffer).**
  - Rejected earlier: it replays history instead of end state.
  - Its size follows write rate.
  - Clients offline longer than the ring covers get full refetches anyway.
- **Per-shape logs of row data (Electric's approach).** These are option 2 per shape: storage
  multiplies with the number of shapes.

## Validation, and when to revisit

We know of no mainstream sync engine that splits keys and rows this way, which is one more reason
to validate it early. Phase 1b (PLAN §10.3, S7) runs this benchmark matrix:

| Dimension | Values |
|---|---|
| Table size | 1k, 100k, 1M rows |
| Churn between reconnects | 0.1%, 10%, 100% |
| Clients reconnecting at once | 1, 1,000 |

For each combination it measures:

- bytes sent;
- Postgres CPU and query time (`pg_stat_statements`);
- engine CPU, memory and disk;
- time until the client is up to date;
- live latency for other clients while the reconnects run.

Options 1 and 3 are both measured on the real engine: option 1 is a forced full catch-up. Option 2
is estimated from the store spike.

Revisit this decision if:

- **Catch-up load on Postgres dominates a realistic workload**, for example many new clients on
  large tables. The first answer is caching full catch-ups (compressed snapshots, CDN), not a row
  store.
- **Key reads are slower than full reads** at the churn levels real apps have.
- **The join can't be made reliably correct** under the model test.
- **Serving catch-ups while Postgres is unavailable becomes a requirement.** Only option 2 can do
  that.
