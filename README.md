# walrus

**WAL + Rust.** A sync engine that streams Postgres tables to clients over Server-Sent Events.
Clients keep a local copy that converges to the latest state, and a reconnecting client
receives only what changed while it was away.

- Reads Postgres's logical replication stream (`pgoutput`) directly. No triggers, no extra
  tables in your database.
- Keeps no copy of your data. The engine stores only *which keys changed when* (a change index)
  and reads the rows themselves from Postgres. See
  [ADR 0001](docs/decisions/0001-state-model.md) for why.
- Clients resume from an offset: nothing changed costs no Postgres query; a few changed rows cost
  a primary-key lookup each.

Status: MVP (read path only, whole tables, no auth). The plan is in [PLAN.md](PLAN.md).

## Quickstart

```sh
docker compose up -d     # Postgres 17 with wal_level=logical on localhost:54329

docker compose exec postgres psql -U postgres -c "
  CREATE TABLE todos (id bigint PRIMARY KEY, title text NOT NULL, done boolean NOT NULL DEFAULT false);
  INSERT INTO todos VALUES (1, 'milk', false), (2, 'eggs', true);"

WALRUS_DATABASE_URL=postgres://postgres:postgres@localhost:54329/postgres \
WALRUS_TABLES=public.todos \
cargo run --release -p walrus-engine
```

In another terminal:

```sh
curl -N "localhost:4000/v1/stream?tables=public.todos"
```

```
retry: 1000

event: table-reset
data: {"table":"public.todos","columns":[{"name":"id","type":"bigint","pk":true},{"name":"title","type":"text","pk":false},{"name":"done","type":"boolean","pk":false}]}

event: rows
data: {"table":"public.todos","ops":[["u",["1","milk","f"]],["u",["2","eggs","t"]]]}

event: commit
id: 2970ef25db6e780b-000000001f83afd0
data: {"offset":"2970ef25db6e780b-000000001f83afd0"}
```

Write to the table and the change arrives as `rows` + `commit`. To resume later, send the last
`commit` offset back: browsers do this automatically with `Last-Event-ID`; otherwise use
`-H "Last-Event-ID: <offset>"` or `&offset=<offset>`.

The protocol (events, row operations, offsets, client rules) is in [PLAN.md §6](PLAN.md).

## Configuration

Environment variables: `WALRUS_DATABASE_URL` and `WALRUS_TABLES` are required. The rest
(`WALRUS_DATA_DIR`, `WALRUS_LISTEN_ADDR`, `WALRUS_SLOT`, `WALRUS_PUBLICATION`,
`WALRUS_CATCHUP_CONNECTIONS`, `WALRUS_RETENTION`) have defaults; see [PLAN.md §8](PLAN.md).

Requirements:

- `wal_level=logical`;
- a role with `REPLICATION`;
- every synced table needs a primary key of a type whose equal values always render the same
  text: integer types, uuid, text, varchar, date, timestamps, bytea.

Endpoints: `/v1/stream`, `/health`, `/ready`, `/metrics` (Prometheus text).

## Development

```sh
docker compose up -d
./scripts/check.sh       # fmt, clippy -D warnings, all tests (unit + integration against Postgres)
```

Layout:

| Crate | Contents |
|---|---|
| `crates/walrus-engine` | The engine (`walrus` binary) |
| `crates/walrus-protocol` | Wire types shared with clients |
| `crates/walrus-client` | Minimal native client, used by the tests |
