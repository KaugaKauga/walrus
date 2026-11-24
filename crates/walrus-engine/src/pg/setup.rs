//! Connections, startup checks, catalog, publication and new-epoch setup (PLAN.md §4.1–4.5).

use anyhow::Result;
use pgwire_replication::ReplicationConfig;
use tokio_postgres::Client;

use crate::change_index::{IndexReader, IndexWriter, Meta};
use crate::config::Config;
use crate::schema::TableSchema;

/// Pinned session settings for every connection (I9): values arrive as text and must render
/// identically in catch-ups and in the stream. `standard_conforming_strings` because we build
/// key literals (pg/literal.rs).
pub const SESSION_OPTIONS: &str = "-c bytea_output=hex -c DateStyle=ISO,DMY -c TimeZone=UTC \
     -c extra_float_digits=1 -c IntervalStyle=iso_8601 -c standard_conforming_strings=on";

/// Primary-key column types whose equal values always render the same text (PLAN.md §4.1).
/// `text`/`varchar` additionally need a deterministic collation.
pub const KEY_TYPES: &[&str] = &[
    "smallint",
    "integer",
    "bigint",
    "uuid",
    "text",
    "character varying",
    "date",
    "timestamp without time zone",
    "timestamp with time zone",
    "bytea",
];

/// Connection parameters parsed once from `WALRUS_DATABASE_URL`.
#[derive(Clone, Debug)]
pub struct PgParams {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
}

impl PgParams {
    /// Parse a `postgres://user:password@host:port/dbname` URL (via `tokio_postgres::Config`).
    /// Only TCP hosts; defaults: port 5432, dbname = user.
    pub fn from_url(url: &str) -> Result<PgParams> {
        let _ = url;
        todo!()
    }

    /// `tokio_postgres` config with `SESSION_OPTIONS` and `application_name = walrus`.
    pub fn tokio_config(&self) -> tokio_postgres::Config {
        todo!()
    }

    /// Replication config for `slot`/`publication` with `SESSION_OPTIONS`, TLS disabled
    /// (PLAN.md §1: TLS is out of scope), start LSN 0/0 (resume from the slot).
    pub fn replication_config(&self, slot: &str, publication: &str) -> ReplicationConfig {
        let _ = (slot, publication);
        todo!()
    }
}

/// Connect a plain SQL client (spawns its connection task).
pub async fn connect(params: &PgParams) -> Result<Client> {
    let _ = params;
    todo!()
}

/// §4.1 server checks: `wal_level = logical`; free `max_replication_slots`, `max_wal_senders`
/// and `max_connections` headroom for `catchup_connections`; role has REPLICATION or is
/// superuser. Errors are actionable ("set wal_level=logical and restart Postgres").
pub async fn check_server(client: &Client, catchup_connections: usize) -> Result<()> {
    let _ = (client, catchup_connections);
    todo!()
}

/// Read and check the configured tables' schemas from the catalog (§4.1, §4.7):
/// - the table exists, has a primary key, and its replica identity is not NOTHING;
/// - every key column's type (by `format_type` without typmod) is in `KEY_TYPES`, and text
///   keys use a deterministic collation;
/// - columns: `attnum > 0`, not dropped, not generated, in `attnum` order, with
///   `format_type(atttypid, atttypmod)` and the type oid; `key` = indexes of the key columns,
///   ascending (column order, see `TableSchema::key`).
pub async fn read_schemas(client: &Client, names: &[String]) -> Result<Vec<TableSchema>> {
    let _ = (client, names);
    todo!()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotState {
    Missing,
    /// `pg_replication_slots.wal_status = 'lost'`.
    Lost,
    Ready,
}

pub async fn slot_state(client: &Client, slot: &str) -> Result<SlotState> {
    let _ = (client, slot);
    todo!()
}

/// Start a new epoch (PLAN.md §4.5, I10):
/// 1. drop the slot if it exists; delete the store (`change_index::remove`);
/// 2. create the publication `FOR TABLE <tables>`, or `ALTER PUBLICATION ... SET TABLE` it;
/// 3. create the slot on a gated replication session:
///    `CREATE_REPLICATION_SLOT <slot> LOGICAL pgoutput (SNAPSHOT 'nothing')` → consistent
///    point C (log what blocks it from `pg_stat_activity` every few seconds while waiting);
/// 4. `change_index::create(data_dir, random epoch, tables, C)`.
pub async fn new_epoch(
    params: &PgParams,
    client: &Client,
    config: &Config,
    tables: &[TableSchema],
) -> Result<(IndexWriter, IndexReader, Meta)> {
    let _ = (params, client, config, tables);
    todo!()
}
