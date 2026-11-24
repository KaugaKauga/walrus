//! Connections, startup checks, catalog, publication and new-epoch setup (PLAN.md §4.1–4.5).

use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, TlsConfig};
use tokio_postgres::config::Host;
use tokio_postgres::{Client, NoTls};

use crate::change_index::{IndexReader, IndexWriter, Meta};
use crate::config::Config;
use crate::schema::{ColumnSchema, TableSchema, quote_ident};

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

/// How often slot creation reports what it is waiting for.
const BLOCKER_LOG_INTERVAL: Duration = Duration::from_secs(5);

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
        // The URL contains the password: never put the URL itself in an error message.
        let cfg: tokio_postgres::Config = url
            .parse()
            .map_err(|e| anyhow!("WALRUS_DATABASE_URL is not a valid Postgres URL: {e}"))?;
        // The replication client speaks TCP only, so reject socket paths up front.
        let Some(Host::Tcp(host)) = cfg.get_hosts().first() else {
            bail!(
                "WALRUS_DATABASE_URL needs a TCP host, like postgres://user:pass@localhost:5432/db"
            );
        };
        let user = cfg
            .get_user()
            .context("WALRUS_DATABASE_URL needs a user")?
            .to_string();
        let password = match cfg.get_password() {
            Some(p) => {
                String::from_utf8(p.to_vec()).context("the database password is not UTF-8")?
            }
            None => String::new(),
        };
        Ok(PgParams {
            host: host.clone(),
            port: cfg.get_ports().first().copied().unwrap_or(5432),
            dbname: cfg.get_dbname().unwrap_or(&user).to_string(),
            user,
            password,
        })
    }

    /// `tokio_postgres` config with `SESSION_OPTIONS` and `application_name = walrus`.
    pub fn tokio_config(&self) -> tokio_postgres::Config {
        let mut cfg = tokio_postgres::Config::new();
        cfg.host(&self.host)
            .port(self.port)
            .user(&self.user)
            .password(&self.password)
            .dbname(&self.dbname)
            .options(SESSION_OPTIONS)
            .application_name("walrus");
        cfg
    }

    /// Replication config for `slot`/`publication` with `SESSION_OPTIONS`, TLS disabled
    /// (PLAN.md §1: TLS is out of scope), start LSN 0/0 (resume from the slot).
    pub fn replication_config(&self, slot: &str, publication: &str) -> ReplicationConfig {
        ReplicationConfig::new(
            self.host.clone(),
            self.user.clone(),
            self.password.clone(),
            self.dbname.clone(),
            slot,
            publication,
        )
        .with_port(self.port)
        .with_options(SESSION_OPTIONS)
        .with_tls(TlsConfig::disabled())
    }
}

/// Connect a plain SQL client (spawns its connection task).
pub async fn connect(params: &PgParams) -> Result<Client> {
    let (client, connection) = params
        .tokio_config()
        .connect(NoTls)
        .await
        .with_context(|| {
            format!(
                "cannot connect to Postgres at {}:{} (database {:?}, user {:?})",
                params.host, params.port, params.dbname, params.user
            )
        })?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::warn!("setup connection closed: {e}");
        }
    });
    Ok(client)
}

/// §4.1 server checks: `wal_level = logical`; free `max_replication_slots`, `max_wal_senders`
/// and `max_connections` headroom for `catchup_connections`; role has REPLICATION or is
/// superuser. Errors are actionable ("set wal_level=logical and restart Postgres").
pub async fn check_server(client: &Client, catchup_connections: usize) -> Result<()> {
    let row = client
        .query_one(
            "SELECT current_setting('wal_level'),
                    current_setting('max_replication_slots')::bigint,
                    current_setting('max_wal_senders')::bigint,
                    current_setting('max_connections')::bigint,
                    current_setting('superuser_reserved_connections')::bigint,
                    (SELECT count(*) FROM pg_stat_replication),
                    (SELECT count(*) FROM pg_stat_activity),
                    (SELECT rolsuper OR rolreplication FROM pg_roles WHERE rolname = current_user),
                    current_user::text",
            &[],
        )
        .await
        .context("reading server settings")?;
    let wal_level: String = row.get(0);
    let (max_slots, max_senders, max_conns, reserved): (i64, i64, i64, i64) =
        (row.get(1), row.get(2), row.get(3), row.get(4));
    let (senders_used, conns_used): (i64, i64) = (row.get(5), row.get(6));
    let can_replicate: bool = row.get(7);
    let user: String = row.get(8);

    if wal_level != "logical" {
        bail!(
            "wal_level is {wal_level:?}; set wal_level=logical in postgresql.conf and restart Postgres"
        );
    }
    // Whether a slot is free depends on whether ours already exists. Postgres reports that
    // precisely when we create or start the slot, so here we only catch the hopeless setting.
    if max_slots < 1 {
        bail!("max_replication_slots is 0; set it to at least 1 and restart Postgres");
    }
    if max_senders - senders_used < 1 {
        bail!(
            "no free WAL sender ({senders_used} of max_wal_senders={max_senders} in use); \
             raise max_wal_senders and restart Postgres"
        );
    }
    // One replication connection plus the catch-up pool. This setup connection is already
    // counted in `conns_used` and is closed before the pool is used.
    let needed = catchup_connections as i64 + 1;
    let free = max_conns - reserved - conns_used;
    if free < needed {
        bail!(
            "not enough free connections: walrus needs {needed} (1 replication + \
             {catchup_connections} catch-up) but only {free} remain (max_connections={max_conns}, \
             {conns_used} in use, {reserved} reserved for superusers); raise max_connections \
             or lower WALRUS_CATCHUP_CONNECTIONS"
        );
    }
    if !can_replicate {
        bail!("role {user:?} lacks the REPLICATION attribute; run ALTER ROLE {user} REPLICATION");
    }
    Ok(())
}

/// Read and check the configured tables' schemas from the catalog (§4.1, §4.7):
/// - the table exists, has a primary key, and its replica identity is not NOTHING;
/// - every key column's type (by `format_type` without typmod) is in `KEY_TYPES`, and text
///   keys use a deterministic collation;
/// - columns: `attnum > 0`, not dropped, not generated, in `attnum` order, with
///   `format_type(atttypid, atttypmod)` and the type oid; `key` = indexes of the key columns,
///   ascending (column order, see `TableSchema::key`).
pub async fn read_schemas(client: &Client, names: &[String]) -> Result<Vec<TableSchema>> {
    let mut tables = Vec::with_capacity(names.len());
    for name in names {
        let table = read_schema(client, name)
            .await
            .with_context(|| format!("table {name}"))?;
        tables.push(table);
    }
    Ok(tables)
}

async fn read_schema(client: &Client, name: &str) -> Result<TableSchema> {
    let (schema, table) = name
        .split_once('.')
        .with_context(|| format!("{name:?} must be schema-qualified, like public.todos"))?;

    let class = client
        .query_opt(
            "SELECT c.oid, c.relkind::text, c.relreplident::text,
                    has_table_privilege(c.oid, 'SELECT')
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = $2",
            &[&schema, &table],
        )
        .await
        .context("reading pg_class")?;
    let Some(class) = class else {
        bail!("does not exist; create it, or fix WALRUS_TABLES (names are schema-qualified)");
    };
    let oid: u32 = class.get(0);
    let relkind: String = class.get(1);
    let replident: String = class.get(2);
    let can_select: bool = class.get(3);
    if relkind != "r" {
        bail!(
            "is not an ordinary table (relkind {relkind:?}); views, partitioned tables and \
             other relations are not supported"
        );
    }
    if replident == "n" {
        bail!(
            "has REPLICA IDENTITY NOTHING, which would make UPDATE and DELETE fail once it is \
             published; run ALTER TABLE {}.{} REPLICA IDENTITY DEFAULT",
            quote_ident(schema),
            quote_ident(table)
        );
    }
    if !can_select {
        bail!("the connected role cannot SELECT from it; GRANT SELECT on it to the role");
    }

    // `pg_index.indkey` lists key columns in constraint order, but we only need membership:
    // keys are always in column order (schema.rs). `collisdeterministic` is NULL (no join
    // match) for columns whose type is not collatable.
    let rows = client
        .query(
            "SELECT a.attname::text,
                    format_type(a.atttypid, a.atttypmod),
                    a.atttypid,
                    format_type(a.atttypid, NULL),
                    EXISTS (SELECT 1 FROM pg_index i
                            WHERE i.indrelid = a.attrelid AND i.indisprimary
                              AND a.attnum = ANY (i.indkey::int2[])),
                    coll.collisdeterministic
             FROM pg_attribute a
             LEFT JOIN pg_collation coll ON coll.oid = a.attcollation
             WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
               AND a.attgenerated = ''
             ORDER BY a.attnum",
            &[&oid],
        )
        .await
        .context("reading pg_attribute")?;

    let mut columns = Vec::with_capacity(rows.len());
    let mut key = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let col_name: String = row.get(0);
        columns.push(ColumnSchema {
            name: col_name.clone(),
            type_name: row.get(1),
            type_oid: row.get(2),
        });
        let is_key: bool = row.get(4);
        if !is_key {
            continue;
        }
        let bare_type: String = row.get(3);
        if !KEY_TYPES.contains(&bare_type.as_str()) {
            bail!(
                "primary-key column {col_name:?} has type {bare_type}, which is not supported: \
                 equal values of that type can render as different text, and clients identify \
                 rows by the text of their key. Supported key types: {}",
                KEY_TYPES.join(", ")
            );
        }
        let deterministic: Option<bool> = row.get(5);
        if deterministic == Some(false) {
            bail!(
                "primary-key column {col_name:?} uses a nondeterministic collation, so equal \
                 keys can have different text; use a deterministic collation"
            );
        }
        key.push(i);
    }
    if key.is_empty() {
        bail!("has no primary key; walrus identifies rows by primary key");
    }
    Ok(TableSchema {
        name: name.to_string(),
        columns,
        key,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotState {
    Missing,
    /// `pg_replication_slots.wal_status = 'lost'`.
    Lost,
    Ready,
}

pub async fn slot_state(client: &Client, slot: &str) -> Result<SlotState> {
    let row = client
        .query_opt(
            "SELECT wal_status FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .context("reading pg_replication_slots")?;
    Ok(match row {
        None => SlotState::Missing,
        // NULL wal_status: the slot has no restart_lsn, i.e. it is invalidated as well.
        Some(row) => match row.get::<_, Option<String>>(0).as_deref() {
            Some("lost") | None => SlotState::Lost,
            Some(_) => SlotState::Ready,
        },
    })
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
    // Remove the store first: until step 4 there is no initialized store, so a crash anywhere
    // in between just starts another new epoch (I10).
    crate::change_index::remove(&config.data_dir).context("deleting the old change index")?;
    let consistent_point =
        create_publication_and_slot(params, client, &config.slot, &config.publication, tables)
            .await?;

    let mut buf = [0u8; 8];
    getrandom::fill(&mut buf).map_err(|e| anyhow!("no randomness for the epoch: {e}"))?;
    let epoch = u64::from_le_bytes(buf);

    // I10: the store is created, with `initialized` set in the same transaction as the epoch
    // and floors, only now that the slot exists.
    let store = crate::change_index::create(&config.data_dir, epoch, tables, consistent_point)
        .context("creating the change index")?;
    tracing::info!(
        epoch = format!("{epoch:016x}"),
        consistent_point = %Lsn(consistent_point),
        "new epoch"
    );
    Ok(store)
}

/// The Postgres half of `new_epoch` (steps 1–3, without touching the store): drop the slot if
/// it exists, create or update the publication, create the slot. Returns the consistent point
/// C as a raw LSN.
pub async fn create_publication_and_slot(
    params: &PgParams,
    client: &Client,
    slot: &str,
    publication: &str,
    tables: &[TableSchema],
) -> Result<u64> {
    // Only if it exists: pg_drop_replication_slot errors on a missing slot, and also on one
    // that another process is streaming from (walrus already running?).
    client
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
             WHERE slot_name = $1",
            &[&slot],
        )
        .await
        .with_context(|| {
            format!("dropping replication slot {slot:?} (is another walrus streaming from it?)")
        })?;

    let table_list = tables
        .iter()
        .map(TableSchema::quoted_name)
        .collect::<Vec<_>>()
        .join(", ");
    let exists = client
        .query_opt(
            "SELECT 1 FROM pg_publication WHERE pubname = $1",
            &[&publication],
        )
        .await
        .context("reading pg_publication")?
        .is_some();
    let statement = if exists {
        format!(
            "ALTER PUBLICATION {} SET TABLE {table_list}",
            quote_ident(publication)
        )
    } else {
        format!(
            "CREATE PUBLICATION {} FOR TABLE {table_list}",
            quote_ident(publication)
        )
    };
    client
        .batch_execute(&statement)
        .await
        .with_context(|| format!("publication {publication:?}: {statement}"))?;

    let mut repl = ReplicationClient::connect_gated(params.replication_config(slot, publication))
        .await
        .context("opening the replication connection")?;
    let create_sql = format!(
        "CREATE_REPLICATION_SLOT {} LOGICAL pgoutput (SNAPSHOT 'nothing')",
        quote_ident(slot)
    );
    // Slot creation waits for every transaction running at this moment (PLAN.md §4.5 step 3),
    // so say what it is waiting for instead of hanging silently.
    let rows = {
        let create = repl.simple_query(&create_sql);
        tokio::pin!(create);
        let mut blockers = tokio::time::interval_at(
            tokio::time::Instant::now() + BLOCKER_LOG_INTERVAL,
            BLOCKER_LOG_INTERVAL,
        );
        loop {
            tokio::select! {
                r = &mut create => {
                    break r.with_context(|| format!("creating replication slot {slot:?}"))?;
                }
                _ = blockers.tick() => log_blockers(client).await,
            }
        }
    };
    // The slot is released when CREATE_REPLICATION_SLOT finishes, so dropping this
    // never-started session leaves it free for the streaming connection.
    drop(repl);

    // Columns: slot_name, consistent_point, snapshot_name, output_plugin.
    let point = rows
        .first()
        .and_then(|r| r.get(1))
        .and_then(|v| v.as_ref())
        .context("CREATE_REPLICATION_SLOT returned no consistent point")?;
    let point = std::str::from_utf8(point).context("consistent point is not UTF-8")?;
    let lsn = Lsn::parse(point).map_err(|e| anyhow!("bad consistent point {point:?}: {e}"))?;
    Ok(lsn.as_u64())
}

/// Log the transactions that can hold up slot creation. Best effort: a failure here must not
/// fail setup.
async fn log_blockers(client: &Client) {
    let rows = client
        .query(
            "SELECT pid, state, usename::text, xact_start::text, left(query, 120)
             FROM pg_stat_activity
             WHERE xact_start IS NOT NULL AND pid <> pg_backend_pid()
               AND backend_type = 'client backend' AND datname = current_database()
             ORDER BY xact_start LIMIT 10",
            &[],
        )
        .await;
    match rows {
        Ok(rows) if rows.is_empty() => {
            tracing::warn!("still waiting to create the replication slot");
        }
        Ok(rows) => {
            tracing::warn!(
                "still waiting to create the replication slot: it waits for these open \
                 transactions to finish"
            );
            for row in rows {
                let pid: i32 = row.get(0);
                let state: Option<String> = row.get(1);
                let user: Option<String> = row.get(2);
                let since: Option<String> = row.get(3);
                let query: Option<String> = row.get(4);
                tracing::warn!(pid, ?state, ?user, ?since, ?query, "open transaction");
            }
        }
        Err(e) => tracing::warn!("still waiting to create the replication slot ({e})"),
    }
}
