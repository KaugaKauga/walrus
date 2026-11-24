//! Catch-up reads from Postgres (PLAN.md §4.7, §7.1 step 5, §7.2): the catch-up pool, the
//! snapshot check, and COPY of full tables and delta keys.

use anyhow::Result;
use deadpool_postgres::{Pool, Transaction};

use crate::pg::setup::PgParams;
use crate::pg::xid::Snapshot;
use crate::schema::{Key, Row, TableSchema};

/// Delta keys per COPY statement (PLAN.md §4.7). Constant, tuned by benchmark.
pub const DELTA_BATCH: usize = 500;

/// The catch-up pool: `size` connections with `SESSION_OPTIONS`. Its size is the
/// admission-control limit (§3).
pub fn create_pool(params: &PgParams, size: usize) -> Result<Pool> {
    let _ = (params, size);
    todo!()
}

/// In a fresh `REPEATABLE READ READ ONLY` transaction, take its snapshot and run the §7.2 check
/// in the transaction's first statement:
///
/// ```sql
/// SELECT s::text, ARRAY(SELECT x::text FROM pg_snapshot_xip(s) AS x
///                       WHERE pg_xact_status(x) = 'committed')
///   FROM pg_current_snapshot() AS s
/// ```
///
/// Returns `None` when the check finds an in-progress xid that is already committed: the
/// caller must roll back and retry with a new transaction.
pub async fn take_snapshot(tx: &Transaction<'_>) -> Result<Option<Snapshot>> {
    let _ = tx;
    todo!()
}

/// `COPY (SELECT <columns> FROM <table>) TO STDOUT`.
pub fn full_query(table: &TableSchema) -> String {
    let _ = table;
    todo!()
}

/// COPY of the rows with the given keys (non-empty), using the primary-key index:
/// - single-column key: `... WHERE "id" = ANY(<literal>::<type>[])`;
/// - composite key: `... WHERE ("a", "b") IN (SELECT * FROM unnest(<lit a>::<ta>[], <lit b>::<tb>[]))`.
///
/// `<type>` is the column's `type_name`; literals come from `pg::literal::array_literal`.
pub fn delta_query(table: &TableSchema, keys: &[Key]) -> String {
    let _ = (table, keys);
    todo!()
}

/// Run a `COPY ... TO STDOUT` statement and parse it with `CopyParser`, calling `on_rows` with
/// each batch of rows as chunks arrive (so memory stays bounded and the client's pace drives the
/// read). Returns the number of rows read.
pub async fn copy_rows<F, Fut>(
    tx: &Transaction<'_>,
    sql: &str,
    ncols: usize,
    mut on_rows: F,
) -> Result<u64>
where
    F: FnMut(Vec<Row>) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let _ = (tx, sql, ncols, &mut on_rows);
    todo!()
}
