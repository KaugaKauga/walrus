//! Catch-up reads from Postgres (PLAN.md §4.7, §7.1 step 5, §7.2): the catch-up pool, the
//! snapshot check, and COPY of full tables and delta keys.

use anyhow::{Context, Result, anyhow};
use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod, Transaction};
use futures_util::StreamExt;
use tokio_postgres::NoTls;

use crate::pg::copy_text::CopyParser;
use crate::pg::literal;
use crate::pg::setup::PgParams;
use crate::pg::xid::Snapshot;
use crate::schema::{Key, Row, TableSchema, quote_ident};

/// Delta keys per COPY statement (PLAN.md §4.7). Constant, tuned by benchmark.
pub const DELTA_BATCH: usize = 500;

/// The catch-up pool: `size` connections with `SESSION_OPTIONS`. Its size is the
/// admission-control limit (§3).
pub fn create_pool(params: &PgParams, size: usize) -> Result<Pool> {
    let manager = deadpool_postgres::Manager::from_config(
        params.tokio_config(),
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    Pool::builder(manager)
        .max_size(size)
        .build()
        .context("building the catch-up pool")
}

/// In a fresh `REPEATABLE READ READ ONLY` transaction, fix and return its snapshot. Must be the
/// transaction's first statement. Whether the snapshot is usable is the session's check against
/// the transactions the engine already applied (`RecentXids`, PLAN.md §7.2).
pub async fn take_snapshot(tx: &Transaction<'_>) -> Result<Snapshot> {
    let row = tx
        .query_one("SELECT pg_current_snapshot()::text", &[])
        .await
        .context("taking the catch-up snapshot")?;
    Snapshot::parse(row.get(0))
}

/// `COPY (SELECT <columns> FROM <table>) TO STDOUT`.
pub fn full_query(table: &TableSchema) -> String {
    format!(
        "COPY (SELECT {} FROM {}) TO STDOUT",
        table.quoted_columns(),
        table.quoted_name()
    )
}

/// COPY of the rows with the given keys (non-empty), using the primary-key index:
/// - single-column key: `... WHERE "id" = ANY(<literal>::<type>[])`;
/// - composite key: `... WHERE ("a", "b") IN (SELECT * FROM unnest(<lit a>::<ta>[], <lit b>::<tb>[]))`.
///
/// `<type>` is the column's `type_name`; literals come from `pg::literal::array_literal`.
pub fn delta_query(table: &TableSchema, keys: &[Key]) -> String {
    // Per key column: its quoted name and its `<literal>::<type>[]` array.
    let parts: Vec<(String, String)> = table
        .key
        .iter()
        .enumerate()
        .map(|(j, &col)| {
            let column = &table.columns[col];
            let lit = literal::array_literal(keys.iter().map(|k| k[j].as_str()));
            (
                quote_ident(&column.name),
                format!("{lit}::{}[]", column.type_name),
            )
        })
        .collect();
    let predicate = if let [(name, array)] = parts.as_slice() {
        format!("{name} = ANY({array})")
    } else {
        let names: Vec<&str> = parts.iter().map(|(n, _)| n.as_str()).collect();
        let arrays: Vec<&str> = parts.iter().map(|(_, a)| a.as_str()).collect();
        format!(
            "({}) IN (SELECT * FROM unnest({}))",
            names.join(", "),
            arrays.join(", ")
        )
    };
    format!(
        "COPY (SELECT {} FROM {} WHERE {predicate}) TO STDOUT",
        table.quoted_columns(),
        table.quoted_name()
    )
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
    let stream = tx.copy_out(sql).await.context("starting COPY")?;
    let mut stream = std::pin::pin!(stream);
    let mut parser = CopyParser::new(ncols);
    let mut total = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| anyhow!(e)).context("reading COPY data")?;
        let mut rows = Vec::new();
        parser.feed(&chunk, &mut rows)?;
        if !rows.is_empty() {
            total += rows.len() as u64;
            on_rows(rows).await?;
        }
    }
    parser.finish()?;
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnSchema;

    fn col(name: &str, ty: &str) -> ColumnSchema {
        ColumnSchema {
            name: name.into(),
            type_name: ty.into(),
            type_oid: 0,
        }
    }

    fn single() -> TableSchema {
        TableSchema {
            name: "public.todos".into(),
            columns: vec![col("id", "bigint"), col("title", "text")],
            key: vec![0],
        }
    }

    fn composite() -> TableSchema {
        TableSchema {
            name: "public.items".into(),
            columns: vec![
                col("list_id", "bigint"),
                col("pos", "integer"),
                col("v", "text"),
            ],
            key: vec![0, 1],
        }
    }

    #[test]
    fn full_query_text() {
        assert_eq!(
            full_query(&single()),
            r#"COPY (SELECT "id", "title" FROM "public"."todos") TO STDOUT"#
        );
    }

    #[test]
    fn delta_query_single_key() {
        let keys = vec![vec!["1".to_string()], vec!["it's".to_string()]];
        assert_eq!(
            delta_query(&single(), &keys),
            r#"COPY (SELECT "id", "title" FROM "public"."todos" WHERE "id" = ANY('{"1","it''s"}'::bigint[])) TO STDOUT"#
        );
    }

    #[test]
    fn delta_query_composite_key() {
        let keys = vec![
            vec!["1".to_string(), "2".to_string()],
            vec!["3".to_string(), "4".to_string()],
        ];
        assert_eq!(
            delta_query(&composite(), &keys),
            r#"COPY (SELECT "list_id", "pos", "v" FROM "public"."items" WHERE ("list_id", "pos") IN (SELECT * FROM unnest('{"1","3"}'::bigint[], '{"2","4"}'::integer[]))) TO STDOUT"#
        );
    }
}
