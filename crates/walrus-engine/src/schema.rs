//! Table schemas and row values shared by the decoder, the change index, encoding and catch-ups.

use serde::{Deserialize, Serialize};

pub use walrus_protocol::{Key, Row, Value};

/// Index of a table in the engine's `Vec<TableSchema>` (configured order). Stable within an
/// epoch; also the table's id in the change index.
pub type TableId = usize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    /// `format_type(atttypid, atttypmod)`, e.g. `numeric(5,2)`.
    pub type_name: String,
    pub type_oid: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    /// Schema-qualified name as configured, e.g. `public.todos`.
    pub name: String,
    /// Published columns in `attnum` order: not dropped, not generated.
    pub columns: Vec<ColumnSchema>,
    /// Indexes into `columns` of the primary-key columns, ascending (column order, not the
    /// constraint's order): the protocol identifies key columns only by `pk: true`, so keys are
    /// always the key columns' values in column order, everywhere.
    pub key: Vec<usize>,
}

impl TableSchema {
    /// The primary key of `row`. `None` if a key column is NULL (an invariant violation upstream:
    /// key columns are never NULL and never "unchanged" in pgoutput).
    pub fn key_of(&self, row: &Row) -> Option<Key> {
        self.key
            .iter()
            .map(|&i| row.get(i).cloned().flatten())
            .collect()
    }

    /// `"schema"."table"`, quoted for SQL.
    pub fn quoted_name(&self) -> String {
        let (schema, table) = self
            .name
            .split_once('.')
            .expect("configured table names are schema-qualified (validated in config)");
        format!("{}.{}", quote_ident(schema), quote_ident(table))
    }

    /// Column list in column order, quoted for SQL: `"a", "b", ...`.
    pub fn quoted_columns(&self) -> String {
        self.columns
            .iter()
            .map(|c| quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn column_info(&self) -> Vec<walrus_protocol::ColumnInfo> {
        self.columns
            .iter()
            .enumerate()
            .map(|(i, c)| walrus_protocol::ColumnInfo {
                name: c.name.clone(),
                type_name: c.type_name.clone(),
                pk: self.key.contains(&i),
            })
            .collect()
    }
}

pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_and_quoting() {
        let t = TableSchema {
            name: "public.to\"dos".into(),
            columns: vec![
                ColumnSchema {
                    name: "a".into(),
                    type_name: "bigint".into(),
                    type_oid: 20,
                },
                ColumnSchema {
                    name: "b".into(),
                    type_name: "text".into(),
                    type_oid: 25,
                },
            ],
            key: vec![1, 0],
        };
        let row = vec![Some("1".into()), Some("x".into())];
        assert_eq!(t.key_of(&row), Some(vec!["x".to_string(), "1".to_string()]));
        assert_eq!(t.key_of(&vec![Some("1".into()), None]), None);
        assert_eq!(t.quoted_name(), "\"public\".\"to\"\"dos\"");
        assert_eq!(t.quoted_columns(), "\"a\", \"b\"");
    }
}
