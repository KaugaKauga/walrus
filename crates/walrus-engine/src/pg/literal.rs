//! Key literals for delta reads (PLAN.md §4.7). Pure.
//!
//! COPY takes no bind parameters, so delta keys are inlined. `standard_conforming_strings` is
//! pinned on for every connection (pg/setup.rs), so inside a SQL string literal only `'` needs
//! escaping (doubled).

/// A SQL string literal holding a Postgres array literal of `values`, every element
/// double-quoted, e.g. `["a", "b\"c", "it's"]` → `'{"a","b\"c","it''s"}'`.
///
/// Element escaping (array syntax): `\` → `\\`, `"` → `\"`. Then the whole array text is wrapped
/// in `'` with `'` doubled. Safe for arbitrary text, including the empty string, braces, commas,
/// whitespace, `NULL`-looking strings and unicode. Postgres text never contains NUL.
pub fn array_literal<'a>(values: impl IntoIterator<Item = &'a str>) -> String {
    let mut out = String::from("'{");
    for (i, value) in values.into_iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        // Quoting every element makes braces, commas, whitespace and NULL plain text, so only
        // the two array-syntax specials need a backslash. `'` is the SQL layer's concern.
        out.push('"');
        for c in value.chars() {
            match c {
                '\\' => out.push_str("\\\\"),
                '"' => out.push_str("\\\""),
                '\'' => out.push_str("''"),
                c => out.push(c),
            }
        }
        out.push('"');
    }
    out.push_str("}'");
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn lit(values: &[&str]) -> String {
        array_literal(values.iter().copied())
    }

    #[test]
    fn documented_example() {
        assert_eq!(lit(&["a", "b\"c", "it's"]), r#"'{"a","b\"c","it''s"}'"#);
    }

    #[test]
    fn empty_array_and_empty_string() {
        assert_eq!(lit(&[]), "'{}'");
        assert_eq!(lit(&[""]), r#"'{""}'"#);
        assert_eq!(lit(&["", ""]), r#"'{"",""}'"#);
    }

    #[test]
    fn specials() {
        assert_eq!(lit(&["a\\b"]), r#"'{"a\\b"}'"#);
        assert_eq!(lit(&["\\"]), r#"'{"\\"}'"#);
        assert_eq!(lit(&["\"\""]), r#"'{"\"\""}'"#);
        assert_eq!(lit(&["{a,b}"]), r#"'{"{a,b}"}'"#);
        assert_eq!(lit(&["a,b", "c"]), r#"'{"a,b","c"}'"#);
        assert_eq!(lit(&["  padded \t\n"]), "'{\"  padded \t\n\"}'");
        assert_eq!(lit(&["NULL", "null"]), r#"'{"NULL","null"}'"#);
        assert_eq!(lit(&["héllo 🦭"]), r#"'{"héllo 🦭"}'"#);
        assert_eq!(lit(&["'"]), r#"'{"''"}'"#);
        assert_eq!(lit(&["''"]), r#"'{"''''"}'"#);
        // Backslash before a quote: both escapes apply independently.
        assert_eq!(lit(&["\\'"]), r#"'{"\\''"}'"#);
        assert_eq!(lit(&["\\\""]), r#"'{"\\\""}'"#);
    }

    #[test]
    fn owned_strings_work() {
        let keys = ["1".to_string(), "2".to_string()];
        assert_eq!(
            array_literal(keys.iter().map(String::as_str)),
            r#"'{"1","2"}'"#
        );
    }

    /// Round trip through Postgres's own parser. Needs the dev database (localhost:54329).
    #[tokio::test]
    async fn postgres_roundtrip() {
        use rand::Rng;

        const NASTY: &[char] = &[
            '\\', '"', '\'', '{', '}', ',', ' ', '\t', '\n', '\r', 'N', 'U', 'L', 'é', '🦭',
            '\u{1}', 'a', 'b', ';', '-', '$', '%', '_', '\u{7f}',
        ];

        let (client, conn) = tokio_postgres::connect(
            "host=localhost port=54329 user=postgres password=postgres dbname=postgres \
             options='-c standard_conforming_strings=on'",
            tokio_postgres::NoTls,
        )
        .await
        .expect("test Postgres at localhost:54329 must be running");
        let conn = tokio::spawn(conn);

        let mut rng = rand::rng();
        let mut values: Vec<String> = ["", "NULL", "null", "\\", "'", "\"", "{}", "a,b", " x "]
            .iter()
            .map(|s| s.to_string())
            .collect();
        for _ in 0..300 {
            let len = rng.random_range(0..12);
            values.push(
                (0..len)
                    .map(|_| match rng.random_range(0..4) {
                        0 => rng.random_range('a'..='z'),
                        // Arbitrary non-NUL scalar values.
                        1 => rng.random_range('\u{1}'..=char::MAX),
                        _ => NASTY[rng.random_range(0..NASTY.len())],
                    })
                    .collect(),
            );
        }

        let sql = format!(
            "SELECT unnest({}::text[])",
            array_literal(values.iter().map(String::as_str))
        );
        let got: Vec<String> = client
            .simple_query(&sql)
            .await
            .expect("query")
            .into_iter()
            .filter_map(|m| match m {
                tokio_postgres::SimpleQueryMessage::Row(r) => {
                    Some(r.get(0).expect("no NULLs expected").to_string())
                }
                _ => None,
            })
            .collect();
        assert_eq!(got, values);

        // The empty array too.
        let rows = client
            .simple_query("SELECT unnest('{}'::text[])")
            .await
            .expect("query");
        assert!(
            rows.iter()
                .all(|m| !matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        );

        drop(client);
        conn.await.expect("join").expect("connection");
    }
}
