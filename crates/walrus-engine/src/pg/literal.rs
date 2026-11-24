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
    let _ = values.into_iter();
    todo!()
}
