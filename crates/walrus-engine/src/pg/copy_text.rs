//! Incremental parser for `COPY ... TO STDOUT` text format (PLAN.md §4.7). Pure.
//!
//! Rows end with `\n`, columns are separated by `\t`, `\N` is NULL. Backslash escapes:
//! `\\` `\b` `\f` `\n` `\r` `\t` `\v`, octal `\NNN` (1–3 digits) and hex `\xHH` (1–2 digits);
//! any other escaped character stands for itself. Rows may be split across chunks at any byte.
//! Values are UTF-8 (the server encoding we require).

use anyhow::Result;

use crate::schema::Row;

pub struct CopyParser {
    ncols: usize,
    // Implementation-defined: bytes of an incomplete trailing row, etc.
}

impl CopyParser {
    pub fn new(ncols: usize) -> Self {
        let _ = ncols;
        todo!()
    }

    /// Feed the next chunk of COPY data; appends every row completed by it to `out`.
    /// Errors on a row whose column count differs from `ncols` or on invalid UTF-8.
    pub fn feed(&mut self, chunk: &[u8], out: &mut Vec<Row>) -> Result<()> {
        let _ = (self.ncols, chunk, out);
        todo!()
    }

    /// End of data: errors if an incomplete row remains.
    pub fn finish(self) -> Result<()> {
        todo!()
    }
}
