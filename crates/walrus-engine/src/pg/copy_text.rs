//! Incremental parser for `COPY ... TO STDOUT` text format (PLAN.md §4.7). Pure.
//!
//! Rows end with `\n`, columns are separated by `\t`, `\N` is NULL. Backslash escapes:
//! `\\` `\b` `\f` `\n` `\r` `\t` `\v`, octal `\NNN` (1–3 digits) and hex `\xHH` (1–2 digits);
//! any other escaped character stands for itself. Rows may be split across chunks at any byte.
//! Values are UTF-8 (the server encoding we require).

use anyhow::{Context, Result, bail};

use crate::schema::Row;

pub struct CopyParser {
    ncols: usize,
    /// Bytes of an incomplete trailing row (no `\n` seen yet). Always empty between rows.
    pending: Vec<u8>,
    /// Rows completed so far, for error messages.
    rows_seen: u64,
}

impl CopyParser {
    pub fn new(ncols: usize) -> Self {
        CopyParser {
            ncols,
            pending: Vec::new(),
            rows_seen: 0,
        }
    }

    /// Feed the next chunk of COPY data; appends every row completed by it to `out`.
    /// Errors on a row whose column count differs from `ncols` or on invalid UTF-8.
    pub fn feed(&mut self, chunk: &[u8], out: &mut Vec<Row>) -> Result<()> {
        let mut rest = chunk;

        // Finish a row left over from the previous chunk. Only that row's bytes are copied;
        // everything else is parsed straight out of `chunk`.
        if !self.pending.is_empty() {
            let Some(nl) = rest.iter().position(|&b| b == b'\n') else {
                self.pending.extend_from_slice(rest);
                return Ok(());
            };
            self.pending.extend_from_slice(&rest[..nl]);
            rest = &rest[nl + 1..];
            // Take the buffer so `parse_line` can borrow `self`; put it back to reuse capacity.
            let line = std::mem::take(&mut self.pending);
            let row = self.parse_line(&line);
            self.pending = line;
            self.pending.clear();
            out.push(row?);
        }

        // A raw `\n` is always a row end: newlines inside values arrive as `\n` escapes.
        while let Some(nl) = rest.iter().position(|&b| b == b'\n') {
            out.push(self.parse_line(&rest[..nl])?);
            rest = &rest[nl + 1..];
        }
        self.pending.extend_from_slice(rest);
        Ok(())
    }

    /// End of data: errors if an incomplete row remains.
    pub fn finish(self) -> Result<()> {
        if !self.pending.is_empty() {
            bail!(
                "COPY data ended inside row {} ({} bytes without a terminating newline)",
                self.rows_seen + 1,
                self.pending.len()
            );
        }
        Ok(())
    }

    fn parse_line(&mut self, line: &[u8]) -> Result<Row> {
        self.rows_seen += 1;
        let row =
            parse_row(line, self.ncols).with_context(|| format!("COPY row {}", self.rows_seen))?;
        Ok(row)
    }
}

fn parse_row(line: &[u8], ncols: usize) -> Result<Row> {
    let mut row = Vec::with_capacity(ncols);
    for field in line.split(|&b| b == b'\t') {
        // Bail before allocating more values for a row that is already too wide.
        if row.len() == ncols {
            bail!("more than {ncols} columns");
        }
        row.push(parse_field(field)?);
    }
    if row.len() != ncols {
        bail!("expected {ncols} columns, got {}", row.len());
    }
    Ok(row)
}

fn parse_field(field: &[u8]) -> Result<Option<String>> {
    if field == b"\\N" {
        return Ok(None);
    }
    if !field.contains(&b'\\') {
        let s = std::str::from_utf8(field).context("invalid UTF-8")?;
        return Ok(Some(s.to_owned()));
    }
    // Decode to bytes first: octal/hex escapes can spell out multi-byte characters.
    let mut buf = Vec::with_capacity(field.len());
    let mut i = 0;
    while i < field.len() {
        let b = field[i];
        i += 1;
        if b != b'\\' {
            buf.push(b);
            continue;
        }
        let Some(&e) = field.get(i) else {
            // A lone trailing backslash; the server never writes one, keep it literally.
            buf.push(b'\\');
            break;
        };
        i += 1;
        match e {
            b'b' => buf.push(0x08),
            b'f' => buf.push(0x0c),
            b'n' => buf.push(b'\n'),
            b'r' => buf.push(b'\r'),
            b't' => buf.push(b'\t'),
            b'v' => buf.push(0x0b),
            b'0'..=b'7' => {
                let mut v = u32::from(e - b'0');
                for _ in 0..2 {
                    match field.get(i) {
                        Some(&d @ b'0'..=b'7') => {
                            v = v * 8 + u32::from(d - b'0');
                            i += 1;
                        }
                        _ => break,
                    }
                }
                // Postgres keeps the low 8 bits of an out-of-range octal escape.
                buf.push((v & 0xff) as u8);
            }
            b'x' => match field.get(i).and_then(|&d| (d as char).to_digit(16)) {
                Some(hi) => {
                    i += 1;
                    let mut v = hi;
                    if let Some(lo) = field.get(i).and_then(|&d| (d as char).to_digit(16)) {
                        v = v * 16 + lo;
                        i += 1;
                    }
                    buf.push(v as u8);
                }
                // `\x` without a hex digit is just `x`.
                None => buf.push(b'x'),
            },
            other => buf.push(other),
        }
    }
    let s = String::from_utf8(buf).context("invalid UTF-8")?;
    Ok(Some(s))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn s(v: &str) -> Option<String> {
        Some(v.to_string())
    }

    fn parse_all(ncols: usize, data: &[u8]) -> Result<Vec<Row>> {
        let mut p = CopyParser::new(ncols);
        let mut out = Vec::new();
        p.feed(data, &mut out)?;
        p.finish()?;
        Ok(out)
    }

    #[test]
    fn simple_rows() {
        let rows = parse_all(3, b"1\tmilk\tf\n2\teggs\tt\n").unwrap();
        assert_eq!(
            rows,
            vec![
                vec![s("1"), s("milk"), s("f")],
                vec![s("2"), s("eggs"), s("t")]
            ]
        );
    }

    #[test]
    fn null_and_empty() {
        let rows = parse_all(3, b"\\N\t\t\\N\n\t\\N\t\n").unwrap();
        assert_eq!(
            rows,
            vec![vec![None, s(""), None], vec![s(""), None, s("")]]
        );
        // One column: an empty line is one empty string.
        assert_eq!(parse_all(1, b"\n").unwrap(), vec![vec![s("")]]);
    }

    #[test]
    fn escapes() {
        let cases: &[(&[u8], &str)] = &[
            (b"a\\\\b", "a\\b"),
            (b"\\b", "\u{8}"),
            (b"\\f", "\u{c}"),
            (b"\\n", "\n"),
            (b"\\r", "\r"),
            (b"\\t", "\t"),
            (b"\\v", "\u{b}"),
            (b"\\101", "A"),
            (b"\\1019", "A9"),
            (b"\\7", "\u{7}"),
            (b"\\0", "\0"),
            (b"\\x41", "A"),
            (b"\\x4a", "J"),
            (b"\\x4A", "J"),
            (b"\\x4", "\u{4}"),
            (b"\\x414", "A4"),
            (b"\\xg", "xg"),
            (b"\\x", "x"),
            (b"\\q", "q"),
            (b"\\\"", "\""),
            (b"\\N1", "N1"),
            (b"x\\N", "xN"),
            // Multi-byte characters spelled with octal and hex escapes.
            (b"\\303\\251", "é"),
            (b"\\xc3\\xa9", "é"),
            ("é🦭".as_bytes(), "é🦭"),
        ];
        for (input, expected) in cases {
            let mut data = input.to_vec();
            data.push(b'\n');
            let rows = parse_all(1, &data).unwrap();
            assert_eq!(rows, vec![vec![s(expected)]], "input {input:?}");
        }
    }

    #[test]
    fn column_count_errors() {
        assert!(parse_all(2, b"a\n").is_err());
        assert!(parse_all(2, b"a\tb\tc\n").is_err());
        assert!(parse_all(2, b"\n").is_err());
        assert!(parse_all(1, b"a\tb\n").is_err());
        let err = parse_all(2, b"a\tb\nc\n").unwrap_err();
        assert!(format!("{err:#}").contains("row 2"), "{err:#}");
    }

    #[test]
    fn invalid_utf8() {
        assert!(parse_all(1, b"\xff\n").is_err());
        assert!(parse_all(1, b"\\xff\n").is_err());
        assert!(parse_all(1, b"\\303\n").is_err());
    }

    #[test]
    fn finish_with_partial_row() {
        let mut p = CopyParser::new(2);
        let mut out = Vec::new();
        p.feed(b"a\tb\nc\t", &mut out).unwrap();
        assert_eq!(out.len(), 1);
        assert!(p.finish().is_err());

        // Data that ends exactly at a row boundary is fine, and so is no data at all.
        let p = CopyParser::new(2);
        p.finish().unwrap();
    }

    const SAMPLE: &[u8] = b"1\tmilk \xc3\xa9\xf0\x9f\xa6\xad\t\\N\n\
        2\t\\\\ \\t \\n \\r \\b \\f \\v\t\n\
        \\N\t\\101\\xc3\\xa9\t\\N\n\
        4\t\t\\N\n\
        5\ttail\\\\\tx\n";

    fn sample_expected() -> Vec<Row> {
        vec![
            vec![s("1"), s("milk é🦭"), None],
            vec![s("2"), s("\\ \t \n \r \u{8} \u{c} \u{b}"), s("")],
            vec![None, s("Aé"), None],
            vec![s("4"), s(""), None],
            vec![s("5"), s("tail\\"), s("x")],
        ]
    }

    #[test]
    fn split_at_every_position() {
        assert_eq!(parse_all(3, SAMPLE).unwrap(), sample_expected());
        for i in 0..=SAMPLE.len() {
            let mut p = CopyParser::new(3);
            let mut out = Vec::new();
            p.feed(&SAMPLE[..i], &mut out).unwrap();
            p.feed(&SAMPLE[i..], &mut out).unwrap();
            p.finish().unwrap();
            assert_eq!(out, sample_expected(), "split at {i}");
        }
    }

    #[test]
    fn split_into_three_and_single_bytes() {
        for i in 0..=SAMPLE.len() {
            for j in i..=SAMPLE.len() {
                let mut p = CopyParser::new(3);
                let mut out = Vec::new();
                for part in [&SAMPLE[..i], &SAMPLE[i..j], &SAMPLE[j..]] {
                    p.feed(part, &mut out).unwrap();
                }
                p.finish().unwrap();
                assert_eq!(out, sample_expected(), "split at {i},{j}");
            }
        }
        let mut p = CopyParser::new(3);
        let mut out = Vec::new();
        for b in SAMPLE {
            p.feed(std::slice::from_ref(b), &mut out).unwrap();
        }
        p.finish().unwrap();
        assert_eq!(out, sample_expected());
    }

    #[test]
    fn empty_chunks_are_fine() {
        let mut p = CopyParser::new(1);
        let mut out = Vec::new();
        p.feed(b"", &mut out).unwrap();
        p.feed(b"a", &mut out).unwrap();
        p.feed(b"", &mut out).unwrap();
        p.feed(b"\n", &mut out).unwrap();
        p.finish().unwrap();
        assert_eq!(out, vec![vec![s("a")]]);
    }
}
