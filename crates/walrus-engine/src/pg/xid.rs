//! Snapshot visibility for the join (PLAN.md §7.1). Pure.

use anyhow::Result;

/// A Postgres snapshot (`pg_snapshot`), as used by a catch-up transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub xmin: u64,
    pub xmax: u64,
    /// In-progress transaction ids.
    pub xip: Vec<u64>,
}

impl Snapshot {
    /// Parse the text form of `pg_snapshot`: `xmin:xmax:xip1,xip2,...` (the xip list may be
    /// empty: `10:20:`).
    pub fn parse(text: &str) -> Result<Snapshot> {
        let _ = text;
        todo!()
    }

    /// Whether the transaction with this (32-bit, as streamed) xid is visible in the snapshot:
    /// `precedes(xid, xmin) || (precedes(xid, xmax) && xid ∉ xip)`, comparing the snapshot's
    /// 64-bit xids truncated to 32 bits.
    pub fn visible(&self, xid: u32) -> bool {
        let _ = xid;
        todo!()
    }

    /// Whether the transaction began after the snapshot was taken (`xid >= xmax`, wraparound
    /// aware). Streamed transactions after the first such one can't be visible: the join stops
    /// filtering there.
    pub fn began_after(&self, xid: u32) -> bool {
        let _ = xid;
        todo!()
    }
}

/// Postgres's wraparound-aware xid order: `(a - b) as i32 < 0`.
pub fn precedes(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) < 0
}
