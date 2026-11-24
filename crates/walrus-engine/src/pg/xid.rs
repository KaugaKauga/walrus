//! Snapshot visibility for the join (PLAN.md §7.1). Pure.

use anyhow::{Context, Result, bail};

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
        let mut parts = text.trim().split(':');
        let (Some(xmin), Some(xmax), Some(xip), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            bail!("malformed snapshot {text:?}: expected xmin:xmax:xip,...");
        };
        let num = |s: &str| {
            s.parse::<u64>()
                .with_context(|| format!("malformed snapshot {text:?}: bad xid {s:?}"))
        };
        let xip = if xip.is_empty() {
            Vec::new()
        } else {
            xip.split(',').map(num).collect::<Result<_>>()?
        };
        Ok(Snapshot {
            xmin: num(xmin)?,
            xmax: num(xmax)?,
            xip,
        })
    }

    /// Whether the transaction with this (32-bit, as streamed) xid is visible in the snapshot:
    /// `precedes(xid, xmin) || (precedes(xid, xmax) && xid ∉ xip)`, comparing the snapshot's
    /// 64-bit xids truncated to 32 bits.
    pub fn visible(&self, xid: u32) -> bool {
        precedes(xid, self.xmin as u32)
            || (precedes(xid, self.xmax as u32) && !self.xip.iter().any(|&p| p as u32 == xid))
    }

    /// Whether the transaction began after the snapshot was taken (`xid >= xmax`, wraparound
    /// aware). Streamed transactions after the first such one can't be visible: the join stops
    /// filtering there.
    pub fn began_after(&self, xid: u32) -> bool {
        !precedes(xid, self.xmax as u32)
    }
}

/// Postgres's wraparound-aware xid order: `(a - b) as i32 < 0`.
pub fn precedes(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) < 0
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn snap(xmin: u64, xmax: u64, xip: &[u64]) -> Snapshot {
        Snapshot {
            xmin,
            xmax,
            xip: xip.to_vec(),
        }
    }

    #[test]
    fn parse_forms() {
        assert_eq!(Snapshot::parse("10:20:").unwrap(), snap(10, 20, &[]));
        assert_eq!(Snapshot::parse("10:20:10").unwrap(), snap(10, 20, &[10]));
        assert_eq!(
            Snapshot::parse("10:20:10,13,17\n").unwrap(),
            snap(10, 20, &[10, 13, 17])
        );
        assert_eq!(
            Snapshot::parse("4294967290:8589934600:4294967295").unwrap(),
            snap(4294967290, 8589934600, &[4294967295])
        );
    }

    #[test]
    fn parse_rejects() {
        for bad in [
            "",
            "10",
            "10:20",
            "10:20:1:2",
            "a:20:",
            "10:b:",
            "10:20:1,,2",
            "10:20:1,x",
            "-1:2:",
            "10:20:,",
        ] {
            assert!(Snapshot::parse(bad).is_err(), "accepted {bad:?}");
        }
    }

    #[test]
    fn visibility_basic() {
        let s = snap(10, 20, &[12, 15]);
        assert!(s.visible(5));
        assert!(s.visible(9));
        assert!(s.visible(10)); // not < xmin, but < xmax and not in xip
        assert!(!s.visible(12));
        assert!(s.visible(13));
        assert!(!s.visible(15));
        assert!(s.visible(19));
        assert!(!s.visible(20));
        assert!(!s.visible(21));
    }

    #[test]
    fn empty_xip() {
        let s = snap(10, 20, &[]);
        assert!(s.visible(0));
        assert!(s.visible(19));
        assert!(!s.visible(20));
        assert!(!s.began_after(19));
        assert!(s.began_after(20));
        assert!(s.began_after(1000));
    }

    #[test]
    fn began_after_boundaries() {
        let s = snap(10, 20, &[12]);
        assert!(!s.began_after(12));
        assert!(!s.began_after(19));
        assert!(s.began_after(20));
    }

    #[test]
    fn wraparound_near_u32_max() {
        // xmax wrapped past u32::MAX: xids 4294967290..=4294967295 and 0..=4 precede it.
        let s = snap(4294967290, 4294967301, &[4294967295]);
        assert!(s.visible(4294967289));
        assert!(s.visible(4294967290));
        assert!(!s.visible(4294967295)); // in progress
        assert!(s.visible(0)); // 4294967296 < xmax
        assert!(s.visible(4));
        assert!(!s.visible(5)); // 4294967301 == xmax
        assert!(!s.visible(6));
        assert!(!s.began_after(4294967295));
        assert!(!s.began_after(4));
        assert!(s.began_after(5));
        assert!(s.began_after(100));
    }

    #[test]
    fn sixty_four_bit_xids_are_truncated() {
        // Epoch 1: 64-bit xids 4294967300.. have low 32 bits 4...
        let base = 1u64 << 32;
        let s = snap(base + 4, base + 20, &[base + 12]);
        assert!(s.visible(3));
        assert!(s.visible(4));
        assert!(!s.visible(12));
        assert!(s.visible(13));
        assert!(!s.visible(20));
        assert!(s.began_after(20));
        assert!(!s.began_after(19));

        // xmin in the previous epoch, xmax just after the wrap.
        let s = snap(base - 5, base + 3, &[base - 2, base + 1]);
        assert!(s.visible(u32::MAX - 6));
        assert!(s.visible(u32::MAX - 5));
        assert!(!s.visible(u32::MAX - 1)); // base - 2
        assert!(s.visible(u32::MAX));
        assert!(s.visible(0));
        assert!(!s.visible(1)); // base + 1
        assert!(s.visible(2));
        assert!(!s.visible(3));
        assert!(s.began_after(3));
        assert!(!s.began_after(2));
    }

    #[test]
    fn precedes_order() {
        assert!(precedes(1, 2));
        assert!(!precedes(2, 2));
        assert!(!precedes(3, 2));
        assert!(precedes(u32::MAX, 0));
        assert!(!precedes(0, u32::MAX));
    }
}
