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

impl Snapshot {
    /// Exact visibility of a full 64-bit xid (no wraparound ambiguity).
    pub fn visible64(&self, xid: u64) -> bool {
        xid < self.xmin || (xid < self.xmax && !self.xip.contains(&xid))
    }
}

/// Widen a streamed 32-bit xid to 64 bits: the value with these low 32 bits nearest to
/// `reference` (a recent 64-bit xid). Valid while the two are less than 2^31 apart, which holds
/// for xids streamed close together.
pub fn extend(reference: u64, xid: u32) -> u64 {
    let candidate = (reference & !0xffff_ffff) | u64::from(xid);
    if candidate > reference.saturating_add(1 << 31) {
        candidate.saturating_sub(1 << 32)
    } else if candidate.saturating_add(1 << 31) < reference {
        candidate + (1 << 32)
    } else {
        candidate
    }
}

/// Transactions the engine has applied that a catch-up snapshot might not see yet
/// (PLAN.md §7.2, I8).
///
/// Postgres can stream a commit before the transaction leaves the process array, i.e. before it
/// is visible to new snapshots, and with synchronous replication that gap lasts a standby round
/// trip or longer. Postgres can't tell a session about such a transaction: it isn't in the
/// snapshot's in-progress list when its xid is at or above `xmax`, and `pg_xact_status` reports
/// it as in progress. Only the engine knows it has already streamed it, so the engine keeps the
/// xids it applied and a session rejects a snapshot that misses one of them.
pub struct RecentXids {
    /// `(xid, commit LSN)` in commit order.
    entries: std::sync::Mutex<std::collections::VecDeque<(u64, u64)>>,
}

/// Entries kept at most. Pruning by snapshot `xmin` normally keeps the list tiny; the cap only
/// bounds memory when no catch-ups run for a long time.
pub const RECENT_XIDS_CAP: usize = 1 << 18;

impl Default for RecentXids {
    fn default() -> Self {
        RecentXids {
            entries: std::sync::Mutex::new(std::collections::VecDeque::new()),
        }
    }
}

impl RecentXids {
    /// Record an applied transaction. The writer calls this before the index commit that
    /// covers `lsn`, so a session that reads `P0 >= lsn` always finds the entry.
    pub fn push(&self, xid: u64, lsn: u64) {
        let mut entries = self.lock();
        if entries.len() >= RECENT_XIDS_CAP {
            entries.pop_front();
        }
        entries.push_back((xid, lsn));
    }

    /// Whether some transaction with commit LSN `<= p0` is not visible in `snapshot`: a catch-up
    /// on that snapshot would miss it (it is neither in the snapshot nor after `P0` on the live
    /// stream), so the session must take a later snapshot.
    ///
    /// Otherwise forget every transaction below the snapshot's `xmin`: it has finished, so it is
    /// visible to every later snapshot.
    pub fn misses_applied(&self, snapshot: &Snapshot, p0: u64) -> bool {
        let mut entries = self.lock();
        if entries
            .iter()
            .any(|&(xid, lsn)| lsn <= p0 && !snapshot.visible64(xid))
        {
            return true;
        }
        entries.retain(|&(xid, _)| xid >= snapshot.xmin);
        false
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, std::collections::VecDeque<(u64, u64)>> {
        // A panic while holding this lock leaves the list intact (push/retain are atomic).
        self.entries.lock().unwrap_or_else(|e| e.into_inner())
    }
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

#[cfg(test)]
mod recent_tests {
    use super::*;

    fn snap(xmin: u64, xmax: u64, xip: &[u64]) -> Snapshot {
        Snapshot {
            xmin,
            xmax,
            xip: xip.to_vec(),
        }
    }

    #[test]
    fn extend_picks_the_nearest_epoch() {
        let base = (5u64 << 32) | 100;
        assert_eq!(extend(base, 150), (5 << 32) | 150);
        assert_eq!(extend(base, 50), (5 << 32) | 50);
        // Low bits wrapped past the reference: next epoch.
        let near_end = (5u64 << 32) | 0xffff_fff0;
        assert_eq!(extend(near_end, 5), (6 << 32) | 5);
        // Reference just past a wrap, xid just before it: previous epoch.
        let after_wrap = (6u64 << 32) | 3;
        assert_eq!(extend(after_wrap, 0xffff_fffe), (5 << 32) | 0xffff_fffe);
    }

    #[test]
    fn visible64_is_exact() {
        let s = snap(100, 110, &[103, 105]);
        assert!(s.visible64(99));
        assert!(s.visible64(104));
        assert!(!s.visible64(103));
        assert!(
            !s.visible64(110),
            "xid >= xmax is not visible, even if it started earlier"
        );
    }

    #[test]
    fn rejects_a_snapshot_missing_an_applied_commit() {
        let recent = RecentXids::default();
        recent.push(100, 10);
        recent.push(101, 20); // streamed, but still running when the snapshot is taken
        // The case Postgres can't report: xid == xmax, so not in the in-progress list.
        assert!(recent.misses_applied(&snap(101, 101, &[]), 20));
        // A catch-up at P0 = 10 doesn't need txn 101 (it arrives on the live stream).
        assert!(!recent.misses_applied(&snap(101, 101, &[]), 10));
        // Later, it is visible.
        assert!(!recent.misses_applied(&snap(102, 102, &[]), 20));
    }

    #[test]
    fn prunes_below_xmin_and_caps() {
        let recent = RecentXids::default();
        for x in 0..10 {
            recent.push(x, x);
        }
        assert!(!recent.misses_applied(&snap(7, 12, &[]), 9));
        assert_eq!(recent.lock().len(), 3);
        for x in 0..(RECENT_XIDS_CAP as u64 + 5) {
            recent.push(100 + x, 100 + x);
        }
        assert_eq!(recent.lock().len(), RECENT_XIDS_CAP);
    }
}
