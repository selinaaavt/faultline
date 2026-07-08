//! The consistency checker -- decides whether a run exposed a bug.
//!
//! We record every client operation in a `History` and then check a
//! consistency property over it. Full linearizability checking is NP-hard in
//! general, so we check a strong, cheap-to-verify *necessary condition* that
//! any linearizable register must satisfy:
//!
//!   **Monotonic reads / no stale-back-in-time reads.** Values written to the
//!   primary carry strictly increasing versions (1, 2, 3, ...). Once any client
//!   has observed value `v`, no later read may return a value older than `v`.
//!   A linearizable store can never travel backwards in time like that.
//!
//! Primary-backup replication with local reads on backups *violates* this: under
//! a partition or reordering, a client can read a backup that is behind, seeing
//! an older value after a newer one was already visible elsewhere. When that
//! happens, the checker reports the exact operations involved -- and because the
//! run is deterministic, the seed reproduces it.

use crate::net::NodeId;
use crate::sim::Tick;

/// One recorded client operation. `seq` is a strictly-increasing operation
/// index giving a total, unambiguous order (two ops never share a `seq`, unlike
/// logical ticks which several ops can share). `tick` is kept for reporting.
#[derive(Debug, Clone)]
pub enum Op {
    Write { value: u64, seq: u64, tick: Tick },
    Read { value: Option<u64>, node: NodeId, seq: u64, tick: Tick },
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::Write { value, seq, .. } => write!(f, "#{seq} WRITE {value}"),
            Op::Read { value, node, seq, .. } => match value {
                Some(v) => write!(f, "#{seq} READ={v} (node {node})"),
                None => write!(f, "#{seq} READ=none (node {node})"),
            },
        }
    }
}

/// A detected consistency violation, with enough detail to explain it.
#[derive(Debug, Clone)]
pub struct Violation {
    pub kind: String,
    pub detail: String,
    pub at_tick: Tick,
}

/// Check the monotonic-read property over the operation history. Returns the
/// first violation found, or `None` if the run stayed consistent.
pub fn check_linearizability(history: &[Op]) -> Option<Violation> {
    // The greatest value any client has observed so far, and the op `seq` at
    // which it was observed. Ordering is by `seq` (a total order), so a flagged
    // read genuinely happened *after* the observation -- not merely at the same
    // logical tick. That distinction is what keeps this free of false positives.
    let mut max_observed: u64 = 0;
    let mut observed_seq: u64 = 0;

    for op in history {
        match op {
            Op::Write { value, .. } => {
                if *value > max_observed {
                    max_observed = *value;
                }
            }
            Op::Read { value, node, seq, tick } => {
                if let Some(v) = value {
                    if *v >= max_observed {
                        max_observed = *v;
                        observed_seq = *seq;
                    } else {
                        // Strictly-later op (larger seq) returned an older value:
                        // a real backwards-in-time read.
                        return Some(Violation {
                            kind: "stale_read".to_string(),
                            detail: format!(
                                "op #{seq}: node {node} read value {v} (tick {tick}), but \
                                 value {max_observed} was already observed by op \
                                 #{observed_seq} (read went backwards in time)"
                            ),
                            at_tick: *tick,
                        });
                    }
                } else if max_observed > 0 {
                    return Some(Violation {
                        kind: "lost_value".to_string(),
                        detail: format!(
                            "op #{seq}: node {node} read empty (tick {tick}), but value \
                             {max_observed} was already observed by op #{observed_seq} \
                             (committed value disappeared)"
                        ),
                        at_tick: *tick,
                    });
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consistent_history_passes() {
        let h = vec![
            Op::Write { value: 1, seq: 0, tick: 1 },
            Op::Read { value: Some(1), node: 1, seq: 1, tick: 2 },
            Op::Write { value: 2, seq: 2, tick: 3 },
            Op::Read { value: Some(2), node: 0, seq: 3, tick: 4 },
        ];
        assert!(check_linearizability(&h).is_none());
    }

    #[test]
    fn stale_read_is_caught() {
        let h = vec![
            Op::Write { value: 1, seq: 0, tick: 1 },
            Op::Write { value: 2, seq: 1, tick: 2 },
            Op::Read { value: Some(2), node: 0, seq: 2, tick: 3 }, // sees 2
            Op::Read { value: Some(1), node: 2, seq: 3, tick: 4 }, // back to 1 -- BUG
        ];
        let v = check_linearizability(&h).expect("should catch the stale read");
        assert_eq!(v.kind, "stale_read");
        assert_eq!(v.at_tick, 4);
    }

    #[test]
    fn lost_value_is_caught() {
        let h = vec![
            Op::Write { value: 5, seq: 0, tick: 1 },
            Op::Read { value: Some(5), node: 1, seq: 1, tick: 2 },
            Op::Read { value: None, node: 2, seq: 2, tick: 3 }, // value vanished -- BUG
        ];
        let v = check_linearizability(&h).expect("should catch the lost value");
        assert_eq!(v.kind, "lost_value");
    }

    #[test]
    fn reads_before_any_write_are_fine() {
        let h = vec![
            Op::Read { value: None, node: 1, seq: 0, tick: 1 },
            Op::Write { value: 1, seq: 1, tick: 2 },
            Op::Read { value: Some(1), node: 0, seq: 2, tick: 3 },
        ];
        assert!(check_linearizability(&h).is_none());
    }
}
