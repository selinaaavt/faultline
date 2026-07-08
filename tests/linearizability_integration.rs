//! Cross-check: the real Wing-Gong linearizability checker agrees with the
//! cheap monotonic-read checker on the primary-backup bug -- and, being a full
//! decision procedure, it's the authoritative verdict.

use faultline::linearizability::{is_linearizable, HistoryOp};

#[test]
fn primary_backup_stale_read_is_not_linearizable() {
    // The essence of the bug faultline finds: value 2 is written and observed,
    // then a stale backup serves 1 afterwards. As a concurrent register history
    // with these (non-overlapping) intervals, this is not linearizable.
    let h = vec![
        HistoryOp::Write { value: 1, inv: 0, res: 1 },
        HistoryOp::Write { value: 2, inv: 2, res: 3 },
        HistoryOp::Read { value: Some(2), inv: 4, res: 5 },
        HistoryOp::Read { value: Some(1), inv: 6, res: 7 }, // stale -> illegal
    ];
    assert!(!is_linearizable(&h, None), "stale backup read must be non-linearizable");
}

#[test]
fn correct_primary_reads_are_linearizable() {
    // The corrected design (always read latest) yields a monotonic history,
    // which the full checker confirms is linearizable.
    let h = vec![
        HistoryOp::Write { value: 1, inv: 0, res: 1 },
        HistoryOp::Read { value: Some(1), inv: 2, res: 3 },
        HistoryOp::Write { value: 2, inv: 4, res: 5 },
        HistoryOp::Read { value: Some(2), inv: 6, res: 7 },
        HistoryOp::Read { value: Some(2), inv: 8, res: 9 },
    ];
    assert!(is_linearizable(&h, None));
}
