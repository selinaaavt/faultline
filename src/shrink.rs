//! Shrinking: reduce a failing operation history to a minimal one that still
//! violates. A raw failing run has hundreds of ops; the essential story is
//! usually 3-4. Minimizing it is what makes a DST failure *debuggable*, and it's
//! a signature feature of real ones (QuickCheck/Hypothesis-style delta
//! debugging).
//!
//! Algorithm (greedy delta deb: "ddmin"-lite): repeatedly pass over the history
//! trying to delete each operation; keep any deletion that preserves the
//! violation. Repeat until a full pass removes nothing (a local minimum). The
//! result is a subsequence of the original ops that still fails the checker --
//! typically the write that set the high value, the read that observed it, and
//! the stale read that went backwards.

use crate::checker::{check_linearizability, Op, Violation};

/// Reduce `history` to a minimal subsequence that still violates. Returns the
/// minimized history and its violation. If `history` doesn't violate, returns it
/// unchanged with `None`.
pub fn shrink(history: &[Op]) -> (Vec<Op>, Option<Violation>) {
    let original = check_linearizability(history);
    if original.is_none() {
        return (history.to_vec(), None);
    }

    let mut current: Vec<Op> = history.to_vec();
    loop {
        let mut removed_any = false;
        let mut i = 0;
        while i < current.len() {
            // Try deleting op i.
            let mut candidate = current.clone();
            candidate.remove(i);
            if check_linearizability(&candidate).is_some() {
                // Still fails without op i -> op i was inessential; drop it.
                current = candidate;
                removed_any = true;
                // Don't advance i: the next op shifted into this slot.
            } else {
                i += 1;
            }
        }
        if !removed_any {
            break; // local minimum: nothing else can be removed
        }
    }
    let v = check_linearizability(&current);
    (current, v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(value: u64, seq: u64) -> Op {
        Op::Write { value, seq, tick: seq }
    }
    fn r(value: Option<u64>, seq: u64) -> Op {
        Op::Read { value, node: 1, seq, tick: seq }
    }

    #[test]
    fn shrinks_to_minimal_stale_read() {
        // A long history with lots of consistent noise around one stale read.
        let mut h = vec![w(1, 0), r(Some(1), 1), w(2, 2), w(3, 3), r(Some(3), 4)];
        // ... more consistent ops ...
        for s in 5..30 {
            h.push(r(Some(3), s));
        }
        // The bug: a read goes back to 2 after 3 was observed.
        h.push(r(Some(2), 30));
        // ... trailing noise ...
        for s in 31..40 {
            h.push(r(Some(3), s));
        }

        let (min, v) = shrink(&h);
        assert!(v.is_some(), "shrunk history must still violate");
        // The minimal case needs only: something establishing 3 was observed,
        // then the read of 2. That's far smaller than the original.
        assert!(min.len() < h.len(), "shrinking should reduce the history");
        assert!(min.len() <= 4, "minimal stale-read case is tiny, got {}", min.len());
    }

    #[test]
    fn consistent_history_is_unchanged() {
        let h = vec![w(1, 0), r(Some(1), 1), w(2, 2), r(Some(2), 3)];
        let (out, v) = shrink(&h);
        assert!(v.is_none());
        assert_eq!(out.len(), h.len(), "a passing history is returned unchanged");
    }
}
