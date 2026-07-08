//! A real linearizability checker (Wing & Gong, 1993), the algorithm Jepsen's
//! Knossos and Elle descend from.
//!
//! The monotonic-read check in `checker.rs` is a cheap *necessary* condition: it
//! catches reads that go backwards, but it can't decide full linearizability for
//! arbitrary concurrent histories. This module does the real thing for a
//! read/write register: given operations with concurrency (each has an
//! invocation time and a response time, so operations can overlap), it decides
//! whether there exists a total order that (a) respects real-time precedence -
//! if op A completed before op B started, A comes first - and (b) is a legal
//! register history (every read returns the most recently written value).
//!
//! Algorithm (Wing-Gong depth-first search with the standard memoization):
//!   - Consider the set of currently-*minimal* operations: those whose
//!     invocation is not preceded by an un-linearized operation's response.
//!   - Try to linearize each minimal op next: apply it to the model; if it's a
//!     read, it must match the model's current value.
//!   - Recurse on the remaining ops. If a branch dead-ends, backtrack (undo the
//!     op) and try the next candidate.
//!   - Memoize visited (remaining-set, model-value) states to prune the search,
//!     which is what makes it tractable in practice despite the worst-case
//!     exponential blowup.
//!
//! For a single register the "model" is just the current value, so state is
//! cheap and the search is fast for the history sizes a per-run check produces.

use std::collections::HashSet;

/// One operation in a concurrent history over a single register.
/// `inv` = invocation time (logical), `res` = response time. `inv < res`.
#[derive(Debug, Clone)]
pub enum HistoryOp {
    Write { value: u64, inv: u64, res: u64 },
    /// A read that returned `value` (None = key absent / read of "nothing").
    Read { value: Option<u64>, inv: u64, res: u64 },
}

impl HistoryOp {
    fn inv(&self) -> u64 {
        match self {
            HistoryOp::Write { inv, .. } | HistoryOp::Read { inv, .. } => *inv,
        }
    }
    fn res(&self) -> u64 {
        match self {
            HistoryOp::Write { res, .. } | HistoryOp::Read { res, .. } => *res,
        }
    }
}

/// Decide whether `history` is linearizable as a single read/write register.
/// The register starts holding `initial` (None = empty). Returns true iff some
/// valid linearization exists.
pub fn is_linearizable(history: &[HistoryOp], initial: Option<u64>) -> bool {
    let n = history.len();
    if n == 0 {
        return true;
    }
    // `done[i]` = op i already linearized. We track the set as a bitmask for
    // cheap memoization (histories per run are small).
    let done = vec![false; n];
    let mut memo: HashSet<(u64, Option<u64>)> = HashSet::new();
    search(history, &done, initial, 0, &mut memo)
}

fn mask(done: &[bool]) -> u64 {
    // Up to 64 ops per checked history (per-run histories are far smaller); the
    // caller can window if ever needed.
    let mut m = 0u64;
    for (i, &d) in done.iter().enumerate().take(64) {
        if d {
            m |= 1 << i;
        }
    }
    m
}

fn search(
    history: &[HistoryOp],
    done: &[bool],
    value: Option<u64>,
    completed: usize,
    memo: &mut HashSet<(u64, Option<u64>)>,
) -> bool {
    if completed == history.len() {
        return true; // linearized everything
    }
    let state = (mask(done), value);
    if memo.contains(&state) {
        return false; // already explored this (remaining-set, value) - dead end
    }

    // The earliest response time among not-yet-linearized ops. Any op whose
    // invocation is <= this time is "minimal" (nothing forces it to come later),
    // so it's a legal next candidate. This enforces real-time precedence: an op
    // that finished before another started must be linearized first.
    let min_res = history
        .iter()
        .enumerate()
        .filter(|(i, _)| !done[*i])
        .map(|(_, op)| op.res())
        .min()
        .unwrap();

    let mut done = done.to_vec();
    for i in 0..history.len() {
        if done[i] {
            continue;
        }
        // Candidate must be minimal: its invocation precedes the earliest
        // pending response (i.e. it doesn't strictly follow another pending op).
        if history[i].inv() > min_res {
            continue;
        }
        // Apply op i to the model.
        let next_value = match &history[i] {
            HistoryOp::Write { value, .. } => Some(*value),
            HistoryOp::Read { value: read_val, .. } => {
                if *read_val != value {
                    continue; // read doesn't match current model value -> illegal here
                }
                value
            }
        };
        done[i] = true;
        if search(history, &done, next_value, completed + 1, memo) {
            return true;
        }
        done[i] = false; // backtrack
    }

    memo.insert(state);
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(value: u64, inv: u64, res: u64) -> HistoryOp {
        HistoryOp::Write { value, inv, res }
    }
    fn r(value: Option<u64>, inv: u64, res: u64) -> HistoryOp {
        HistoryOp::Read { value, inv, res }
    }

    #[test]
    fn empty_history_is_linearizable() {
        assert!(is_linearizable(&[], None));
    }

    #[test]
    fn simple_sequential_history_ok() {
        // W(1) then R->1 then W(2) then R->2, all non-overlapping.
        let h = vec![w(1, 0, 1), r(Some(1), 2, 3), w(2, 4, 5), r(Some(2), 6, 7)];
        assert!(is_linearizable(&h, None));
    }

    #[test]
    fn read_of_stale_value_after_write_completes_is_not_linearizable() {
        // W(1) completes at t=1; then W(2) completes at t=3; then a read that
        // starts at t=4 (after both) returns 1. No valid order allows that.
        let h = vec![w(1, 0, 1), w(2, 2, 3), r(Some(1), 4, 5)];
        assert!(!is_linearizable(&h, None));
    }

    #[test]
    fn concurrent_read_can_pick_either_order() {
        // W(1) and W(2) overlap; a read overlapping both returns 1. Linearizable
        // by ordering W(2) then W(1) then read... but then a later read of 2
        // would fail. Here just the single read of 1 during concurrency is fine.
        let h = vec![w(1, 0, 10), w(2, 0, 10), r(Some(1), 1, 9)];
        assert!(is_linearizable(&h, None));
    }

    #[test]
    fn concurrent_writes_both_readable_in_some_order() {
        // Two concurrent writes, then two sequential reads returning 2 then...2.
        // Order W(1),W(2) makes both reads see 2. Linearizable.
        let h = vec![w(1, 0, 5), w(2, 0, 5), r(Some(2), 6, 7), r(Some(2), 8, 9)];
        assert!(is_linearizable(&h, None));
    }

    #[test]
    fn impossible_read_pair_is_rejected() {
        // Sequential reads that go 2 then 1 with no write between, after W(1),W(2):
        // once 2 is observed, 1 can never be observed again. Not linearizable.
        let h = vec![w(1, 0, 1), w(2, 2, 3), r(Some(2), 4, 5), r(Some(1), 6, 7)];
        assert!(!is_linearizable(&h, None));
    }
}
