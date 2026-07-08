//! End-to-end tests for the deterministic simulation tester itself.
//!
//! These pin down the two properties that make the tool trustworthy:
//!   1. It *finds* the known bug in the buggy config (soundness of detection).
//!   2. It does *not* flag the correct config across many seeds (no false
//!      positives) -- otherwise "found a bug" would be meaningless.
//!   3. Runs are deterministic: same seed => identical outcome.

use faultline::runner::{run, RunConfig};

#[test]
fn finds_bug_in_buggy_config() {
    let cfg = RunConfig::default(); // reads from any node (buggy)
    // Some seed within a small range must expose a violation.
    let found = (0..200).any(|seed| run(seed, &cfg).violation.is_some());
    assert!(found, "DST should find the primary-backup stale-read bug");
}

#[test]
fn correct_config_has_no_violations() {
    // Reads from the primary only -> correct. Across many seeds, zero
    // violations. This is what proves the checker isn't just flagging noise.
    let cfg = RunConfig { read_from_primary_only: true, ..RunConfig::default() };
    for seed in 0..500 {
        let r = run(seed, &cfg);
        assert!(
            r.violation.is_none(),
            "correct (primary-read) config must never violate, but seed {seed} did: {:?}",
            r.violation
        );
    }
}

#[test]
fn runs_are_deterministic() {
    let cfg = RunConfig::default();
    for seed in [0u64, 1, 7, 42, 100] {
        let a = run(seed, &cfg);
        let b = run(seed, &cfg);
        assert_eq!(a.ops, b.ops, "seed {seed}: op count must match");
        assert_eq!(a.sent, b.sent, "seed {seed}: messages sent must match");
        match (a.violation, b.violation) {
            (Some(x), Some(y)) => {
                assert_eq!(x.kind, y.kind);
                assert_eq!(x.at_tick, y.at_tick);
            }
            (None, None) => {}
            _ => panic!("seed {seed}: violation presence diverged between runs"),
        }
    }
}
