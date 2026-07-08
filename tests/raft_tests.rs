//! Unit tests for the Raft implementation's core mechanics: election, the
//! up-to-date-log voting restriction, log matching, and the commit rule. These
//! exercise the logic directly (no network) so failures point at the algorithm.

use faultline::raft::{RaftMsg, RaftNode, Role};

#[test]
fn candidate_wins_with_majority_votes() {
    let mut n = RaftNode::new(0, 3);
    let reqs = n.start_election();
    assert_eq!(n.role, Role::Candidate);
    assert_eq!(reqs.len(), 2, "requests one vote per peer");
    // One peer grants -> that's 2 of 3 (incl. self) -> majority -> leader.
    n.handle(1, RaftMsg::RequestVoteReply { term: n.current_term, granted: true });
    assert_eq!(n.role, Role::Leader);
}

#[test]
fn candidate_stays_without_majority() {
    let mut n = RaftNode::new(0, 5);
    n.start_election();
    n.handle(1, RaftMsg::RequestVoteReply { term: n.current_term, granted: true });
    // 2 of 5 is not a majority.
    assert_eq!(n.role, Role::Candidate);
}

#[test]
fn higher_term_forces_step_down() {
    let mut n = RaftNode::new(0, 3);
    n.start_election(); // becomes candidate at term 1
    n.handle(1, RaftMsg::RequestVoteReply { term: 1, granted: true }); // leader
    assert_eq!(n.role, Role::Leader);
    // A message from a higher term must make us step down.
    n.handle(2, RaftMsg::AppendEntries {
        term: 5, leader: 2, prev_log_index: 0, prev_log_term: 0,
        entries: vec![], leader_commit: 0,
    });
    assert_eq!(n.role, Role::Follower);
    assert_eq!(n.current_term, 5);
}

#[test]
fn vote_denied_to_stale_log() {
    // A voter with a longer/newer log must reject a candidate whose log is behind.
    let mut voter = RaftNode::new(1, 3);
    // Give the voter a log at term 2.
    voter.handle(0, RaftMsg::AppendEntries {
        term: 2, leader: 0, prev_log_index: 0, prev_log_term: 0,
        entries: vec![
            faultline::raft::LogEntry { term: 2, command: 10 },
            faultline::raft::LogEntry { term: 2, command: 11 },
        ],
        leader_commit: 0,
    });
    // Candidate at a higher term but with an empty (stale) log.
    let reply = voter.handle(2, RaftMsg::RequestVote {
        term: 3, candidate: 2, last_log_index: 0, last_log_term: 0,
    });
    match reply.first().map(|(_, m)| m) {
        Some(RaftMsg::RequestVoteReply { granted, .. }) => {
            assert!(!granted, "must deny vote to a candidate with a less up-to-date log");
        }
        _ => panic!("expected a vote reply"),
    }
}

#[test]
fn append_entries_rejects_on_log_mismatch() {
    let mut f = RaftNode::new(1, 3);
    // Leader claims prev_log_index=5 which the empty follower doesn't have.
    let reply = f.handle(0, RaftMsg::AppendEntries {
        term: 1, leader: 0, prev_log_index: 5, prev_log_term: 1,
        entries: vec![], leader_commit: 0,
    });
    match reply.first().map(|(_, m)| m) {
        Some(RaftMsg::AppendEntriesReply { success, .. }) => {
            assert!(!success, "log-matching check must reject the mismatch");
        }
        _ => panic!("expected an append reply"),
    }
}

#[test]
fn conflicting_entry_truncates_follower_log() {
    let mut f = RaftNode::new(1, 3);
    // First, accept two entries at term 1.
    f.handle(0, RaftMsg::AppendEntries {
        term: 1, leader: 0, prev_log_index: 0, prev_log_term: 0,
        entries: vec![
            faultline::raft::LogEntry { term: 1, command: 1 },
            faultline::raft::LogEntry { term: 1, command: 2 },
        ],
        leader_commit: 0,
    });
    assert_eq!(f.log.len(), 2);
    // Now a new leader (term 2) overwrites index 2 with a different-term entry.
    f.handle(2, RaftMsg::AppendEntries {
        term: 2, leader: 2, prev_log_index: 1, prev_log_term: 1,
        entries: vec![faultline::raft::LogEntry { term: 2, command: 99 }],
        leader_commit: 0,
    });
    assert_eq!(f.log.len(), 2, "conflicting suffix truncated, new entry appended");
    assert_eq!(f.log[1].command, 99);
    assert_eq!(f.log[1].term, 2);
}

#[test]
fn leader_commits_replicated_current_term_entry() {
    let mut leader = RaftNode::new(0, 3);
    leader.start_election();
    leader.handle(1, RaftMsg::RequestVoteReply { term: 1, granted: true }); // leader, term 1
    assert!(leader.client_command(42));
    // Two followers ack the entry (match_index 1) -> majority -> commit.
    leader.handle(1, RaftMsg::AppendEntriesReply { term: 1, success: true, match_index: 1 });
    assert_eq!(leader.commit_index, 1, "entry on a majority in current term must commit");
}

// --- Integration: Raft under the deterministic simulator with fault injection ---

use faultline::raft_runner::{run as raft_run, RaftConfig};

#[test]
fn raft_holds_safety_across_many_seeds() {
    // The headline property: correct Raft survives adversarial fault injection.
    // No seed in this range may produce a safety violation.
    let cfg = RaftConfig::default();
    for seed in 0..300 {
        let r = raft_run(seed, &cfg);
        assert!(
            r.violation.is_none(),
            "seed {seed} produced a Raft safety violation: {:?}",
            r.violation
        );
    }
}

#[test]
fn raft_makes_real_progress() {
    // A safe-but-idle Raft would be a meaningless test. Confirm commands actually
    // commit under the fault load (across seeds, meaningful commit volume).
    let cfg = RaftConfig::default();
    let total: usize = (0..100).map(|s| raft_run(s, &cfg).commands_committed).sum();
    assert!(total > 50, "expected real commit progress across seeds, got {total}");
}

#[test]
fn raft_runs_are_deterministic() {
    let cfg = RaftConfig::default();
    for seed in [0u64, 1, 42, 1111] {
        let a = raft_run(seed, &cfg);
        let b = raft_run(seed, &cfg);
        assert_eq!(a.commands_committed, b.commands_committed, "seed {seed}");
        assert_eq!(a.leaders_elected, b.leaders_elected, "seed {seed}");
    }
}

#[test]
fn raft_recovers_liveness_after_stabilization() {
    // After faults stop and the network heals, a correct cluster must elect a
    // leader and commit new entries. Every seed should report live=true.
    let cfg = RaftConfig::default();
    for seed in 0..200 {
        let r = raft_run(seed, &cfg);
        // Only meaningful when safety held (it always does here).
        assert!(r.violation.is_none(), "unexpected safety violation at seed {seed}");
        assert_eq!(r.live, Some(true), "seed {seed} failed to recover liveness");
    }
}

#[test]
fn liveness_check_has_teeth() {
    // A stabilization window too short to elect + commit must report live=false.
    // If this passed as live=true, the liveness check would be meaningless.
    let cfg = RaftConfig { stabilize_ticks: 3, ..RaftConfig::default() };
    let failures = (0..50).filter(|&s| raft_run(s, &cfg).live == Some(false)).count();
    assert!(failures >= 45, "liveness check should fail when there's no time to progress, got {failures}/50");
}

#[test]
fn injected_bug_is_caught_but_fixed_version_is_safe() {
    // Mutation test: with the match-index bug reintroduced, the checker must find
    // a safety violation (proving it has teeth); with the bug fixed (inject_bug
    // = false, the default), the same churn-heavy profile stays safe.
    use faultline::net::NetConfig;

    let churn = |inject_bug| RaftConfig {
        n_nodes: 5,
        ticks: 6000,
        net: NetConfig { drop_prob: 0.1, min_delay: 1, max_delay: 20 },
        fault_prob: 0.06,
        stabilize_ticks: 0,
        inject_bug,
        ..RaftConfig::default()
    };

    // Buggy: some seed in a modest range must expose a violation.
    let buggy = churn(true);
    let caught = (0..1500).any(|s| raft_run(s, &buggy).violation.is_some());
    assert!(caught, "the reintroduced bug must be caught by the checker");

    // Fixed: the SAME profile must stay safe across the same range.
    let fixed = churn(false);
    for s in 0..1500 {
        assert!(
            raft_run(s, &fixed).violation.is_none(),
            "fixed protocol must hold safety under the churn profile (seed {s})"
        );
    }
}
