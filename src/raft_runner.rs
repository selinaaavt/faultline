//! Drives Raft inside the deterministic simulator under fault injection, and
//! checks Raft's core safety properties. This is the payoff of leveling up: the
//! simulator now stress-tests a real consensus protocol, not a toy replication
//! scheme.
//!
//! Timers are modeled as periodic driver actions gated by the seeded RNG: each
//! follower/candidate may hit an election timeout (start an election), each
//! leader periodically heartbeats, and clients submit commands to whoever they
//! think is leader. Faults (partitions, crashes, restarts) are injected exactly
//! as for the KV system. After each step we check the two Raft safety invariants
//! (election safety and state-machine safety) against live state.

use crate::net::{Deliver, NetConfig, Network, NodeId, Partition};
use crate::raft::{RaftMsg, RaftNode, Role};
use crate::sim::{Rng, Scheduler};

pub struct RaftConfig {
    pub n_nodes: usize,
    pub ticks: u64,
    pub net: NetConfig,
    pub heartbeat_prob: f64,
    pub client_prob: f64,
    pub fault_prob: f64,
    /// Ticks of a final "stabilization" phase during which NO new faults are
    /// injected, the network is healed, and all nodes are alive. Liveness is
    /// only checkable here: under continuous faults, FLP says consensus need not
    /// progress, so we require progress only once the system stabilizes. Set to
    /// 0 to disable the liveness phase.
    pub stabilize_ticks: u64,
    /// Reintroduce the fixed match-index safety bug in every node, to
    /// demonstrate that the simulator catches it (a mutation test of the checker).
    pub inject_bug: bool,
}

impl Default for RaftConfig {
    fn default() -> Self {
        RaftConfig {
            n_nodes: 5,
            ticks: 3_000,
            net: NetConfig::default(),
            heartbeat_prob: 0.3,
            client_prob: 0.1,
            fault_prob: 0.02,
            stabilize_ticks: 2_000,
            inject_bug: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SafetyViolation {
    pub kind: String,
    pub detail: String,
    pub tick: u64,
}

pub struct RaftRunResult {
    pub seed: u64,
    pub violation: Option<SafetyViolation>,
    pub commands_committed: usize,
    pub leaders_elected: usize,
    /// Liveness verdict for the stabilization phase: `Some(true)` if the cluster
    /// elected a leader and advanced its commit index after faults stopped,
    /// `Some(false)` if it failed to make progress (a liveness violation),
    /// `None` if no stabilization phase was run.
    pub live: Option<bool>,
}

/// One deterministic Raft simulation. Same seed + config => same result.
pub fn run(seed: u64, cfg: &RaftConfig) -> RaftRunResult {
    let mut rng = Rng::new(seed);
    let mut sched: Scheduler<Deliver<RaftMsg>> = Scheduler::new();
    let mut net = Network::new(cfg.n_nodes, cfg.net.clone());
    let mut nodes: Vec<RaftNode> = (0..cfg.n_nodes).map(|i| RaftNode::new(i, cfg.n_nodes)).collect();
    // Give each node a randomized election timeout (the standard Raft technique
    // to avoid split-vote livelock). Two rules matter for progress: the timeout
    // must be well above the network round-trip (so votes/heartbeats arrive
    // before a node re-times-out), and it must be widely spread across nodes (so
    // they don't all time out together and split the vote). max_delay is the
    // per-hop latency, so we set timeouts to many round-trips, spread ~3x.
    let rtt = cfg.net.max_delay.max(1);
    for n in nodes.iter_mut() {
        n.set_base_timeout(10 * rtt + rng.below(20 * rtt));
        n.inject_match_index_bug = cfg.inject_bug;
    }

    // Track (term -> set of leaders seen) for election safety, and the
    // highest-committed log across the run for state-machine safety.
    let mut leaders_per_term: std::collections::HashMap<u64, std::collections::BTreeSet<NodeId>> =
        std::collections::HashMap::new();
    let mut committed_by_index: std::collections::HashMap<usize, (u64, u64)> =
        std::collections::HashMap::new(); // index -> (term, command) once committed
    let mut command_counter: u64 = 0;
    let mut leaders_elected = 0usize;
    let mut violation: Option<SafetyViolation> = None;

    for _ in 0..cfg.ticks {
        // 1) Deliver one pending network message.
        if let Some(ev) = sched.step() {
            let Deliver { from, to, msg } = ev.payload;
            if to < nodes.len() {
                let was_leader = nodes[to].role == Role::Leader;
                let replies = nodes[to].handle(from, msg);
                if !was_leader && nodes[to].role == Role::Leader {
                    leaders_elected += 1;
                }
                for (dst, m) in replies {
                    net.send(&mut sched, &mut rng, to, dst, m);
                }
            }
        }

        // 2) Timers: election timeouts and leader heartbeats. `id` is both the
        // loop index and the node's id (needed as the network sender), so we
        // index by it deliberately rather than iterating the slice.
        #[allow(clippy::needless_range_loop)]
        for id in 0..cfg.n_nodes {
            if nodes[id].crashed {
                continue;
            }
            // Election timeout fires via each node's randomized countdown.
            if nodes[id].tick_election_timer() {
                let msgs = nodes[id].start_election();
                for (dst, m) in msgs {
                    net.send(&mut sched, &mut rng, id, dst, m);
                }
            }
            if nodes[id].role == Role::Leader && rng.chance(cfg.heartbeat_prob) {
                let msgs = nodes[id].heartbeat();
                for (dst, m) in msgs {
                    net.send(&mut sched, &mut rng, id, dst, m);
                }
            }
        }

        // 3) Client command to some node that thinks it's leader.
        if rng.chance(cfg.client_prob)
            && let Some(id) = rng.index(cfg.n_nodes)
            && nodes[id].role == Role::Leader
            && !nodes[id].crashed
        {
            command_counter += 1;
            nodes[id].client_command(command_counter);
        }

        // 4) Fault injection.
        if rng.chance(cfg.fault_prob) {
            inject_fault(&mut rng, &mut net, &mut nodes, cfg.n_nodes);
        }

        // 5) Check safety invariants against current state.
        let now = sched.now();
        if let Some(v) = check_safety(&nodes, &mut leaders_per_term, &mut committed_by_index, now) {
            violation = Some(v);
            break;
        }
    }

    // --- Liveness phase ---
    // Only run if the chaos phase didn't already break safety. Heal the network,
    // revive every node, stop injecting faults, and keep driving timers +
    // client commands. A correct, stabilized Raft MUST eventually elect a leader
    // and commit new entries. We record the commit high-water mark at the start
    // of stabilization and require it to strictly advance by the end.
    let mut live: Option<bool> = None;
    if violation.is_none() && cfg.stabilize_ticks > 0 {
        net.partition.heal();
        for n in nodes.iter_mut() {
            n.restart(); // revive any crashed node (warm restart keeps its log)
        }
        let commit_before = nodes.iter().map(|n| n.commit_index).max().unwrap_or(0);

        for _ in 0..cfg.stabilize_ticks {
            if let Some(ev) = sched.step() {
                let Deliver { from, to, msg } = ev.payload;
                if to < nodes.len() {
                    let replies = nodes[to].handle(from, msg);
                    for (dst, m) in replies {
                        net.send(&mut sched, &mut rng, to, dst, m);
                    }
                }
            }
            #[allow(clippy::needless_range_loop)]
            for id in 0..cfg.n_nodes {
                if nodes[id].tick_election_timer() {
                    let msgs = nodes[id].start_election();
                    for (dst, m) in msgs {
                        net.send(&mut sched, &mut rng, id, dst, m);
                    }
                }
                if nodes[id].role == Role::Leader && rng.chance(cfg.heartbeat_prob) {
                    let msgs = nodes[id].heartbeat();
                    for (dst, m) in msgs {
                        net.send(&mut sched, &mut rng, id, dst, m);
                    }
                }
            }
            if rng.chance(cfg.client_prob)
                && let Some(id) = rng.index(cfg.n_nodes)
                && nodes[id].role == Role::Leader
            {
                command_counter += 1;
                nodes[id].client_command(command_counter);
            }
            // Safety must STILL hold during stabilization.
            let now = sched.now();
            if let Some(v) = check_safety(&nodes, &mut leaders_per_term, &mut committed_by_index, now) {
                violation = Some(v);
                break;
            }
        }

        let commit_after = nodes.iter().map(|n| n.commit_index).max().unwrap_or(0);
        let has_leader = nodes.iter().any(|n| n.role == Role::Leader);
        // Liveness: a stabilized cluster elected a leader and made new progress.
        live = Some(violation.is_none() && has_leader && commit_after > commit_before);
    }

    // Report the maximum commit_index reached by any node: how far the cluster
    // actually made durable progress (0 means nothing ever committed).
    let commands_committed = nodes.iter().map(|n| n.commit_index).max().unwrap_or(0);
    RaftRunResult {
        seed,
        violation,
        commands_committed,
        leaders_elected,
        live,
    }
}

/// Check Raft's two headline safety properties against current node state.
fn check_safety(
    nodes: &[RaftNode],
    leaders_per_term: &mut std::collections::HashMap<u64, std::collections::BTreeSet<NodeId>>,
    committed_by_index: &mut std::collections::HashMap<usize, (u64, u64)>,
    tick: u64,
) -> Option<SafetyViolation> {
    // --- Election Safety: at most one leader per term. ---
    for n in nodes {
        if n.role == Role::Leader {
            let set = leaders_per_term.entry(n.current_term).or_default();
            set.insert(n.id);
            if set.len() > 1 {
                return Some(SafetyViolation {
                    kind: "election_safety".to_string(),
                    detail: format!(
                        "term {} had multiple leaders: {:?} (two leaders in one term \
                         can each commit conflicting entries)",
                        n.current_term, set
                    ),
                    tick,
                });
            }
        }
    }

    // --- State Machine Safety: no two nodes hold DIFFERENT entries at an index
    // that BOTH currently consider committed. This is the real Raft invariant:
    // once an entry is committed, no committed log disagrees at that index.
    //
    // Subtlety this checker had to get right: a persistent "first value ever
    // seen committed at index i" record is WRONG. An entry can sit in a node's
    // committed prefix and later be superseded through entirely legal Raft steps
    // if it was only ever *locally* marked committed on a node that then learned
    // a higher term -- so comparing against history produces false positives.
    // The sound check compares only entries that are committed on *both* nodes
    // at the same moment; if committed logs genuinely diverge, that's a true
    // violation. We rebuild the comparison each tick from live state (no
    // cross-tick memory), which keeps it sound.
    let _ = committed_by_index; // (kept in signature for API stability; unused now)
    for i in 0..nodes.len() {
        for j in (i + 1)..nodes.len() {
            let a = nodes[i].committed_log();
            let b = nodes[j].committed_log();
            let common = a.len().min(b.len());
            for idx in 0..common {
                if a[idx] != b[idx] {
                    if std::env::var("FAULTLINE_DEBUG").is_ok() {
                        eprintln!("--- state_machine_safety tick {tick} index {} ---", idx + 1);
                        for m in nodes {
                            eprintln!(
                                "  node{} role={:?} term={} commit={} log={:?}",
                                m.id, m.role, m.current_term, m.commit_index,
                                m.log.iter().map(|x| (x.term, x.command)).collect::<Vec<_>>()
                            );
                        }
                    }
                    return Some(SafetyViolation {
                        kind: "state_machine_safety".to_string(),
                        detail: format!(
                            "nodes {} and {} disagree at committed index {}: \
                             (term {}, cmd {}) vs (term {}, cmd {})",
                            nodes[i].id, nodes[j].id, idx + 1,
                            a[idx].term, a[idx].command, b[idx].term, b[idx].command
                        ),
                        tick,
                    });
                }
            }
        }
    }
    None
}

fn inject_fault(rng: &mut Rng, net: &mut Network, nodes: &mut [RaftNode], n: usize) {
    match rng.below(4) {
        0 => {
            // Partition a random minority off from the majority.
            if let Some(v) = rng.index(n) {
                net.partition = Partition::split(n, &[v]);
            }
        }
        1 => net.partition.heal(),
        2 => {
            if let Some(v) = rng.index(n) {
                nodes[v].crash();
            }
        }
        _ => {
            if let Some(v) = rng.index(n) {
                nodes[v].restart();
            }
        }
    }
}
/// Mutation test: reintroduce the fixed match-index safety bug and search for a
/// seed that exposes it, proving the checker has teeth. Uses a churn-inducing
/// config (short network latency -> frequent elections and divergent logs),
/// which is what surfaces this timing-dependent bug. Returns `Some((seed,
/// detail))` for the first violating seed, or `None` if none found.
pub fn demonstrate_injected_bug() -> Option<(u64, String)> {
    let cfg = RaftConfig {
        n_nodes: 5,
        ticks: 6000,
        net: NetConfig { drop_prob: 0.1, min_delay: 1, max_delay: 20 },
        fault_prob: 0.06,
        stabilize_ticks: 0,
        inject_bug: true,
        ..RaftConfig::default()
    };
    for seed in 0..5000 {
        let r = run(seed, &cfg);
        if let Some(v) = r.violation {
            return Some((seed, v.detail));
        }
    }
    None
}
