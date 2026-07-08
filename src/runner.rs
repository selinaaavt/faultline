//! The simulation runner -- spawns the system, injects faults, records history,
//! and checks invariants. A whole run is a pure function of `(seed, config)`.
//!
//! Each tick of the driver, guided by the seeded RNG, it may: issue a client
//! write to the primary, issue a client read somewhere, inject a fault (partition,
//! heal, crash, restart), and always drains any network messages that are due.
//! Every client operation is appended to a `History`; after the run the
//! `checker` inspects that history for a consistency violation.
//!
//! The key property: because every choice comes from the seed, a run that finds
//! a violation can be replayed exactly by re-running the same seed.

use crate::checker::{check_linearizability, Op, Violation};
use crate::kv::{Msg, Node};
use crate::net::{Deliver, NetConfig, Network, NodeId, Partition};
use crate::sim::{Rng, Scheduler};

pub struct RunConfig {
    pub n_nodes: usize,
    pub ticks: u64,
    pub net: NetConfig,
    pub write_prob: f64,
    pub read_prob: f64,
    pub fault_prob: f64,
    /// If true, all reads are served by the primary only. This is the *correct*
    /// configuration: the primary always holds the latest acknowledged value, so
    /// no read can go backwards in time. Contrast with the default (reads served
    /// by any node), whose stale backups violate consistency. Running the tool
    /// in this mode and finding zero violations across thousands of seeds is what
    /// proves the checker doesn't just flag everything -- it distinguishes a
    /// correct design from a buggy one.
    pub read_from_primary_only: bool,
}

impl Default for RunConfig {
    fn default() -> Self {
        RunConfig {
            n_nodes: 3,
            ticks: 2_000,
            net: NetConfig::default(),
            write_prob: 0.15,
            read_prob: 0.15,
            fault_prob: 0.03,
            read_from_primary_only: false,
        }
    }
}

pub struct RunResult {
    pub seed: u64,
    pub violation: Option<Violation>,
    pub ops: usize,
    pub sent: u64,
    pub dropped: u64,
}

/// Run one full simulation for `seed`. Deterministic: same seed + config => same
/// result, including any violation found.
pub fn run(seed: u64, cfg: &RunConfig) -> RunResult {
    let mut rng = Rng::new(seed);
    let mut sched: Scheduler<Deliver<Msg>> = Scheduler::new();
    let mut net = Network::new(cfg.n_nodes, cfg.net.clone());

    // Node 0 is primary; the rest are backups.
    let mut nodes: Vec<Node> = (0..cfg.n_nodes).map(|i| Node::new(i, i == 0)).collect();
    let backups: Vec<NodeId> = (1..cfg.n_nodes).collect();

    let mut history: Vec<Op> = Vec::new();
    let mut value_counter: u64 = 0;
    let mut op_seq: u64 = 0; // total order over CLIENT ops (issued sequentially)
    let key = "k"; // single key keeps the linearizability check tractable

    // Modeling choice: a single logical client issues operations sequentially
    // (at most one per driver iteration, in program order), so client ops are
    // totally ordered by `op_seq`. Concurrency lives in replication/faults, not
    // in the client -- which is what makes the monotonic-read check sound (a
    // later-seq read genuinely follows an earlier observation).

    for _tick in 0..cfg.ticks {
        // 1) Drain all network messages due at or before "now" by stepping the
        //    scheduler once per iteration (events carry their own delay).
        if let Some(ev) = sched.step() {
            let Deliver { from, to, msg } = ev.payload;
            if to < nodes.len() {
                let replies = nodes[to].handle(from, msg);
                for (dst, m) in replies {
                    net.send(&mut sched, &mut rng, to, dst, m);
                }
            }
        }

        // 2) Maybe issue a client write to the primary.
        if rng.chance(cfg.write_prob) {
            value_counter += 1;
            let v = value_counter;
            let logical = sched.now();
            let msgs = nodes[0].client_write(key, v, &backups);
            if !msgs.is_empty() {
                // The write was accepted by the primary -> record it.
                history.push(Op::Write { value: v, seq: op_seq, tick: logical });
                op_seq += 1;
                for (dst, m) in msgs {
                    net.send(&mut sched, &mut rng, 0, dst, m);
                }
            }
        }

        // 3) Maybe issue a client read. In the correct config, reads go to the
        //    primary (node 0); otherwise to a random node (which may be a stale
        //    backup -- the source of the bug).
        if rng.chance(cfg.read_prob) {
            let idx = if cfg.read_from_primary_only { 0 } else { rng.index(nodes.len()).unwrap_or(0) };
            if !nodes[idx].crashed {
                let observed = nodes[idx].read(key);
                history.push(Op::Read { value: observed, node: idx, seq: op_seq, tick: sched.now() });
                op_seq += 1;
            }
        }

        // 4) Maybe inject a fault.
        if rng.chance(cfg.fault_prob) {
            inject_fault(&mut rng, &mut net, &mut nodes, cfg.n_nodes);
        }
    }

    // Drain any remaining in-flight messages so final state settles.
    while let Some(ev) = sched.step() {
        let Deliver { from, to, msg } = ev.payload;
        if to < nodes.len() {
            let replies = nodes[to].handle(from, msg);
            for (dst, m) in replies {
                net.send(&mut sched, &mut rng, to, dst, m);
            }
        }
    }

    let violation = check_linearizability(&history);
    RunResult {
        seed,
        violation,
        ops: history.len(),
        sent: net.sent,
        dropped: net.dropped,
    }
}

fn inject_fault(rng: &mut Rng, net: &mut Network, nodes: &mut [Node], n: usize) {
    match rng.below(4) {
        0 => {
            // Partition a random node off from the rest.
            if let Some(victim) = rng.index(n) {
                net.partition = Partition::split(n, &[victim]);
            }
        }
        1 => net.partition.heal(),
        2 => {
            // Crash a random node.
            if let Some(v) = rng.index(n) {
                nodes[v].crash();
            }
        }
        _ => {
            // Restart a random node.
            if let Some(v) = rng.index(n) {
                nodes[v].restart();
            }
        }
    }
}
