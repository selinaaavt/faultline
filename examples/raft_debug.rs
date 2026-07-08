// Instrument seed 181: print each node's term, commit_index, and committed log
// at the moment the violation is detected, to tell a real Raft bug from a
// checker bug.
use faultline::net::{Deliver, Network, Partition};
use faultline::raft::{RaftMsg, RaftNode, Role};
use faultline::raft_runner::RaftConfig;
use faultline::sim::{Rng, Scheduler};

fn main() {
    let cfg = RaftConfig::default();
    let seed = 181u64;
    let mut rng = Rng::new(seed);
    let mut sched: Scheduler<Deliver<RaftMsg>> = Scheduler::new();
    let mut net = Network::new(cfg.n_nodes, cfg.net.clone());
    let mut nodes: Vec<RaftNode> = (0..cfg.n_nodes).map(|i| RaftNode::new(i, cfg.n_nodes)).collect();
    let mut cmd = 0u64;

    for t in 0..cfg.ticks {
        if let Some(ev) = sched.step() {
            let Deliver { from, to, msg } = ev.payload;
            if to < nodes.len() {
                let replies = nodes[to].handle(from, msg);
                for (d, m) in replies { net.send(&mut sched, &mut rng, to, d, m); }
            }
        }
        for id in 0..cfg.n_nodes {
            if nodes[id].crashed { continue; }
            if nodes[id].role != Role::Leader && rng.chance(cfg.election_prob) {
                let m = nodes[id].start_election();
                for (d, mm) in m { net.send(&mut sched, &mut rng, id, d, mm); }
            }
            if nodes[id].role == Role::Leader && rng.chance(cfg.heartbeat_prob) {
                let m = nodes[id].heartbeat();
                for (d, mm) in m { net.send(&mut sched, &mut rng, id, d, mm); }
            }
        }
        if rng.chance(cfg.client_prob) {
            if let Some(id) = rng.index(cfg.n_nodes) {
                if nodes[id].role == Role::Leader && !nodes[id].crashed {
                    cmd += 1; nodes[id].client_command(cmd);
                }
            }
        }
        if rng.chance(cfg.fault_prob) {
            match rng.below(4) {
                0 => if let Some(v)=rng.index(cfg.n_nodes){ net.partition = Partition::split(cfg.n_nodes,&[v]); },
                1 => net.partition.heal(),
                2 => if let Some(v)=rng.index(cfg.n_nodes){ nodes[v].crash(); },
                _ => if let Some(v)=rng.index(cfg.n_nodes){ nodes[v].restart(); },
            }
        }
        // Detect the index-2 disagreement and dump state.
        let mut seen: Option<(u64,u64)> = None;
        for n in &nodes {
            let c = n.committed_log();
            if c.len() >= 2 {
                let e = (c[1].term, c[1].command);
                match seen {
                    None => seen = Some(e),
                    Some(s) if s != e => {
                        println!("tick {t}: index-2 disagreement {s:?} vs node{} {e:?}", n.id);
                        for m in &nodes {
                            println!("  node{} role={:?} term={} commit={} log={:?}",
                                m.id, m.role, m.current_term, m.commit_index,
                                m.log.iter().map(|x|(x.term,x.command)).collect::<Vec<_>>());
                        }
                        return;
                    }
                    _ => {}
                }
            }
        }
    }
    println!("no disagreement reproduced in trace");
}
