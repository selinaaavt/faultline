//! The simulated network -- where faults are injected.
//!
//! Nodes never talk directly; every message goes through the network, which
//! decides its fate using the seeded RNG and the current fault configuration:
//!   - **drop**: the message is silently lost (with probability `drop_prob`).
//!   - **delay**: delivery is scheduled some random number of ticks later, so
//!     messages can arrive out of order -- the reordering real networks do.
//!   - **partition**: if the two nodes are on opposite sides of a partition, the
//!     message is dropped regardless (a network split).
//!
//! Delivery is modeled by scheduling a `Deliver` event on the shared scheduler,
//! so message arrival interleaves deterministically with everything else. The
//! network holds no real sockets -- it's pure logic over the seeded world.

use crate::sim::{Rng, Scheduler, Tick};

/// A node identifier (0..n).
pub type NodeId = usize;

/// The event type the scheduler carries for network activity: deliver `msg`
/// from `from` to `to`. The payload `M` is the application's message type.
#[derive(Debug, Clone)]
pub struct Deliver<M> {
    pub from: NodeId,
    pub to: NodeId,
    pub msg: M,
}

/// Tunable fault model. All probabilities in [0, 1]; delays in ticks.
#[derive(Clone)]
pub struct NetConfig {
    pub drop_prob: f64,
    pub min_delay: Tick,
    pub max_delay: Tick,
}

impl Default for NetConfig {
    fn default() -> Self {
        // A lively-but-not-hostile default: some loss, variable latency.
        NetConfig { drop_prob: 0.05, min_delay: 1, max_delay: 20 }
    }
}

/// Tracks a network partition as a 2-coloring of nodes: nodes with different
/// colors cannot exchange messages. `None` = fully connected.
#[derive(Clone, Default)]
pub struct Partition {
    // side[node] = which side of the split a node is on. Empty = no partition.
    side: Vec<u8>,
}

impl Partition {
    pub fn none(n: usize) -> Self {
        Partition { side: vec![0; n] }
    }

    /// Split so that nodes in `group_a` are isolated from the rest.
    pub fn split(n: usize, group_a: &[NodeId]) -> Self {
        let mut side = vec![0u8; n];
        for &node in group_a {
            if node < n {
                side[node] = 1;
            }
        }
        Partition { side }
    }

    /// Heal: everyone back on the same side.
    pub fn heal(&mut self) {
        for s in self.side.iter_mut() {
            *s = 0;
        }
    }

    /// Can `a` and `b` currently exchange messages?
    pub fn connected(&self, a: NodeId, b: NodeId) -> bool {
        match (self.side.get(a), self.side.get(b)) {
            (Some(x), Some(y)) => x == y,
            _ => true,
        }
    }
}

pub struct Network {
    pub config: NetConfig,
    pub partition: Partition,
    // Stats for the run summary.
    pub sent: u64,
    pub dropped: u64,
    pub delivered: u64,
}

impl Network {
    pub fn new(n_nodes: usize, config: NetConfig) -> Self {
        Network {
            config,
            partition: Partition::none(n_nodes),
            sent: 0,
            dropped: 0,
            delivered: 0,
        }
    }

    /// Attempt to send `msg` from `from` to `to`. Decides drop/delay using
    /// `rng`, and on success schedules a `Deliver` event on `sched`. Returns
    /// true if the message was accepted for (eventual) delivery.
    pub fn send<M>(
        &mut self,
        sched: &mut Scheduler<Deliver<M>>,
        rng: &mut Rng,
        from: NodeId,
        to: NodeId,
        msg: M,
    ) -> bool {
        self.sent += 1;

        // Partitioned peers cannot communicate.
        if !self.partition.connected(from, to) {
            self.dropped += 1;
            return false;
        }
        // Random loss.
        if rng.chance(self.config.drop_prob) {
            self.dropped += 1;
            return false;
        }
        // Random delay -> out-of-order arrival is possible and deterministic.
        let span = self.config.max_delay.saturating_sub(self.config.min_delay) + 1;
        let delay = self.config.min_delay + rng.below(span);
        sched.schedule(delay, Deliver { from, to, msg });
        self.delivered += 1;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_blocks_cross_side() {
        let p = Partition::split(4, &[0, 1]); // {0,1} | {2,3}
        assert!(p.connected(0, 1));
        assert!(p.connected(2, 3));
        assert!(!p.connected(0, 2));
        assert!(!p.connected(1, 3));
    }

    #[test]
    fn heal_reconnects() {
        let mut p = Partition::split(3, &[0]);
        assert!(!p.connected(0, 1));
        p.heal();
        assert!(p.connected(0, 1));
    }

    #[test]
    fn zero_drop_always_delivers_when_connected() {
        let cfg = NetConfig { drop_prob: 0.0, min_delay: 1, max_delay: 1 };
        let mut net = Network::new(2, cfg);
        let mut sched: Scheduler<Deliver<u32>> = Scheduler::new();
        let mut rng = Rng::new(7);
        for _ in 0..100 {
            assert!(net.send(&mut sched, &mut rng, 0, 1, 42));
        }
        assert_eq!(net.dropped, 0);
        assert_eq!(net.delivered, 100);
    }

    #[test]
    fn deterministic_drop_pattern() {
        // Same seed -> identical drop decisions.
        let run = || {
            let cfg = NetConfig { drop_prob: 0.5, min_delay: 1, max_delay: 5 };
            let mut net = Network::new(2, cfg);
            let mut sched: Scheduler<Deliver<u32>> = Scheduler::new();
            let mut rng = Rng::new(99);
            for _ in 0..200 {
                net.send(&mut sched, &mut rng, 0, 1, 1);
            }
            (net.dropped, net.delivered)
        };
        assert_eq!(run(), run(), "same seed must reproduce the same drops");
    }
}
