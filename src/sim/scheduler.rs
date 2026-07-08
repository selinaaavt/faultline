//! The deterministic event scheduler -- the simulation's clock and engine.
//!
//! There is no real time and no real concurrency. "Time" is a logical counter
//! (`Tick`), and the whole simulation is a priority queue of events ordered by
//! the tick they fire at. `step()` pops the earliest event; the simulation
//! advances by processing events one at a time. Because the queue order is fully
//! determined by (tick, sequence) and every scheduling choice comes from the
//! seeded [`crate::sim::rng::Rng`], a given seed replays the exact same run --
//! every message, every fault, every interleaving -- bit for bit.
//!
//! Ties (two events at the same tick) break by insertion sequence, so ordering
//! is total and deterministic rather than dependent on hashing or float compare.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// Logical time. Not milliseconds -- just a monotonic ordering of events.
pub type Tick = u64;

/// An opaque, ordered event id (also the tie-breaker for same-tick events).
pub type EventId = u64;

/// One scheduled event: fire `payload` at `tick`. `seq` makes ordering total.
#[derive(Debug, Clone)]
pub struct Event<E> {
    pub tick: Tick,
    pub seq: EventId,
    pub payload: E,
}

// Order so the BinaryHeap (a max-heap) yields the *earliest* event first:
// smaller tick first, then smaller seq. We invert in Ord to get min-heap behavior.
impl<E> PartialEq for Event<E> {
    fn eq(&self, other: &Self) -> bool {
        self.tick == other.tick && self.seq == other.seq
    }
}
impl<E> Eq for Event<E> {}
impl<E> PartialOrd for Event<E> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<E> Ord for Event<E> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse: earliest (tick, seq) should be "greatest" so it pops first.
        other
            .tick
            .cmp(&self.tick)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

pub struct Scheduler<E> {
    now: Tick,
    next_seq: EventId,
    queue: BinaryHeap<Event<E>>,
}

impl<E> Scheduler<E> {
    pub fn new() -> Self {
        Scheduler { now: 0, next_seq: 0, queue: BinaryHeap::new() }
    }

    /// Current logical time (the tick of the last event processed).
    pub fn now(&self) -> Tick {
        self.now
    }

    /// Schedule `payload` to fire `delay` ticks from now. Returns its event id.
    /// A delay of 0 fires at the current tick, but strictly after already-queued
    /// events at this tick (via the monotonic sequence number).
    pub fn schedule(&mut self, delay: Tick, payload: E) -> EventId {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.queue.push(Event { tick: self.now + delay, seq, payload });
        seq
    }

    /// Pop and return the next event (advancing logical time to its tick), or
    /// `None` if the simulation has quiesced (no more events).
    pub fn step(&mut self) -> Option<Event<E>> {
        let ev = self.queue.pop()?;
        debug_assert!(ev.tick >= self.now, "time must not go backwards");
        self.now = ev.tick;
        Some(ev)
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn pending(&self) -> usize {
        self.queue.len()
    }
}

impl<E> Default for Scheduler<E> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fires_in_time_order() {
        let mut s: Scheduler<&str> = Scheduler::new();
        s.schedule(5, "late");
        s.schedule(1, "early");
        s.schedule(3, "mid");
        let order: Vec<_> = std::iter::from_fn(|| s.step().map(|e| e.payload)).collect();
        assert_eq!(order, vec!["early", "mid", "late"]);
    }

    #[test]
    fn same_tick_breaks_by_insertion_order() {
        let mut s: Scheduler<u32> = Scheduler::new();
        s.schedule(2, 1);
        s.schedule(2, 2);
        s.schedule(2, 3);
        let order: Vec<_> = std::iter::from_fn(|| s.step().map(|e| e.payload)).collect();
        assert_eq!(order, vec![1, 2, 3], "ties must break deterministically by seq");
    }

    #[test]
    fn now_advances_monotonically() {
        let mut s: Scheduler<()> = Scheduler::new();
        s.schedule(10, ());
        s.schedule(3, ());
        let a = s.step().unwrap().tick;
        let b = s.step().unwrap().tick;
        assert!(a <= b);
        assert_eq!(s.now(), 10);
    }
}
