//! The single source of randomness for the entire simulation.
//!
//! Determinism is the whole point of this project: a run is fully defined by its
//! seed. That only holds if *every* nondeterministic choice -- which message is
//! delivered next, whether a packet drops, when a node crashes -- is drawn from
//! one seeded generator and nothing else. No `rand::thread_rng`, no wall clock,
//! no OS entropy anywhere in the system under test or the simulator.
//!
//! SplitMix64: tiny, fast, well-distributed, and trivially reproducible.

pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform integer in `[0, n)`. `n == 0` returns 0.
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next_u64() % n }
    }

    /// True with probability `p` (clamped to [0, 1]).
    pub fn chance(&mut self, p: f64) -> bool {
        let p = p.clamp(0.0, 1.0);
        // 2^53 gives full f64 mantissa precision for the comparison.
        ((self.next_u64() >> 11) as f64 / (1u64 << 53) as f64) < p
    }

    /// Pick a uniform index in `[0, len)`, or `None` if `len == 0`.
    pub fn index(&mut self, len: usize) -> Option<usize> {
        if len == 0 { None } else { Some((self.next_u64() % len as u64) as usize) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_sequence() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seed_diverges() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(2);
        // Extremely unlikely to match on the first draw.
        assert_ne!(a.next_u64(), b.next_u64());
    }
}
