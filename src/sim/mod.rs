//! The deterministic simulation core: seeded RNG + logical-time scheduler.
//! Everything nondeterministic in the system under test must route through
//! these, so a seed fully determines a run.

pub mod rng;
pub mod scheduler;

pub use rng::Rng;
pub use scheduler::{Event, EventId, Scheduler, Tick};
