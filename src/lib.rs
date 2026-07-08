//! faultline: a deterministic simulation tester for distributed systems.
//!
//! It runs a small distributed system (replicated nodes talking over a
//! simulated network) inside a fully deterministic world: one seed drives all
//! randomness and a logical-time scheduler drives all execution. The simulator
//! injects faults -- dropped/reordered messages, network partitions, node
//! crashes -- and checks system invariants after every step. Because the run is
//! a pure function of its seed, any invariant violation it finds is reproducible
//! *exactly* by replaying that seed. This is how systems like FoundationDB and
//! TigerBeetle test correctness under partial failure.

pub mod checker;
pub mod kv;
pub mod net;
pub mod runner;
pub mod sim;
