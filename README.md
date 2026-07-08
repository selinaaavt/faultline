# faultline

A deterministic simulation tester for distributed systems, in Rust. It runs a
replicated key-value store inside a fully simulated world - one seed drives all
randomness and all execution - injects faults (dropped/reordered messages,
network partitions, node crashes), checks consistency after every run, and when
it finds a violation it **reproduces it exactly from the seed** and **shrinks it
to the essential operations**.

This is the testing methodology behind FoundationDB and TigerBeetle, built from
scratch and small enough to read in one sitting.

## What it does, in one run

```
$ cargo run --release --bin faultline

searching seeds 0..5000 for a consistency violation
(system under test: primary-backup KV with local reads on backups)

FOUND a violation at seed 0
  VIOLATION: stale_read
  op #8: node 2 read value 4, but value 5 was already observed by op #6
          (read went backwards in time)

shrank failing history: 411 ops -> 2 essential ops
minimal failing trace:
    #404 WRITE 288
    #406 READ=286 (node 1)

reproducibility check: seed 0 re-run twice -> IDENTICAL violation (deterministic!)
```

It found a real linearizability bug in a plausible replication design, cut a
411-operation run down to the 2 operations that matter, and proved the failure
replays identically.

## Why this is the interesting way to test distributed systems

Distributed bugs hide in timing: a message arrives late, a node crashes at the
wrong instant, a partition heals mid-write. With real threads and real clocks,
such a bug shows up once in a million runs and never reproduces. faultline
removes the nondeterminism:

- **One seed = one exact run.** All randomness comes from a single seeded PRNG;
  all execution is a logical-time event queue. No wall clock, no OS threads, no
  entropy anywhere in the system under test.
- So a bug found at seed 0 is a bug you can replay at seed 0, forever - on any
  machine, in a debugger, as a regression test.
- And because the run is just data, the failing history can be **shrunk**
  automatically to the minimal set of operations that still fails.

## It distinguishes correct from buggy - not just "flags everything"

The system under test is primary-backup replication where **reads are served by
any node**. That's a real design with a real flaw: a backup can serve a stale
value after a newer one was already observed elsewhere. faultline finds it at
seed 0.

Flip one thing - serve reads from the primary only (`--correct`) - and the
design becomes linearizable. faultline then finds **zero violations across 5000
seeds**:

```
$ cargo run --release --bin faultline -- --correct
no violation found in 5000 seeds (system held up)
```

That contrast is the point: the checker passes correct designs and fails buggy
ones. A tool that flagged everything would be worthless.

## How it works

```
src/
  sim/
    rng.rs         the single seeded PRNG (SplitMix64) - all randomness
    scheduler.rs   logical-time event queue - all execution ordering
  net.rs           simulated network: drop / delay / reorder / partition
  kv.rs            system under test: primary-backup replicated KV
  runner.rs        drives clients + faults, records an operation history
  checker.rs       monotonic-read linearizability check over the history
  shrink.rs        delta-debugging: minimize a failing history
  bin/faultline.rs search seeds, reproduce, shrink
tests/             21 tests: determinism, bug-found, correct-passes, shrinking
```

Key design decisions:

- **Total order over client ops.** A single logical client issues operations
  sequentially, so client ops are totally ordered while concurrency lives in
  replication and faults. That's what makes the monotonic-read check sound - a
  flagged read genuinely follows the observation it contradicts, never merely
  shares a logical tick with it.
- **The checker is a necessary condition, not full linearizability.** Full
  linearizability checking is NP-hard; faultline checks that reads never go
  backwards in time, which any linearizable register must satisfy and which is
  cheap to verify. It catches the class of bug this system exhibits without an
  exponential search.

## Try it

```bash
cargo build --release
cargo run --release --bin faultline            # find + shrink + reproduce a bug
cargo run --release --bin faultline -- --correct   # correct design: no bugs
cargo run --release --bin faultline -- 0        # replay a specific seed
cargo test --release                            # 21 tests
```

## Honest scope

- The system under test is intentionally small (one key, primary-backup) so the
  checker stays sound and fast. The framework (seeded scheduler, fault network,
  history + checker + shrinker) is the reusable part; a larger protocol (Raft, a
  quorum store) would plug into the same harness.
- The consistency check is a strong necessary condition, not a full
  linearizability oracle - a deliberate trade of completeness for soundness and
  speed, and enough to catch real violations.
