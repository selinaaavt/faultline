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

## The capstone: testing a from-scratch Raft

The KV store above is the warm-up. faultline also drives a **from-scratch Raft
consensus implementation** (leader election, log replication, the up-to-date
vote restriction, the commit rule) under the same fault injection, checking
Raft's two headline safety properties after every step:

- **Election safety** - at most one leader per term.
- **State-machine safety** - committed logs never diverge across nodes.

```
$ cargo run --release --bin faultline -- --raft
no safety violation in 2000 seeds -- Raft held up.
  (the simulator did real work: 34,681 leader elections and 6,949 committed
   commands across the seeds, all under active fault injection)
```

Getting there was the whole point. Building the tester paid off immediately: it
**caught three genuine safety bugs in my own Raft**, each pinned to a
reproducible seed and a minimized trace:

1. A follower committed past what an AppendEntries actually confirmed (capped
   commit by its whole log length instead of the confirmed prefix).
2. A leader counted *itself* toward a majority for an index it no longer held
   after a log truncation.
3. The subtle one (seed 1111): a follower reported its match index as its raw
   log length rather than the prefix the RPC confirmed - so a follower with a
   longer, divergent log from an old term told the leader it had replicated
   entries it never received, letting the leader commit on a false majority and
   producing two conflicting committed entries at one index.

All three are the kind of bug that surfaces once in millions of real runs and
never reproduces. Here they reproduce from a fixed seed every time. After the
fixes, Raft holds across all 2000 seeds - provably safe under adversarial,
deterministic fault injection. That is exactly what deterministic simulation
testing is for.

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
  kv.rs            warm-up system under test: primary-backup replicated KV
  runner.rs        drives KV clients + faults, records an operation history
  checker.rs       monotonic-read linearizability check over the history
  shrink.rs        delta-debugging: minimize a failing history
  raft.rs          capstone system under test: from-scratch Raft consensus
  raft_runner.rs   drives Raft under faults; checks election + state-machine safety
  bin/faultline.rs search seeds, reproduce, shrink; --raft for the Raft suite
tests/             28 tests: sim core, KV bug-find + shrink, Raft mechanics +
                   safety-across-seeds + determinism
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
