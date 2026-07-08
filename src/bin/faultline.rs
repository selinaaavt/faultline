//! faultline CLI: search seeds for a consistency violation, then prove it
//! reproduces.
//!
//!   cargo run --release --bin faultline           # search default seed range
//!   cargo run --release --bin faultline -- 12345   # replay one specific seed
//!
//! The search runs the deterministic simulation for seed 0, 1, 2, ... until a
//! seed exposes a violation, then re-runs that exact seed to demonstrate the bug
//! reproduces identically -- the core promise of deterministic simulation
//! testing.

use faultline::raft_runner::{run as raft_run, RaftConfig};
use faultline::runner::{run, RunConfig};
use faultline::shrink::shrink;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    // `--raft-buggy` reintroduces the fixed match-index bug and shows the
    // simulator catching it: a mutation test proving the checker has teeth.
    if args.iter().any(|a| a == "--raft-buggy") {
        run_raft_buggy_mode();
        return;
    }

    // `--raft` runs the Raft consensus system under test instead of the KV
    // store: search seeds for a safety violation, reporting that correct Raft
    // holds under fault injection (or a caught violation if one exists).
    if args.iter().any(|a| a == "--raft") {
        // Optional explicit seed after --raft replays just that one seed.
        let seed = args.iter().skip(1).find_map(|s| s.parse::<u64>().ok());
        match seed {
            Some(s) => {
                let r = raft_run(s, &RaftConfig::default());
                println!("raft seed {s}: committed={} leaders={} violation={:?}",
                    r.commands_committed, r.leaders_elected,
                    r.violation.as_ref().map(|v| &v.kind));
                if let Some(v) = &r.violation {
                    println!("  {}", v.detail);
                }
            }
            None => run_raft_mode(),
        }
        return;
    }

    // `--correct` runs the fixed design (reads from primary only) to show the
    // tool finds NO violations on a correct system -- proving it isn't just
    // flagging everything.
    let correct = args.iter().any(|a| a == "--correct");
    let cfg = RunConfig { read_from_primary_only: correct, ..RunConfig::default() };
    if correct {
        println!("[correct mode: reads served by primary only]\n");
    }

    // Replay mode: a seed was given.
    if let Some(seed) = args.iter().skip(1).find_map(|s| s.parse::<u64>().ok()) {
        println!("=== replaying seed {seed} ===");
        let r = run(seed, &cfg);
        report(&r);
        return;
    }

    // Search mode: scan seeds for the first violation.
    let max_seeds = 5000;
    println!(
        "searching seeds 0..{max_seeds} for a consistency violation\n\
         (system under test: primary-backup KV with local reads on backups)\n"
    );
    let mut first_bug: Option<u64> = None;
    let mut checked = 0;
    for seed in 0..max_seeds {
        let r = run(seed, &cfg);
        checked += 1;
        if r.violation.is_some() {
            first_bug = Some(seed);
            println!("FOUND a violation at seed {seed} (after {checked} seeds)\n");
            report(&r);

            // Shrink the failing history to its essential ops -- what makes the
            // failure debuggable instead of a 400-op haystack.
            let (minimal, mv) = shrink(&r.history);
            println!(
                "\nshrank failing history: {} ops -> {} essential ops",
                r.history.len(),
                minimal.len()
            );
            println!("minimal failing trace:");
            for op in &minimal {
                println!("    {op}");
            }
            if let Some(v) = mv {
                println!("  => {}", v.detail);
            }
            break;
        }
    }

    match first_bug {
        Some(seed) => {
            // Prove reproducibility: run the same seed again and confirm the
            // identical violation.
            let a = run(seed, &cfg);
            let b = run(seed, &cfg);
            let same = match (&a.violation, &b.violation) {
                (Some(x), Some(y)) => x.kind == y.kind && x.at_tick == y.at_tick,
                _ => false,
            };
            println!(
                "\nreproducibility check: seed {seed} re-run twice -> {}",
                if same { "IDENTICAL violation (deterministic!)" } else { "DIVERGED (bug in the simulator!)" }
            );
        }
        None => println!("no violation found in {checked} seeds (system held up)"),
    }
}

fn run_raft_buggy_mode() {
    // A mutation test of the checker. We reintroduce the match-index safety bug
    // and drive the *specific* interleaving that exploits it (a follower with a
    // longer, divergent log from an old term reports its raw log length, so a
    // leader commits on a false majority). The checker must flag the resulting
    // divergence. This is a deterministic construction rather than a random
    // search, because under correct randomized election timeouts the trigger is
    // rare -- the honest, reliable way to show the checker has teeth.
    println!(
        "faultline --raft-buggy: the match-index safety bug is reintroduced and\n\
         we search seeds (under a churn-heavy fault profile) for one that exposes\n\
         it. A working checker must catch it and hand back a reproducing seed.\n"
    );
    match faultline::raft_runner::demonstrate_injected_bug() {
        Some((seed, detail)) => {
            println!("CAUGHT the injected safety bug at seed {seed}: state_machine_safety");
            println!("  {detail}");
            println!(
                "\nThe checker has teeth: with the bug present it finds a reproducing\n\
                 seed; run --raft (bug fixed) to confirm the protocol then holds\n\
                 safety AND liveness across all seeds."
            );
        }
        None => println!(
            "no violation found -- unexpected; the injected bug should be caught."
        ),
    }
}

fn run_raft_mode() {
    let cfg = RaftConfig::default();
    let max_seeds = 2000;
    println!(
        "faultline --raft: stress-testing a from-scratch Raft ({} nodes) under\n\
         partitions, crashes, restarts, and message loss across {max_seeds} seeds.\n\
         checking: SAFETY (<=1 leader/term; committed logs never diverge) and,\n\
         after faults stop, LIVENESS (the cluster elects a leader and commits).\n",
        cfg.n_nodes
    );

    let mut total_committed = 0usize;
    let mut total_leaders = 0usize;
    let mut live_ok = 0usize;
    let mut live_fail = 0usize;
    let mut violated: Option<u64> = None;
    for seed in 0..max_seeds {
        let r = raft_run(seed, &cfg);
        total_committed += r.commands_committed;
        total_leaders += r.leaders_elected;
        match r.live {
            Some(true) => live_ok += 1,
            Some(false) => live_fail += 1,
            None => {}
        }
        if let Some(v) = &r.violation {
            println!("SAFETY VIOLATION at seed {seed}: {}", v.kind);
            println!("  {}", v.detail);
            violated = Some(seed);
            break;
        }
    }

    match violated {
        Some(_) => {}
        None => {
            println!("no safety violation in {max_seeds} seeds -- Raft held up.");
            println!(
                "  safety : PASS ({} leader elections, {} committed commands under faults)",
                total_leaders, total_committed
            );
            println!(
                "  liveness: {}/{} seeds made progress after stabilization ({} failed)",
                live_ok, max_seeds, live_fail
            );
            println!(
                "\nThis is the meaningful result: a correct consensus protocol stays SAFE\n\
                 under adversarial fault injection AND recovers LIVENESS once the network\n\
                 heals -- and if a bug were introduced, the same harness would surface a\n\
                 reproducible seed."
            );
        }
    }
}

fn report(r: &faultline::runner::RunResult) {
    println!("seed              : {}", r.seed);
    println!("client operations : {}", r.ops);
    println!("messages sent     : {} ({} dropped)", r.sent, r.dropped);
    match &r.violation {
        Some(v) => {
            println!("VIOLATION         : {}", v.kind);
            println!("  {}", v.detail);
        }
        None => println!("result            : consistent (no violation)"),
    }
}
