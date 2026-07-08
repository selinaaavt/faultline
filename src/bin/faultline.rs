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

use faultline::runner::{run, RunConfig};

fn main() {
    let args: Vec<String> = std::env::args().collect();

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
