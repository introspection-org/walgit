//! Cumulative repository cost across a revision history.
//!
//! The single-version comparisons answer "what does this push cost". The question
//! an operator actually has is "what does this repository cost after N pushes".
//! walgit stores every revision whole; Xet stores each distinct chunk once across
//! the entire history.

use std::collections::HashSet;

use bytes::Bytes;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use xet_core_structures::xorb_object::constants::TARGET_CHUNK_SIZE;
use xet_data::deduplication::Chunker;

fn chunks(data: &[u8], target: usize) -> Vec<([u8; 32], usize)> {
    let b = Bytes::copy_from_slice(data);
    let mut ch = Chunker::new(target);
    let mut out: Vec<([u8; 32], usize)> = ch
        .next_block_bytes(&b, true)
        .into_iter()
        .map(|c| {
            let mut k = [0u8; 32];
            k.copy_from_slice(c.hash.as_bytes());
            (k, c.data.len())
        })
        .collect();
    if let Some(c) = ch.finish() {
        let mut k = [0u8; 32];
        k.copy_from_slice(c.hash.as_bytes());
        out.push((k, c.data.len()));
    }
    out
}

fn csv_row(r: &mut ChaCha8Rng, id: u64) -> String {
    format!(
        "{id},{},{:.4},{},class_{}\n",
        r.gen_range(1000..99999),
        r.gen::<f32>(),
        if r.gen_bool(0.5) { "true" } else { "false" },
        id % 23
    )
}

/// A dataset that grows by appending rows, with a fraction of existing rows edited
/// in place each revision.
fn revision_series(rows0: usize, revs: usize, grow_pct: usize, edit_pct: usize) -> Vec<Vec<u8>> {
    let mut r = ChaCha8Rng::seed_from_u64(2024);
    let mut rows: Vec<String> = (0..rows0 as u64).map(|i| csv_row(&mut r, i)).collect();
    let mut next_id = rows0 as u64;
    let mut out = Vec::with_capacity(revs);

    for rev in 0..revs {
        if rev > 0 {
            let add = rows.len() * grow_pct / 100;
            for _ in 0..add {
                let row = csv_row(&mut r, next_id);
                next_id += 1;
                rows.push(row);
            }
            let edits = rows.len() * edit_pct / 100;
            for _ in 0..edits {
                let at = r.gen_range(0..rows.len());
                let id: u64 = rows[at].split(',').next().and_then(|s| s.parse().ok()).unwrap_or(0);
                rows[at] = csv_row(&mut r, id);
            }
        }
        out.push(rows.concat().into_bytes());
    }
    out
}

fn report(label: &str, series: &[Vec<u8>], target: usize) {
    let mut seen: HashSet<[u8; 32]> = HashSet::new();
    let mut whole = 0usize;
    let mut xet = 0usize;
    println!("### {label}");
    println!("  {:>4} {:>10} {:>12} {:>12} {:>8}", "rev", "size", "walgit cum", "xet cum", "ratio");
    for (i, v) in series.iter().enumerate() {
        whole += v.len();
        for (h, len) in chunks(v, target) {
            if seen.insert(h) {
                xet += len;
            }
        }
        println!(
            "  {:>4} {:>9.1}M {:>11.1}M {:>11.1}M {:>7.1}x",
            i,
            v.len() as f64 / 1048576.0,
            whole as f64 / 1048576.0,
            xet as f64 / 1048576.0,
            whole as f64 / xet.max(1) as f64
        );
    }
    println!();
}

/// An ATIF trajectory (harbor-framework RFC 0001): one JSON root object with a
/// `steps` array, rewritten wholesale after every agent turn. git-lfs therefore
/// stores a complete new object per turn.
fn atif_series(turns: usize, step_bytes: usize) -> Vec<Vec<u8>> {
    let mut r = ChaCha8Rng::seed_from_u64(31337);
    let mut steps: Vec<String> = Vec::with_capacity(turns);
    let mut out = Vec::with_capacity(turns);

    for turn in 1..=turns {
        let reasoning: String = (0..step_bytes / 8)
            .map(|_| char::from(b'a' + r.gen_range(0..26u8)))
            .collect();
        steps.push(format!(
            "    {{\"step_id\":{turn},\"timestamp\":\"2026-09-10T00:{:02}:{:02}Z\",\"source\":\"agent\",\
\"message\":\"{}\",\"reasoning_content\":\"{reasoning}\",\
\"tool_calls\":[{{\"name\":\"bash\",\"arguments\":{{\"command\":\"{}\"}}}}],\
\"metrics\":{{\"input_tokens\":{},\"output_tokens\":{}}}}}",
            turn / 60, turn % 60,
            "step output ".repeat(step_bytes / 96),
            r.gen_range(1000..9999),
            r.gen_range(100..900),
            r.gen_range(50..500),
        ));
        // whole-file rewrite: root metadata, every step so far, then final_metrics
        let doc = format!(
            "{{\n  \"schema_version\":\"1.8\",\n  \"session_id\":\"sess-abc\",\n  \
\"trajectory_id\":\"traj-001\",\n  \"agent\":{{\"name\":\"operator\",\"model_name\":\"m\"}},\n  \
\"steps\":[\n{}\n  ],\n  \"final_metrics\":{{\"steps\":{},\"total_tokens\":{}}}\n}}\n",
            steps.join(",\n"),
            turn,
            turn * 1234
        );
        out.push(doc.into_bytes());
    }
    out
}

fn main() {
    let target = *TARGET_CHUNK_SIZE;
    println!("Cumulative repository cost over a revision history (chunk target {} KiB)\n", target / 1024);

    // ~64 MiB of CSV to start; 12 revisions.
    let rows0 = 900_000;
    report("CSV, append 5%/rev, no edits", &revision_series(rows0, 12, 5, 0), target);
    report("CSV, append 5%/rev, 1% rows edited", &revision_series(rows0, 12, 5, 1), target);
    report("CSV, append-only 1%/rev", &revision_series(rows0, 12, 1, 0), target);

    // ATIF trajectory rewritten in full every turn, at three session scales.
    // The dedup unit is a 64 KiB chunk, so the benefit tracks file size / chunk size.
    for (turns, step) in [(120usize, 8192usize), (300, 32768), (600, 65536)] {
        let traj = atif_series(turns, step);
        let last = traj.last().map(|v| v.len()).unwrap_or(0);
        let chunks_in_final = last / 65536;
        let mut seen: std::collections::HashSet<[u8; 32]> = std::collections::HashSet::new();
        let (mut whole, mut xet) = (0usize, 0usize);
        for v in &traj {
            whole += v.len();
            for (h, len) in chunks(v, target) {
                if seen.insert(h) { xet += len; }
            }
        }
        println!("ATIF {turns} turns, final {:.1} MiB (~{chunks_in_final} chunks): \
walgit {:.0} MiB, xet {:.0} MiB, {:.1}x",
                 last as f64 / 1048576.0, whole as f64 / 1048576.0,
                 xet as f64 / 1048576.0, whole as f64 / xet.max(1) as f64);
    }
}
