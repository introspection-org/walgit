//! End-to-end comparison of the two LFS storage paths over one `ObjectStore`.
//!
//! Whole-object is what `lfs.rs` does today: one sha256-addressed object per
//! version. Xet is `lfs_xet::Xet::store_object`. Both run against the same in-memory
//! store over the same JSONL trajectory revisions, so the difference is the
//! storage path alone, not the backend.
//!
//! Measures, per size tier and path: write and read wall time and throughput,
//! CPU time, peak RSS, and bytes actually held by the store.
//!
//! Run: `cargo bench -p walgit-server --features lfs-xet --bench lfs_xet`

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use futures::StreamExt;
use sha2::{Digest, Sha256};
use walgit_proto::keys;
use walgit_server::lfs_xet;
use walgit_store::memory::MemoryStore;
use walgit_store::{DynStore, ObjectStoreExt, PutBody, PutMode};

struct Xorshift(u64);
impl Xorshift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

const WORDS: [&str; 16] = [
    "the", "model", "returns", "a", "tool", "call", "with", "arguments", "then", "observes",
    "output", "and", "continues", "reasoning", "until", "done",
];

/// One JSONL trajectory line, roughly `bytes` long.
fn line(r: &mut Xorshift, step: usize, bytes: usize) -> String {
    let mut s = String::with_capacity(bytes + 96);
    let _ = write!(s, "{{\"step_id\":{step},\"source\":\"agent\",\"content\":\"");
    while s.len() < bytes {
        s.push_str(WORDS[(r.next() % 16) as usize]);
        s.push(' ');
    }
    let _ = write!(s, "\",\"tokens\":{}}}\n", r.next() % 900 + 100);
    s
}

/// A trajectory that grows by appending `per_rev` lines each revision.
fn revisions(seed: u64, revs: usize, per_rev: usize, line_bytes: usize) -> Vec<Bytes> {
    let mut r = Xorshift(seed);
    let mut doc = String::new();
    let mut out = Vec::with_capacity(revs);
    let mut step = 0;
    for _ in 0..revs {
        for _ in 0..per_rev {
            doc.push_str(&line(&mut r, step, line_bytes));
            step += 1;
        }
        out.push(Bytes::from(doc.clone()));
    }
    out
}

fn cpu_secs() -> f64 {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let Some(i) = s.rfind(')') else { return 0.0 };
    let f: Vec<&str> = s[i + 1..].split_whitespace().collect();
    let u: f64 = f.get(11).and_then(|v| v.parse().ok()).unwrap_or(0.0);
    let k: f64 = f.get(12).and_then(|v| v.parse().ok()).unwrap_or(0.0);
    (u + k) / 100.0
}

fn peak_rss_mib() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with("VmHWM:"))
        .and_then(|l| l.split_whitespace().nth(1)?.parse::<f64>().ok())
        .map(|kb| kb / 1024.0)
        .unwrap_or(0.0)
}

async fn stored_bytes(store: &DynStore) -> u64 {
    let mut total = 0;
    let mut s = store.list("", None);
    while let Some(m) = s.next().await {
        if let Ok(m) = m {
            total += m.size;
        }
    }
    total
}

#[derive(Default)]
struct Run {
    write_secs: f64,
    read_secs: f64,
    cpu_secs: f64,
    logical: u64,
    stored: u64,
    rss_mib: f64,
}

async fn run_whole(series: &[Bytes]) -> Run {
    let store: DynStore = Arc::new(MemoryStore::new());
    let mut run = Run::default();
    let mut keys: Vec<String> = Vec::with_capacity(series.len());

    let c0 = cpu_secs();
    let t = Instant::now();
    for b in series {
        run.logical += b.len() as u64;
        // put_object hashes the whole body to verify the oid; that is the cost.
        let k = keys::lfs_key(&hex::encode(Sha256::digest(b)));
        store
            .put(&k, PutBody::Bytes(b.clone()), PutMode::Overwrite.into())
            .await
            .expect("put");
        keys.push(k);
    }
    run.write_secs = t.elapsed().as_secs_f64();

    let t = Instant::now();
    for (b, k) in series.iter().zip(&keys) {
        let got = store.get_bytes(k).await.expect("get").expect("present");
        assert_eq!(got.1, *b);
    }
    run.read_secs = t.elapsed().as_secs_f64();
    run.cpu_secs = cpu_secs() - c0;
    run.stored = stored_bytes(&store).await;
    run.rss_mib = peak_rss_mib();
    run
}

async fn run_xet(series: &[Bytes]) -> Run {
    let store: DynStore = Arc::new(MemoryStore::new());
    let xet = lfs_xet::Xet::new(Arc::clone(&store));
    let mut run = Run::default();
    let mut hashes = Vec::with_capacity(series.len());

    let c0 = cpu_secs();
    let t = Instant::now();
    for b in series {
        run.logical += b.len() as u64;
        // The LFS oid is sha256 by protocol, so this path pays it too.
        std::hint::black_box(Sha256::digest(b));
        let stats = xet.store_object(b.clone()).await.expect("store");
        hashes.push(stats.file_hash);
    }
    run.write_secs = t.elapsed().as_secs_f64();

    let t = Instant::now();
    for (b, h) in series.iter().zip(&hashes) {
        let got = xet.load_object(h).await.expect("load").expect("present");
        assert_eq!(got, *b);
    }
    run.read_secs = t.elapsed().as_secs_f64();
    run.cpu_secs = cpu_secs() - c0;
    run.stored = stored_bytes(&store).await;
    run.rss_mib = peak_rss_mib();
    run
}

fn mib(b: u64) -> f64 {
    b as f64 / 1048576.0
}

fn main() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let revs = 12;

    println!("whole-object LFS vs Xet, end to end over MemoryStore, JSONL trajectories, {revs} revisions each\n");
    println!(
        "{:<8} {:<6} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>8} {:>8} {:>7}",
        "tier", "path", "final", "logical", "stored", "ratio", "write", "read", "cpu ms", "rss MiB", "w MiB/s"
    );

    // (name, lines per revision, bytes per line) -> final file of roughly the named size.
    for (tier, per_rev, line_bytes) in [("small", 40, 512), ("medium", 200, 1024), ("large", 400, 2048)] {
        let series = revisions(7, revs, per_rev, line_bytes);
        let final_size = series.last().map(|b| b.len()).unwrap_or(0) as u64;

        let iters = 5;
        let avg = |runs: Vec<Run>| {
            let n = runs.len() as f64;
            let mut a = Run::default();
            for r in &runs {
                a.write_secs += r.write_secs / n;
                a.read_secs += r.read_secs / n;
                a.cpu_secs += r.cpu_secs / n;
                a.rss_mib = a.rss_mib.max(r.rss_mib);
            }
            a.logical = runs[0].logical;
            a.stored = runs[0].stored;
            a
        };
        let whole = avg((0..iters).map(|_| rt.block_on(run_whole(&series))).collect());
        let xet = avg((0..iters).map(|_| rt.block_on(run_xet(&series))).collect());

        for (name, r) in [("whole", &whole), ("xet", &xet)] {
            println!(
                "{:<8} {:<6} {:>8.2}M {:>8.1}M {:>8.1}M {:>8.1}x {:>7.1}ms {:>7.1}ms {:>8.0} {:>8.0} {:>7.0}",
                tier,
                name,
                mib(final_size),
                mib(r.logical),
                mib(r.stored),
                r.logical as f64 / r.stored.max(1) as f64,
                r.write_secs * 1000.0,
                r.read_secs * 1000.0,
                r.cpu_secs * 1000.0,
                r.rss_mib,
                mib(r.logical) / r.write_secs.max(1e-9),
            );
        }
        println!(
            "{:<8} {:<6} stored {:.1}x smaller, write {:+.0}%, read {:+.0}%, cpu {:+.0}%\n",
            "",
            "delta",
            whole.stored as f64 / xet.stored.max(1) as f64,
            100.0 * (xet.write_secs / whole.write_secs.max(1e-9) - 1.0),
            100.0 * (xet.read_secs / whole.read_secs.max(1e-9) - 1.0),
            100.0 * (xet.cpu_secs / whole.cpu_secs.max(1e-9) - 1.0),
        );
    }
}
