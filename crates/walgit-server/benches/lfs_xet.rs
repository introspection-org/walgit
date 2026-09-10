//! End-to-end comparison of three ways to keep revisions of a large file.
//!
//! Whole-object is what `lfs.rs` does today: one sha256-addressed object per
//! version. Xet is `lfs_xet::Xet::store_object`. Git is the file committed to a
//! repository and delta-compressed by `git repack -adf`, read back through
//! `git cat-file --batch`. The first two run against the same in-memory store
//! over the same JSONL trajectory revisions, so the difference is the storage
//! path alone, not the backend; git runs on a tempdir, and its numbers include
//! the subprocesses.
//!
//! Two revision shapes: `append` grows the file by a batch of lines each
//! revision (a long-running agent); `rescore` rewrites a score on every line
//! each revision (an eval re-run), which is the case where chunk-granular and
//! byte-granular deltas part ways.
//!
//! Measures, per shape, size tier and path: write and read wall time and
//! throughput, CPU time (children included), peak RSS, and bytes held.
//!
//! Run: `cargo bench -p walgit-server --features lfs-xet --bench lfs_xet`

// A bench panics on a broken fixture rather than reporting an error.
#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::string_slice,
    clippy::cast_precision_loss
)]

use std::fmt::Write as _;
use std::io::Write as _;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

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

/// The variable part of one JSONL trajectory line, roughly `bytes` long.
fn content(r: &mut Xorshift, bytes: usize) -> (String, u64) {
    let mut s = String::with_capacity(bytes + 16);
    while s.len() < bytes {
        s.push_str(WORDS[(r.next() % 16) as usize]);
        s.push(' ');
    }
    (s, r.next() % 900 + 100)
}

fn line(step: usize, content: &str, tokens: u64, score: f64) -> String {
    format!(
        "{{\"step_id\":{step},\"source\":\"agent\",\"content\":\"{content}\",\"score\":{score:.2},\"tokens\":{tokens}}}\n"
    )
}

#[derive(Clone, Copy)]
enum Shape {
    /// Each revision appends `per_rev` lines.
    Append,
    /// Every revision has all the lines; each rewrites the score on every line.
    Rescore,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::Append => "append",
            Shape::Rescore => "rescore",
        }
    }
}

fn revisions(shape: Shape, seed: u64, revs: usize, per_rev: usize, line_bytes: usize) -> Vec<Bytes> {
    let mut r = Xorshift(seed);
    let lines: Vec<(String, u64)> = (0..revs * per_rev).map(|_| content(&mut r, line_bytes)).collect();
    let mut out = Vec::with_capacity(revs);
    for rev in 0..revs {
        let (n, score) = match shape {
            Shape::Append => ((rev + 1) * per_rev, 0.5),
            Shape::Rescore => (lines.len(), rev as f64 / 100.0),
        };
        let mut doc = String::new();
        for (step, (c, tokens)) in lines[..n].iter().enumerate() {
            doc.push_str(&line(step, c, *tokens, score));
        }
        out.push(Bytes::from(doc));
    }
    out
}

/// User + system time of this process and of the children it has waited on.
fn cpu_secs() -> f64 {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let Some(i) = s.rfind(')') else { return 0.0 };
    let f: Vec<&str> = s[i + 1..].split_whitespace().collect();
    let tick = |n: usize| f.get(n).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
    (tick(11) + tick(12) + tick(13) + tick(14)) / 100.0
}

fn vm_hwm_mib(status_path: &str) -> f64 {
    std::fs::read_to_string(status_path)
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with("VmHWM:"))
        .and_then(|l| l.split_whitespace().nth(1)?.parse::<f64>().ok())
        .map_or(0.0, |kb| kb / 1024.0)
}

fn peak_rss_mib() -> f64 {
    vm_hwm_mib("/proc/self/status")
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
    /// Git only: the `repack -adf` share of `write_secs`.
    repack_secs: f64,
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

/// Run git in `dir`, feeding `stdin`, and return stdout plus the child's peak
/// RSS as last seen in `/proc` before it exited (sampled every millisecond, so
/// a hair under the true high-water mark).
fn git(dir: &Path, args: &[&str], stdin: &[u8]) -> (Vec<u8>, f64) {
    let mut child = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["-c", "user.name=bench", "-c", "user.email=bench@localhost", "-c", "gc.auto=0"])
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn git");
    let mut input = child.stdin.take().expect("stdin");
    let stdin = stdin.to_vec();
    let feed = std::thread::spawn(move || {
        let _ = input.write_all(&stdin);
    });
    let mut out = child.stdout.take().expect("stdout");
    let drain = std::thread::spawn(move || {
        let mut v = Vec::new();
        let _ = std::io::Read::read_to_end(&mut out, &mut v);
        v
    });
    let status_path = format!("/proc/{}/status", child.id());
    let mut rss = 0.0f64;
    loop {
        rss = rss.max(vm_hwm_mib(&status_path));
        if let Some(status) = child.try_wait().expect("wait git") {
            assert!(status.success(), "git {args:?} failed");
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    let _ = feed.join();
    (drain.join().expect("drain"), rss)
}

fn dir_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            let p = e.path();
            total += if p.is_dir() { dir_bytes(&p) } else { p.metadata().map_or(0, |m| m.len()) };
        }
    }
    total
}

fn run_git(series: &[Bytes]) -> Run {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let mut run = Run::default();
    let mut rss = 0.0f64;
    let (_, r) = git(dir, &["init", "-q"], b"");
    rss = rss.max(r);
    let file = dir.join("traj.jsonl");

    let c0 = cpu_secs();
    let t = Instant::now();
    for b in series {
        run.logical += b.len() as u64;
        std::fs::write(&file, b).expect("write");
        let (_, r) = git(dir, &["add", "traj.jsonl"], b"");
        rss = rss.max(r);
        let (_, r) = git(dir, &["commit", "-q", "-m", "rev"], b"");
        rss = rss.max(r);
    }
    let commit_secs = t.elapsed().as_secs_f64();
    // What the maintainer does: full delta search so every revision is stored
    // as a delta against its neighbours.
    let t = Instant::now();
    let (_, r) = git(dir, &["repack", "-a", "-d", "-f", "-q"], b"");
    rss = rss.max(r);
    run.repack_secs = t.elapsed().as_secs_f64();
    run.write_secs = commit_secs + run.repack_secs;

    let (shas, _) = git(dir, &["rev-list", "--reverse", "HEAD"], b"");
    let shas: Vec<&str> = std::str::from_utf8(&shas).expect("utf8").lines().collect();
    assert_eq!(shas.len(), series.len());
    // One process for every read, so the cost is delta resolution, not spawning.
    let requests = shas.iter().fold(String::new(), |mut s, sha| {
        let _ = writeln!(s, "{sha}:traj.jsonl");
        s
    });
    let t = Instant::now();
    let (out, r) = git(dir, &["cat-file", "--batch"], requests.as_bytes());
    rss = rss.max(r);
    let mut at = 0;
    for b in series {
        let nl = out[at..].iter().position(|&c| c == b'\n').expect("header") + at;
        let size: usize = std::str::from_utf8(&out[at..nl])
            .expect("utf8")
            .split(' ')
            .nth(2)
            .and_then(|s| s.parse().ok())
            .expect("size");
        assert_eq!(&out[nl + 1..nl + 1 + size], &b[..]);
        at = nl + 1 + size + 1;
    }
    run.read_secs = t.elapsed().as_secs_f64();
    run.cpu_secs = cpu_secs() - c0;
    run.stored = dir_bytes(&dir.join(".git/objects"));
    run.rss_mib = rss;
    run
}

fn mib(b: u64) -> f64 {
    b as f64 / 1_048_576.0
}

fn main() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let revs = 12;
    let iters = 5;

    println!("whole-object LFS vs Xet vs git, JSONL trajectories, {revs} revisions each, {iters}-run average\n");

    let avg = |runs: Vec<Run>| {
        let n = runs.len() as f64;
        let mut a = Run::default();
        for r in &runs {
            a.write_secs += r.write_secs / n;
            a.repack_secs += r.repack_secs / n;
            a.read_secs += r.read_secs / n;
            a.cpu_secs += r.cpu_secs / n;
            a.rss_mib = a.rss_mib.max(r.rss_mib);
        }
        a.logical = runs[0].logical;
        a.stored = runs[0].stored;
        a
    };

    for shape in [Shape::Append, Shape::Rescore] {
        println!(
            "== {} ==\n{:<8} {:<6} {:>9} {:>9} {:>9} {:>7} {:>9} {:>9} {:>8} {:>8} {:>7}",
            shape.name(), "tier", "path", "final", "logical", "stored", "ratio", "write", "read", "cpu ms", "rss MiB", "w MiB/s"
        );
        // (name, lines per revision, bytes per line) -> final file of roughly the named size.
        for (tier, per_rev, line_bytes) in [("small", 40, 512), ("medium", 200, 1024), ("large", 400, 2048)] {
            let series = revisions(shape, 7, revs, per_rev, line_bytes);
            let final_size = series.last().map(|b| b.len()).unwrap_or(0) as u64;

            let whole = avg((0..iters).map(|_| rt.block_on(run_whole(&series))).collect());
            let xet = avg((0..iters).map(|_| rt.block_on(run_xet(&series))).collect());
            let git = avg((0..iters).map(|_| run_git(&series)).collect());

            for (name, r) in [("whole", &whole), ("xet", &xet), ("git", &git)] {
                println!(
                    "{:<8} {:<6} {:>8.2}M {:>8.1}M {:>8.1}M {:>6.1}x {:>7.1}ms {:>7.1}ms {:>8.0} {:>8.0} {:>7.0}",
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
            let pct = |a: f64, b: f64| 100.0 * (a / b.max(1e-9) - 1.0);
            // CPU comes from /proc in 10 ms ticks, so a ratio over a base under
            // one tick is noise.
            let cpu = |r: &Run| {
                if whole.cpu_secs < 0.01 {
                    "n/a".to_owned()
                } else {
                    format!("{:+.0}%", pct(r.cpu_secs, whole.cpu_secs))
                }
            };
            for (name, r) in [("xet", &xet), ("git", &git)] {
                println!(
                    "{:<8} {:<6} vs whole: stored {:.1}x smaller, write {:+.0}%{}, read {:+.0}%, cpu {}",
                    "",
                    name,
                    whole.stored as f64 / r.stored.max(1) as f64,
                    pct(r.write_secs, whole.write_secs),
                    if r.repack_secs > 0.0 { format!(" (repack {:.0}ms of it)", r.repack_secs * 1000.0) } else { String::new() },
                    pct(r.read_secs, whole.read_secs),
                    cpu(r),
                );
            }
            println!();
        }
    }
}
