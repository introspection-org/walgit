//! Resource profile: walgit's whole-object LFS write path vs a Xet chunking path.
//!
//! walgit's PUT computes sha256 over the stream and writes one object. A Xet path
//! computes the same sha256 (the LFS oid is sha256 by protocol) and additionally
//! chunks, hashes and dedup-queries. Both paths are measured over identical bytes,
//! with the buffer materialised before the clock starts.

use std::collections::HashSet;
use std::time::Instant;

use bytes::Bytes;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sha2::{Digest, Sha256};
use xet_core_structures::xorb_object::constants::TARGET_CHUNK_SIZE;
use xet_data::deduplication::Chunker;

/// (utime + stime) for this process, in seconds.
fn cpu_secs() -> f64 {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    // utime and stime are fields 14 and 15, after the comm field which may contain spaces.
    let tail = match s.rfind(')') { Some(i) => s[i + 1..].to_string(), None => return 0.0 };
    let f: Vec<&str> = tail.split_whitespace().collect();
    let hz = 100.0;
    let u: f64 = f.get(11).and_then(|v| v.parse().ok()).unwrap_or(0.0);
    let k: f64 = f.get(12).and_then(|v| v.parse().ok()).unwrap_or(0.0);
    (u + k) / hz
}

/// Peak resident set size in MiB.
fn peak_rss_mib() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with("VmHWM:"))
        .and_then(|l| l.split_whitespace().nth(1)?.parse::<f64>().ok())
        .map(|kb| kb / 1024.0)
        .unwrap_or(0.0)
}

fn main() {
    let target = *TARGET_CHUNK_SIZE;
    println!("LFS whole-object vs Xet chunking — resource profile");
    println!("chunk target {} KiB\n", target / 1024);
    println!("{:>7} {:>10} {:>10} {:>10} {:>10} {:>10} {:>9}",
             "size", "path", "wall ms", "MiB/s", "cpu ms", "cpu/wall", "peakRSS");

    for mib in [8usize, 32, 128, 512] {
        let n = mib * 1024 * 1024;
        let mut r = ChaCha8Rng::seed_from_u64(11);
        let raw: Vec<u8> = (0..n).map(|_| r.gen()).collect();
        let buf = Bytes::from(raw);          // materialised before timing

        // --- LFS path: sha256 the stream ---
        let c0 = cpu_secs();
        let t0 = Instant::now();
        let mut h = Sha256::new();
        h.update(&buf);
        let oid = h.finalize();
        let lfs_wall = t0.elapsed().as_secs_f64();
        let lfs_cpu = cpu_secs() - c0;
        std::hint::black_box(&oid);
        let lfs_rss = peak_rss_mib();
        println!("{:>5}MiB {:>10} {:>10.1} {:>10.0} {:>10.1} {:>10.2} {:>8.0}M",
                 mib, "lfs", lfs_wall * 1000.0, (n as f64 / 1048576.0) / lfs_wall,
                 lfs_cpu * 1000.0, lfs_cpu / lfs_wall.max(1e-9), lfs_rss);

        // --- Xet path: same sha256, plus chunk + hash + dedup query ---
        let mut seen: HashSet<[u8; 32]> = HashSet::new();
        let c0 = cpu_secs();
        let t0 = Instant::now();
        let mut h = Sha256::new();
        h.update(&buf);
        let oid = h.finalize();
        let mut chunker = Chunker::new(target);
        let mut novel = 0usize;
        for c in chunker.next_block_bytes(&buf, true) {
            let mut k = [0u8; 32];
            k.copy_from_slice(c.hash.as_bytes());
            if seen.insert(k) {
                novel += c.data.len();
            }
        }
        if let Some(c) = chunker.finish() {
            let mut k = [0u8; 32];
            k.copy_from_slice(c.hash.as_bytes());
            if seen.insert(k) { novel += c.data.len(); }
        }
        let xet_wall = t0.elapsed().as_secs_f64();
        let xet_cpu = cpu_secs() - c0;
        std::hint::black_box((&oid, novel));
        let xet_rss = peak_rss_mib();
        println!("{:>5}MiB {:>10} {:>10.1} {:>10.0} {:>10.1} {:>10.2} {:>8.0}M",
                 mib, "xet", xet_wall * 1000.0, (n as f64 / 1048576.0) / xet_wall,
                 xet_cpu * 1000.0, xet_cpu / xet_wall.max(1e-9), xet_rss);
        println!("{:>7} {:>10} {:>+9.1}% {:>10} {:>+9.1}% {:>10} {:>+8.0}M",
                 "", "delta", 100.0 * (xet_wall / lfs_wall - 1.0), "",
                 100.0 * (xet_cpu / lfs_cpu.max(1e-9) - 1.0), "", xet_rss - lfs_rss);
        println!("{:>7} {:>10} chunks {}, novel {:.1} MiB\n", "", "",
                 seen.len(), novel as f64 / 1048576.0);
    }
}
