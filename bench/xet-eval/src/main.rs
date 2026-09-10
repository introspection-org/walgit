//! Xet CDC dedup vs walgit's whole-object LFS store, across realistic payload shapes.
//!
//! walgit stores an LFS object whole and sha256-addressed, so it dedups only on an
//! exact byte-for-byte match. Xet dedups at chunk granularity, which shows up three
//! ways this measures separately: inside one file, between versions of a file, and
//! between sibling files that were never byte-identical.

use std::collections::HashSet;
use std::time::Instant;

use bytes::Bytes;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sha2::{Digest, Sha256};
use xet_core_structures::xorb_object::constants::TARGET_CHUNK_SIZE;
use xet_data::deduplication::Chunker;

type Hashes = Vec<(Vec<u8>, usize)>;

fn chunk_all(data: &[u8], target: usize) -> Hashes {
    chunk_bytes(&Bytes::copy_from_slice(data), target)
}

/// Takes an owned `Bytes` so a timed run measures chunking, not a memcpy.
fn chunk_bytes(b: &Bytes, target: usize) -> Hashes {
    let mut chunker = Chunker::new(target);
    let mut out: Hashes = chunker
        .next_block_bytes(b, true)
        .into_iter()
        .map(|c| (c.hash.as_bytes().to_vec(), c.data.len()))
        .collect();
    if let Some(c) = chunker.finish() {
        out.push((c.hash.as_bytes().to_vec(), c.data.len()));
    }
    out
}

fn sha256(d: &[u8]) -> Vec<u8> {
    let mut h = Sha256::new();
    h.update(d);
    h.finalize().to_vec()
}

/// Bytes xet must store given everything already in `seen`.
fn novel(chunks: &Hashes, seen: &HashSet<Vec<u8>>) -> usize {
    let mut local = HashSet::new();
    let mut n = 0;
    for (h, len) in chunks {
        if !seen.contains(h) && local.insert(h.clone()) {
            n += len;
        }
    }
    n
}

// ---- payload shapes -------------------------------------------------------

/// Incompressible: encrypted archives, already-compressed media.
fn random(r: &mut ChaCha8Rng, n: usize) -> Vec<u8> {
    (0..n).map(|_| r.gen()).collect()
}

/// f32 tensors in blocks, the shape of model weights: locally smooth, with a
/// per-block scale so blocks differ from one another.
fn tensor(r: &mut ChaCha8Rng, n: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(n);
    let block = 4096;
    let mut base: f32 = 0.0;
    while v.len() < n {
        base = r.gen_range(-1.0..1.0);
        for _ in 0..block / 4 {
            if v.len() >= n { break; }
            let x = base + r.gen_range(-0.02..0.02);
            v.extend_from_slice(&x.to_le_bytes());
        }
    }
    v.truncate(n);
    v
}

/// Uniform zeros: no content boundary ever fires, so the chunker cuts at
/// MAX_CHUNK_SIZE and every chunk is identical.
fn zeros(_r: &mut ChaCha8Rng, n: usize) -> Vec<u8> {
    vec![0u8; n]
}

/// A 4 MiB motif repeated to fill the file: real internal redundancy that
/// whole-object storage cannot see at all.
fn repeated(r: &mut ChaCha8Rng, n: usize) -> Vec<u8> {
    let motif = random(r, 4 * 1024 * 1024);
    let mut v = Vec::with_capacity(n);
    while v.len() < n {
        let take = motif.len().min(n - v.len());
        v.extend_from_slice(&motif[..take]);
    }
    v
}

/// JSONL rows, the shape of a text dataset.
fn jsonl(r: &mut ChaCha8Rng, n: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(n);
    let mut id = 0u64;
    while v.len() < n {
        id += 1;
        let score: f32 = r.gen();
        v.extend_from_slice(
            format!("{{\"id\":{id},\"score\":{score:.6},\"label\":\"class_{}\"}}\n", id % 17)
                .as_bytes(),
        );
    }
    v.truncate(n);
    v
}

fn main() {
    let target = *TARGET_CHUNK_SIZE;
    let size = 32 * 1024 * 1024;
    println!("Xet CDC vs walgit whole-object LFS");
    println!("TARGET_CHUNK_SIZE = {} KiB, payload = {} MiB\n", target / 1024, size / 1048576);

    let shapes: Vec<(&str, fn(&mut ChaCha8Rng, usize) -> Vec<u8>)> = vec![
        ("random binary", random),
        ("f32 tensor (weights)", tensor),
        ("uniform zeros", zeros),
        ("repeated 4 MiB motif", repeated),
        ("jsonl text dataset", jsonl),
    ];

    for (name, gen) in &shapes {
        let mut r = ChaCha8Rng::seed_from_u64(1);
        let v1 = gen(&mut r, size);

        let t = Instant::now();
        let c1 = chunk_all(&v1, target);
        let chunk_thru = (size as f64 / 1048576.0) / t.elapsed().as_secs_f64();
        let t = Instant::now();
        let _ = sha256(&v1);
        let sha_thru = (size as f64 / 1048576.0) / t.elapsed().as_secs_f64();

        // v1 alone: walgit always stores the whole file; xet drops internal repeats.
        let empty = HashSet::new();
        let internal = novel(&c1, &empty);

        println!("### {name}");
        println!("  {} chunks, avg {:.1} KiB | chunk {:.0} MiB/s, sha256 {:.0} MiB/s, both {:.0} MiB/s",
                 c1.len(), (size as f64 / c1.len() as f64) / 1024.0,
                 chunk_thru, sha_thru, 1.0 / (1.0 / chunk_thru + 1.0 / sha_thru));
        println!("  first upload (internal dedup):  walgit {:>7.1} MiB   xet {:>7.1} MiB   saved {:>5.1}%",
                 size as f64 / 1048576.0, internal as f64 / 1048576.0,
                 100.0 * (1.0 - internal as f64 / size as f64));

        let seen: HashSet<Vec<u8>> = c1.iter().map(|(h, _)| h.clone()).collect();
        let oid1 = sha256(&v1);

        let mut row = |label: &str, v2: &[u8]| {
            let w = if sha256(v2) == oid1 { 0 } else { v2.len() };
            let x = novel(&chunk_all(v2, target), &seen);
            let saved = if w == 0 { 0.0 } else { 100.0 * (1.0 - x as f64 / w as f64) };
            println!("  {:<30} walgit {:>7.1} MiB   xet {:>7.1} MiB   saved {:>5.1}%",
                     label, w as f64 / 1048576.0, x as f64 / 1048576.0, saved);
        };

        // same bytes
        row("re-upload identical", &v1);
        // 1 KiB patched in the middle
        let mut v = v1.clone();
        let at = v.len() / 2;
        let mut r2 = ChaCha8Rng::seed_from_u64(2);
        for i in 0..1024 { v[at + i] = r2.gen(); }
        row("1 KiB edit mid-file", &v);
        // grown by 1%
        let mut v = v1.clone();
        v.extend(gen(&mut ChaCha8Rng::seed_from_u64(3), size / 100));
        row("append 1%", &v);
        // every offset shifted
        let mut v = random(&mut ChaCha8Rng::seed_from_u64(4), 1024);
        v.extend_from_slice(&v1);
        row("prepend 1 KiB (shifts all)", &v);
        // fine-tune: 10% of blocks rewritten
        let mut v = v1.clone();
        let mut r3 = ChaCha8Rng::seed_from_u64(5);
        let span = size / 10;
        let at = size / 4;
        let patch = gen(&mut r3, span);
        v[at..at + span].copy_from_slice(&patch);
        row("fine-tune (10% rewritten)", &v);
        // independent file of the same shape: shares nothing by construction
        row("unrelated file, same shape", &gen(&mut ChaCha8Rng::seed_from_u64(99), size));
        // sibling derived from v1 with 30% replaced -- two variants of one artifact
        let mut v = v1.clone();
        let span = size * 3 / 10;
        let patch = gen(&mut ChaCha8Rng::seed_from_u64(7), span);
        v[..span].copy_from_slice(&patch);
        row("sibling variant (70% shared)", &v);
        println!();
    }
}
