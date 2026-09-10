use std::collections::HashSet;
use std::time::Instant;

use bytes::Bytes;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use sha2::{Digest, Sha256};
use xet_data::deduplication::Chunker;
use xet_core_structures::xorb_object::constants::TARGET_CHUNK_SIZE;

fn chunk_all(data: &Bytes, target: usize) -> Vec<(Vec<u8>, usize)> {
    let mut chunker = Chunker::new(target);
    let mut out = Vec::new();
    for c in chunker.next_block_bytes(data, true) {
        out.push((c.hash.as_bytes().to_vec(), c.data.len()));
    }
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

struct Case {
    name: &'static str,
    v2: fn(&mut ChaCha8Rng, &[u8]) -> Vec<u8>,
}

fn main() {
    let target = *TARGET_CHUNK_SIZE;
    println!("xet TARGET_CHUNK_SIZE = {} bytes ({:.0} KiB)\n", target, target as f64 / 1024.0);

    let cases: Vec<Case> = vec![
        Case { name: "identical re-upload", v2: |_r, base| base.to_vec() },
        Case { name: "1 KiB edit mid-file", v2: |r, base| {
            let mut v = base.to_vec();
            let at = v.len() / 2;
            for i in 0..1024.min(v.len() - at) { v[at + i] = r.gen(); }
            v
        }},
        Case { name: "append 1%", v2: |r, base| {
            let mut v = base.to_vec();
            let extra = base.len() / 100;
            v.extend((0..extra).map(|_| r.gen::<u8>()));
            v
        }},
        Case { name: "prepend 1 KiB (shifts everything)", v2: |r, base| {
            let mut v: Vec<u8> = (0..1024).map(|_| r.gen::<u8>()).collect();
            v.extend_from_slice(base);
            v
        }},
        Case { name: "unrelated file", v2: |r, base| (0..base.len()).map(|_| r.gen::<u8>()).collect() },
        Case { name: "sibling checkpoint (10% differs)", v2: |r, base| {
            let mut v = base.to_vec();
            let span = v.len() / 10;
            let at = v.len() / 4;
            for i in 0..span.min(v.len() - at) { v[at + i] = r.gen(); }
            v
        }},
    ];

    for mib in [1usize, 16, 128] {
        let size = mib * 1024 * 1024;
        let mut rng = ChaCha8Rng::seed_from_u64(42);
        let base: Vec<u8> = (0..size).map(|_| rng.gen::<u8>()).collect();
        let base_b = Bytes::from(base.clone());

        let t = Instant::now();
        let base_chunks = chunk_all(&base_b, target);
        let chunk_secs = t.elapsed().as_secs_f64();
        let thru = (size as f64 / (1024.0 * 1024.0)) / chunk_secs;

        let t = Instant::now();
        let _ = sha256(&base);
        let sha_secs = t.elapsed().as_secs_f64();
        let sha_thru = (size as f64 / (1024.0 * 1024.0)) / sha_secs;

        println!("=== {} MiB base ===", mib);
        println!("  chunks: {}  avg {:.1} KiB", base_chunks.len(),
                 (size as f64 / base_chunks.len() as f64) / 1024.0);
        // LFS oids are sha256 by protocol, so xet is additive, never a replacement.
        let combined = 1.0 / (1.0 / thru + 1.0 / sha_thru);
        println!("  sha256 (walgit, required either way): {:.0} MiB/s", sha_thru);
        println!("  xet chunking+hash alone:              {:.0} MiB/s", thru);
        println!("  sha256 + xet combined:                {:.0} MiB/s  => +{:.0}% CPU time vs sha256 alone",
                 combined, 100.0 * (sha_thru / combined - 1.0));
        // 32-byte chunk hash + ~16 bytes of offset/length bookkeeping per chunk.
        let meta = base_chunks.len() * 48;
        println!("  chunk metadata: {} chunks x ~48 B = {:.2} MiB ({:.3}% of file)",
                 base_chunks.len(), meta as f64 / 1048576.0, 100.0 * meta as f64 / size as f64);

        let seen: HashSet<Vec<u8>> = base_chunks.iter().map(|(h, _)| h.clone()).collect();
        let base_oid = sha256(&base);

        println!("  {:<34} {:>12} {:>12} {:>10}", "scenario (storing v2)", "walgit", "xet", "saved");
        for c in &cases {
            let mut r = ChaCha8Rng::seed_from_u64(7);
            let v2 = (c.v2)(&mut r, &base);
            let walgit = if sha256(&v2) == base_oid { 0 } else { v2.len() };
            let v2c = chunk_all(&Bytes::from(v2.clone()), target);
            let xet: usize = v2c.iter().filter(|(h, _)| !seen.contains(h)).map(|(_, n)| *n).sum();
            let saved = if walgit == 0 { 0.0 } else { 100.0 * (1.0 - xet as f64 / walgit as f64) };
            println!("  {:<34} {:>10.1} MiB {:>10.1} MiB {:>9.1}%",
                     c.name, walgit as f64 / 1048576.0, xet as f64 / 1048576.0, saved);
        }
        println!();
    }
}
