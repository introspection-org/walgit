//! A corpus of GenAI conversations: many long-running sessions that share a
//! common head (system prompt, tool definitions, agent metadata) and diverge
//! into unique turns.
//!
//! Two dedup dimensions matter and they behave differently: growth within one
//! conversation, and structure shared across conversations. The second only
//! pays off when the shared region is large relative to the chunk size.

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

fn text(r: &mut ChaCha8Rng, n: usize) -> String {
    const W: [&str; 16] = [
        "the", "model", "returns", "a", "tool", "call", "with", "arguments",
        "then", "observes", "output", "and", "continues", "reasoning", "until", "done",
    ];
    let mut s = String::with_capacity(n + 16);
    while s.len() < n {
        s.push_str(W[r.gen_range(0..W.len())]);
        s.push(' ');
    }
    s.truncate(n);
    s
}

/// Head shared verbatim by every conversation in the corpus.
fn shared_head(bytes: usize) -> String {
    let mut r = ChaCha8Rng::seed_from_u64(1);
    format!(
        "{{\"schema\":\"trajectory-v1\",\"system\":\"{}\",\"tools\":\"{}\",\"steps\":[",
        text(&mut r, bytes / 2),
        text(&mut r, bytes / 2)
    )
}

/// One conversation: the shared head, then `turns` unique turns.
fn conversation(head: &str, seed: u64, turns: usize, turn_bytes: usize) -> Vec<u8> {
    let mut r = ChaCha8Rng::seed_from_u64(seed);
    let mut s = String::from(head);
    for t in 0..turns {
        s.push_str(&format!(
            "{{\"i\":{t},\"role\":\"assistant\",\"content\":\"{}\"}},",
            text(&mut r, turn_bytes)
        ));
    }
    s.push_str("]}");
    s.into_bytes()
}

fn main() {
    let target = *TARGET_CHUNK_SIZE;
    println!("GenAI conversation corpus — cross-conversation dedup (chunk {} KiB)\n", target / 1024);

    let convs = 200;
    let turns = 60;
    let turn_bytes = 2048; // ~120 KiB of unique content per conversation

    println!("{:>12} {:>10} {:>12} {:>12} {:>8}", "shared head", "conv size", "walgit", "xet", "ratio");
    for head_kib in [8usize, 32, 64, 128, 512] {
        let head = shared_head(head_kib * 1024);
        let mut seen: HashSet<[u8; 32]> = HashSet::new();
        let (mut whole, mut xet) = (0usize, 0usize);
        let mut one = 0usize;
        for i in 0..convs {
            let c = conversation(&head, 1000 + i as u64, turns, turn_bytes);
            one = c.len();
            whole += c.len();
            for (h, len) in chunks(&c, target) {
                if seen.insert(h) {
                    xet += len;
                }
            }
        }
        println!(
            "{:>9} KiB {:>9.2}M {:>11.1}M {:>11.1}M {:>7.2}x",
            head_kib,
            one as f64 / 1048576.0,
            whole as f64 / 1048576.0,
            xet as f64 / 1048576.0,
            whole as f64 / xet.max(1) as f64
        );
    }

    println!("\n{convs} conversations x {turns} turns. Only the head is shared; turns are unique.\n");

    // The chunk target is a parameter, so it is the lever for shared regions
    // that are smaller than the default 64 KiB chunk.
    println!("Chunk-size sensitivity, 16 KiB shared head (a realistic system prompt + tools):");
    println!("{:>12} {:>10} {:>12} {:>12} {:>8} {:>10}",
             "chunk target", "chunks/conv", "walgit", "xet", "ratio", "meta est");
    let head = shared_head(16 * 1024);
    for kib in [64usize, 32, 16, 8, 4, 2] {
        let t = kib * 1024;
        let mut seen: HashSet<[u8; 32]> = HashSet::new();
        let (mut whole, mut xet, mut nchunks) = (0usize, 0usize, 0usize);
        for i in 0..convs {
            let c = conversation(&head, 1000 + i as u64, turns, turn_bytes);
            whole += c.len();
            let cs = chunks(&c, t);
            nchunks += cs.len();
            for (h, len) in cs {
                if seen.insert(h) { xet += len; }
            }
        }
        // 32-byte hash + ~16 B bookkeeping per chunk stored.
        let meta = seen.len() * 48;
        println!("{:>9} KiB {:>10.0} {:>11.1}M {:>11.1}M {:>7.2}x {:>9.2}M",
                 kib, nchunks as f64 / convs as f64,
                 whole as f64 / 1048576.0, xet as f64 / 1048576.0,
                 whole as f64 / xet.max(1) as f64, meta as f64 / 1048576.0);
    }
}
