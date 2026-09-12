//! Content-defined chunking for LFS objects (`docs/XET.md`).
//!
//! An object is cut into content-defined chunks, novel chunks are packed into
//! xorbs, and the chunk-to-xorb mapping lives in one shard per repository. Two
//! objects that differ by a byte therefore share every chunk they have in common,
//! where whole-object storage keeps two complete copies.
//!
//! The shard is the index: `MetadataShard` carries its own chunk lookup, so the
//! dedup query is a seek into a stored object rather than a structure walgit
//! maintains. It is rewritten under a CAS on its own version, so two concurrent
//! writers cannot lose each other's entries.
//!
//! Xorbs are written with their footer, so a read is one ranged GET per term:
//! the footer maps a chunk range to the compressed byte range that holds it.

use std::collections::VecDeque;
use std::io::Cursor;
use std::ops::Range;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt, stream};
use moka::sync::Cache;
use parking_lot::Mutex;
use walgit_store::{
    DynStore, GetOptions, ObjectMeta, PutBody, PutMode, StoreError, Version,
};
use xet_core_structures::merklehash::{MerkleHash, file_hash};
use xet_core_structures::metadata_shard::file_structs::{
    FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo,
};
use xet_core_structures::metadata_shard::shard_format::MDBShardInfo;
use xet_core_structures::metadata_shard::shard_in_memory::MDBInMemoryShard;
use xet_core_structures::xorb_object::constants::{
    MAX_XORB_BYTES, MAX_XORB_CHUNKS, TARGET_CHUNK_SIZE,
};
use xet_core_structures::xorb_object::{
    Chunk, RawXorbData, SerializedXorbObject, XorbObject, deserialize_chunks,
};
use xet_data::deduplication::Chunker;

/// Everything this module writes lives under the repository's own prefix.
const XET_DIR: &str = "lfs/xet/";
const SHARD_KEY: &str = "lfs/xet/shard";
const XORB_COMPRESSION: &str = "lz4";
/// Fragmentation estimator (spec, "Fragmentation Prevention"; the numbers are
/// `git-xet`'s): the mean chunks per term over the last `DEFRAG_WINDOW` terms.
const DEFRAG_WINDOW: usize = 128;
const DEFRAG_MIN_CHUNKS_PER_TERM: f32 = 8.0;
const DEFRAG_HYSTERESIS: f32 = 0.5;
/// A footer is ~40 bytes per chunk, so one tail read covers most xorbs.
const FOOTER_TAIL_BYTES: u64 = 64 * 1024;
const FOOTER_CACHE_ENTRIES: u64 = 4096;
const READ_CONCURRENCY: usize = 8;

type Result<T> = std::result::Result<T, StoreError>;

fn xorb_key(hash: &MerkleHash) -> String {
    let hex = hash.hex();
    let (aa, bb) = (&hex[..2], &hex[2..4]);
    format!("{XET_DIR}xorbs/{aa}/{bb}/{hex}")
}

fn xet_err(what: &str, e: impl std::fmt::Display) -> StoreError {
    StoreError::other(anyhow::anyhow!("xet: {what}: {e}"))
}

/// What a store call did, for metrics and tests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StoreStats {
    /// The spec file hash: a Merkle root over the chunk hashes, not a hash of
    /// the bytes, so it is the id `git-xet` would derive for the same content.
    pub file_hash: MerkleHash,
    pub logical_bytes: u64,
    pub novel_bytes: u64,
    pub chunks: usize,
    pub novel_chunks: usize,
    /// Terms in the reconstruction; one per contiguous chunk run.
    pub terms: usize,
}

fn chunk_all(data: &Bytes) -> Vec<Chunk> {
    let mut chunker = Chunker::new(*TARGET_CHUNK_SIZE);
    let mut out = chunker.next_block_bytes(data, true);
    if let Some(c) = chunker.finish() {
        out.push(c);
    }
    out
}

fn continues(last: &FileDataSequenceEntry, next: &FileDataSequenceEntry) -> bool {
    last.xorb_hash == next.xorb_hash && last.chunk_index_end == next.chunk_index_start
}

/// Append a term, merging it into the previous one when the chunk run
/// continues. Returns whether it merged.
fn push_term(terms: &mut Vec<FileDataSequenceEntry>, next: FileDataSequenceEntry) -> bool {
    match terms.last_mut() {
        Some(last) if continues(last, &next) => {
            last.unpacked_segment_bytes += next.unpacked_segment_bytes;
            last.chunk_index_end = next.chunk_index_end;
            true
        }
        _ => {
            terms.push(next);
            false
        }
    }
}

/// Once a file's recent terms average too few chunks, a matched run shorter
/// than that average is stored again rather than referenced, so a read stays a
/// few large ranges instead of many small ones.
#[derive(Default)]
struct Defrag {
    window: VecDeque<usize>,
    chunks: usize,
    strict: bool,
}

impl Defrag {
    fn record(&mut self, n: usize, merged: bool) {
        self.chunks += n;
        if merged {
            if let Some(last) = self.window.back_mut() {
                *last += n;
            }
            return;
        }
        self.window.push_back(n);
        if self.window.len() > DEFRAG_WINDOW {
            self.chunks -= self.window.pop_front().unwrap_or(0);
        }
    }

    fn allow(&mut self, n: usize) -> bool {
        if self.window.len() < DEFRAG_WINDOW {
            return true;
        }
        let mean = self.chunks as f32 / self.window.len() as f32;
        let target = if self.strict {
            DEFRAG_MIN_CHUNKS_PER_TERM
        } else {
            DEFRAG_MIN_CHUNKS_PER_TERM * DEFRAG_HYSTERESIS
        };
        if mean >= target {
            self.strict = false;
        } else if (n as f32) < mean {
            self.strict = true;
            return false;
        }
        true
    }
}

/// One repository's chunk store. Holds the parsed shard and the xorb footers
/// so a read is a ranged GET per term, not a shard parse and a whole xorb.
pub struct Xet {
    store: DynStore,
    shard: Mutex<Option<(Version, Arc<MDBInMemoryShard>)>>,
    footers: Cache<MerkleHash, Arc<XorbObject>>,
}

impl Xet {
    pub fn new(store: DynStore) -> Self {
        Self {
            store,
            shard: Mutex::new(None),
            footers: Cache::builder().max_capacity(FOOTER_CACHE_ENTRIES).build(),
        }
    }

    /// The shard, revalidated with a conditional GET so an unchanged one costs
    /// no body and no parse.
    async fn shard(&self) -> Result<(Arc<MDBInMemoryShard>, Option<Version>)> {
        let cached = self.shard.lock().clone();
        let opts = GetOptions {
            if_none_match: cached.as_ref().map(|(v, _)| v.clone()),
            ..GetOptions::default()
        };
        let got = match self.store.get(SHARD_KEY, opts).await {
            Ok(r) => r.bytes().await?,
            Err(StoreError::NotFound { .. }) => return Ok((Arc::default(), None)),
            Err(e) => return Err(e),
        };
        let Some((meta, bytes)) = got else {
            let (version, shard) =
                cached.ok_or_else(|| xet_err("shard", "not-modified with nothing cached"))?;
            return Ok((shard, Some(version)));
        };
        let shard = MDBInMemoryShard::from_reader(&mut Cursor::new(bytes.as_ref()))
            .map_err(|e| xet_err("shard read", e))?;
        let shard = Arc::new(shard);
        *self.shard.lock() = Some((meta.version.clone(), Arc::clone(&shard)));
        Ok((shard, Some(meta.version)))
    }

    async fn write_shard(&self, shard: MDBInMemoryShard, at: Option<Version>) -> Result<()> {
        let mut buf = Vec::new();
        MDBShardInfo::serialize_from(&mut buf, &shard, None)
            .map_err(|e| xet_err("shard write", e))?;
        let mode = match at {
            Some(v) => PutMode::Update(v),
            None => PutMode::Create,
        };
        let meta = self.store.put(SHARD_KEY, PutBody::Bytes(buf.into()), mode.into()).await?;
        *self.shard.lock() = Some((meta.version, Arc::new(shard)));
        Ok(())
    }

    /// Serialize `chunks` as one xorb, store it, and register it in `shard`.
    async fn write_xorb(&self, shard: &mut MDBInMemoryShard, chunks: &[Chunk]) -> Result<MerkleHash> {
        let xorb = RawXorbData::from_chunks(chunks, vec![chunks.len()]);
        let hash = xorb.hash();
        let info = xorb.xorb_info.clone();
        let bytes = SerializedXorbObject::from_xorb(xorb, true, XORB_COMPRESSION, 0)
            .map_err(|e| xet_err("xorb write", e))?
            .serialized_data;
        let footer = XorbObject::deserialize(&mut Cursor::new(bytes.as_slice()))
            .map_err(|e| xet_err("xorb footer", e))?;
        match self
            .store
            .put(&xorb_key(&hash), PutBody::Bytes(bytes.into()), PutMode::Create.into())
            .await
        {
            // A concurrent writer storing the identical xorb is not a conflict:
            // the hash is the content, so whoever won wrote the same bytes.
            Ok(_) | Err(StoreError::PreconditionFailed { .. }) => {}
            Err(e) => return Err(e),
        }
        self.footers.insert(hash, Arc::new(footer));
        shard.add_xorb_block(info).map_err(|e| xet_err("shard xorb", e))?;
        Ok(hash)
    }

    /// Chunk `data`, store the chunks this repository has not seen, and record
    /// the reconstruction under the file hash the chunks derive.
    pub async fn store_object(&self, data: Bytes) -> Result<StoreStats> {
        let chunks = chunk_all(&data);
        let hashes: Vec<MerkleHash> = chunks.iter().map(|c| c.hash).collect();
        let sizes: Vec<(MerkleHash, u64)> =
            chunks.iter().map(|c| (c.hash, c.data.len() as u64)).collect();
        let file_hash = file_hash(&sizes);

        let (shard, version) = self.shard().await?;
        // Drop the cache's reference so the shard is unwrapped rather than cloned.
        self.shard.lock().take();
        let mut shard = Arc::unwrap_or_clone(shard);

        let mut stats = StoreStats {
            file_hash,
            logical_bytes: data.len() as u64,
            chunks: chunks.len(),
            ..StoreStats::default()
        };

        // Novel chunks are collected into a xorb under a marker hash and bound
        // to the real one once the xorb is cut, as `git-xet` does.
        let mut terms: Vec<FileDataSequenceEntry> = Vec::new();
        let mut defrag = Defrag::default();
        let mut novel: Vec<Chunk> = Vec::new();
        let mut novel_bytes = 0usize;
        let mut i = 0;
        while i < chunks.len() {
            if let Some((n, entry)) = shard.chunk_hash_dedup_query(&hashes[i..]) {
                let merges = terms.last().is_some_and(|last| continues(last, &entry));
                if merges || defrag.allow(n) {
                    defrag.record(n, push_term(&mut terms, entry));
                    i += n;
                    continue;
                }
            }
            let chunk = &chunks[i];
            if novel_bytes + chunk.data.len() > *MAX_XORB_BYTES || novel.len() >= *MAX_XORB_CHUNKS {
                let hash = self.write_xorb(&mut shard, &novel).await?;
                bind_marker(&mut terms, hash);
                novel.clear();
                novel_bytes = 0;
            }
            let idx = novel.len();
            let term = FileDataSequenceEntry::new(MerkleHash::marker(), chunk.data.len(), idx, idx + 1);
            defrag.record(1, push_term(&mut terms, term));
            novel_bytes += chunk.data.len();
            novel.push(chunk.clone());
            stats.novel_bytes += chunk.data.len() as u64;
            stats.novel_chunks += 1;
            i += 1;
        }
        if !novel.is_empty() {
            let hash = self.write_xorb(&mut shard, &novel).await?;
            bind_marker(&mut terms, hash);
        }
        stats.terms = terms.len();

        let info = MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash, terms.len(), false, false),
            segments: terms,
            verification: Vec::new(),
            metadata_ext: None,
        };
        shard.add_file_reconstruction_info(info).map_err(|e| xet_err("shard add", e))?;
        self.write_shard(shard, version).await?;
        Ok(stats)
    }

    /// Rebuild an object from its chunks. `Ok(None)` when the shard has no record of it.
    pub async fn load_object(&self, file_hash: &MerkleHash) -> Result<Option<Bytes>> {
        let (shard, _) = self.shard().await?;
        let Some(info) = shard.get_file_reconstruction_info(file_hash) else {
            return Ok(None);
        };
        let mut parts: Vec<Bytes> = stream::iter(info.segments.iter().map(|seg| self.term(seg)))
            .buffered(READ_CONCURRENCY)
            .try_collect()
            .await?;
        if parts.len() == 1 {
            return Ok(parts.pop());
        }
        let total = parts.iter().map(Bytes::len).sum();
        let mut out = BytesMut::with_capacity(total);
        for p in &parts {
            out.extend_from_slice(p);
        }
        Ok(Some(out.freeze()))
    }

    async fn term(&self, seg: &FileDataSequenceEntry) -> Result<Bytes> {
        let footer = self.footer(&seg.xorb_hash).await?;
        let (from, to) = footer
            .get_byte_offset(seg.chunk_index_start, seg.chunk_index_end)
            .map_err(|e| xet_err("term range", e))?;
        let bytes = self.range(&xorb_key(&seg.xorb_hash), u64::from(from)..u64::from(to)).await?;
        let (data, _) = deserialize_chunks(&mut Cursor::new(bytes.as_ref()))
            .map_err(|e| xet_err("xorb read", e))?;
        if data.len() != seg.unpacked_segment_bytes as usize {
            return Err(xet_err("term", "unpacked length differs from the shard"));
        }
        Ok(Bytes::from(data))
    }

    /// The xorb's footer, read from the tail of the stored object on a cache miss.
    async fn footer(&self, hash: &MerkleHash) -> Result<Arc<XorbObject>> {
        if let Some(f) = self.footers.get(hash) {
            return Ok(f);
        }
        let key = xorb_key(hash);
        let ObjectMeta { size, .. } = self
            .store
            .head(&key)
            .await?
            .ok_or_else(|| StoreError::NotFound { key: key.clone() })?;
        let from = size.saturating_sub(FOOTER_TAIL_BYTES);
        let mut tail = self.range(&key, from..size).await?;
        let info_len = tail
            .last_chunk::<4>()
            .map(|b| u64::from(u32::from_le_bytes(*b)) + 4)
            .ok_or_else(|| xet_err("xorb footer", "object shorter than its length field"))?;
        if info_len > tail.len() as u64 {
            let head = self.range(&key, size.saturating_sub(info_len)..from).await?;
            let mut joined = BytesMut::with_capacity(head.len() + tail.len());
            joined.extend_from_slice(&head);
            joined.extend_from_slice(&tail);
            tail = joined.freeze();
        }
        let footer = XorbObject::deserialize(&mut Cursor::new(tail.as_ref()))
            .map_err(|e| xet_err("xorb footer", e))?;
        let footer = Arc::new(footer);
        self.footers.insert(*hash, Arc::clone(&footer));
        Ok(footer)
    }

    async fn range(&self, key: &str, range: Range<u64>) -> Result<Bytes> {
        let opts = GetOptions { range: Some(range), ..GetOptions::default() };
        let got = self.store.get(key, opts).await?.bytes().await?;
        got.map(|(_, b)| b).ok_or_else(|| xet_err("read", "unconditional GET returned not-modified"))
    }
}

/// Point every term still under the marker hash at the xorb just written.
fn bind_marker(terms: &mut [FileDataSequenceEntry], hash: MerkleHash) {
    for t in terms.iter_mut().filter(|t| t.xorb_hash == MerkleHash::marker()) {
        t.xorb_hash = hash;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trajectory(turns: usize, step_bytes: usize) -> Bytes {
        let mut s = String::from("{\"schema_version\":\"1.8\",\"steps\":[");
        for t in 0..turns {
            use std::fmt::Write as _;
            let _ = write!(
                s,
                "{{\"step_id\":{t},\"source\":\"agent\",\"reasoning_content\":\"{}\"}},",
                "reasoning ".repeat(step_bytes / 10)
            );
        }
        s.push_str("],\"final_metrics\":{}}");
        Bytes::from(s)
    }

    fn mem() -> Xet {
        Xet::new(Arc::new(walgit_store::memory::MemoryStore::new()))
    }

    #[tokio::test]
    async fn round_trips_an_object() {
        let xet = mem();
        let data = trajectory(40, 512);
        let h = xet.store_object(data.clone()).await.expect("store").file_hash;
        assert_eq!(xet.load_object(&h).await.expect("load"), Some(data));
    }

    #[tokio::test]
    async fn missing_object_is_none() {
        let xet = mem();
        let h = xet_core_structures::merklehash::compute_data_hash(b"absent");
        assert_eq!(xet.load_object(&h).await.expect("load"), None);
    }

    #[tokio::test]
    async fn a_fresh_instance_reads_what_another_wrote() {
        let store: DynStore = Arc::new(walgit_store::memory::MemoryStore::new());
        let data = trajectory(4000, 512);
        let h = Xet::new(Arc::clone(&store)).store_object(data.clone()).await.expect("store").file_hash;
        // Nothing cached: the shard and the footer come from the store.
        assert_eq!(Xet::new(store).load_object(&h).await.expect("load"), Some(data));
    }

    #[tokio::test]
    async fn an_appended_turn_stores_only_the_tail() {
        let xet = mem();
        // ~2 MiB, so the file spans tens of chunks. Below that there are too few
        // dedup units for an append to save much -- see the small-file test.
        let v1 = trajectory(4000, 512);
        let v2 = trajectory(4001, 512);

        let first = xet.store_object(v1.clone()).await.expect("v1");
        assert_eq!(first.novel_bytes, first.logical_bytes, "nothing is shared yet");
        assert_eq!(first.terms, 1);

        let second = xet.store_object(v2.clone()).await.expect("v2");
        assert!(
            second.novel_bytes * 10 < second.logical_bytes,
            "appending one turn to {} bytes stored {} novel across {} chunks",
            second.logical_bytes,
            second.novel_bytes,
            second.chunks
        );
        assert_eq!(second.terms, 2, "the shared prefix, then the new tail");

        assert_eq!(xet.load_object(&first.file_hash).await.unwrap(), Some(v1));
        assert_eq!(xet.load_object(&second.file_hash).await.unwrap(), Some(v2));
    }

    /// A file of only a few chunks gains little: the chunk is the dedup unit, so
    /// an append rewrites a large fraction of them. Pinned so the limit is a
    /// recorded property rather than a surprise.
    #[tokio::test]
    async fn a_small_object_dedups_poorly() {
        let xet = mem();
        let v1 = trajectory(400, 512);
        let v2 = trajectory(401, 512);
        xet.store_object(v1).await.expect("v1");
        let second = xet.store_object(v2.clone()).await.expect("v2");
        assert!(second.chunks < 8, "expected a handful of chunks, got {}", second.chunks);
        assert!(
            second.novel_bytes * 2 > second.logical_bytes / 2,
            "a few-chunk file should still store a large fraction"
        );
        assert_eq!(xet.load_object(&second.file_hash).await.unwrap(), Some(v2));
    }

    #[tokio::test]
    async fn identical_content_adds_no_bytes() {
        let xet = mem();
        let data = trajectory(200, 512);
        let first = xet.store_object(data.clone()).await.expect("first");
        let again = xet.store_object(data).await.expect("second");
        assert_eq!(again.file_hash, first.file_hash);
        assert_eq!(again.novel_bytes, 0);
        assert_eq!(again.novel_chunks, 0);
    }

    /// Conformance against the Xet reference set (`xet-team/xet-spec-reference-files`).
    /// Runs only when `XET_REFERENCE_DIR` points at a checkout of it.
    #[tokio::test]
    async fn matches_the_xet_reference_files() {
        use std::path::PathBuf;

        use xet_core_structures::merklehash::xorb_hash;
        use xet_core_structures::xorb_object::reconstruct_xorb_with_footer;

        let Ok(dir) = std::env::var("XET_REFERENCE_DIR") else {
            return;
        };
        let dir = PathBuf::from(dir);
        let name = "Electric_Vehicle_Population_Data_20250917.csv";
        let read = |suffix: &str| std::fs::read(dir.join(format!("{name}{suffix}"))).expect(suffix);
        let read_hash = |suffix: &str| {
            MerkleHash::from_hex(String::from_utf8(read(suffix)).unwrap().trim()).expect(suffix)
        };

        let data = Bytes::from(read(""));
        let chunks = chunk_all(&data);
        let pairs: Vec<(MerkleHash, u64)> =
            chunks.iter().map(|c| (c.hash, c.data.len() as u64)).collect();

        let expected: Vec<(MerkleHash, u64)> = String::from_utf8(read(".chunks"))
            .unwrap()
            .lines()
            .map(|l| {
                let (h, n) = l.split_once(' ').unwrap();
                (MerkleHash::from_hex(h).unwrap(), n.parse().unwrap())
            })
            .collect();
        assert_eq!(pairs.len(), expected.len(), "chunk count");
        for (i, (got, want)) in pairs.iter().zip(&expected).enumerate() {
            assert_eq!(got.1, want.1, "chunk {i} length");
            assert_eq!(got.0, want.0, "chunk {i} hash");
        }

        assert_eq!(file_hash(&pairs), read_hash(".xet-file-hash"));
        let xorb = read_hash(".xet-xorb-hash");
        assert_eq!(xorb_hash(&pairs), xorb);
        assert_eq!(RawXorbData::from_chunks(&chunks, vec![chunks.len()]).hash(), xorb);

        // The reference xorb is as a client uploads it, without a footer. The
        // footer rebuilt from its chunks reads it back through the same
        // footer-then-range path `load_object` uses.
        let uploaded = std::fs::read(dir.join(format!("{}.xorb", xorb.hex()))).expect("xorb");
        // The published file ends four bytes into a footer ident.
        let uploaded = uploaded.strip_suffix(b"XETB").unwrap_or(&uploaded);
        let mut stored = Vec::with_capacity(uploaded.len());
        let (footer, hash) = reconstruct_xorb_with_footer(&mut stored, uploaded).expect("footer");
        assert_eq!(hash, xorb);
        let (from, to) = footer.get_byte_offset(0, chunks.len() as u32).expect("range");
        let (bytes, offsets) =
            deserialize_chunks(&mut Cursor::new(&stored[from as usize..to as usize])).expect("chunks");
        assert_eq!(offsets.len(), chunks.len() + 1);
        assert_eq!(Bytes::from(bytes), data);

        // And a store call derives the same file id `git-xet` would.
        let xet = mem();
        let stats = xet.store_object(data.clone()).await.expect("store");
        assert_eq!(stats.file_hash, read_hash(".xet-file-hash"));
        assert_eq!(stats.terms, 1);
        assert_eq!(xet.load_object(&stats.file_hash).await.unwrap(), Some(data));
    }
}
