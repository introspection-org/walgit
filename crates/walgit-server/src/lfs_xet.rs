//! Content-defined chunking for LFS objects (`docs/XET.md`).
//!
//! An object is cut into content-defined chunks, novel chunks are packed into a
//! xorb, and the chunk-to-xorb mapping lives in one shard per repository. Two
//! objects that differ by a byte therefore share every chunk they have in common,
//! where whole-object storage keeps two complete copies.
//!
//! The shard is the index: `MetadataShard` carries its own chunk lookup, so the
//! dedup query is a seek into a stored object rather than a structure walgit
//! maintains. It is rewritten under a CAS on its own version, so two concurrent
//! writers cannot lose each other's entries.

use std::io::Cursor;

use bytes::{Bytes, BytesMut};
use walgit_store::{DynStore, ObjectStoreExt, PutBody, PutMode, StoreError, Version};
use xet_core_structures::merklehash::MerkleHash;
use xet_core_structures::metadata_shard::file_structs::{
    FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo,
};
use xet_core_structures::metadata_shard::shard_format::MDBShardInfo;
use xet_core_structures::metadata_shard::xorb_structs::{
    MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader,
};
use xet_core_structures::metadata_shard::shard_in_memory::MDBInMemoryShard;
use xet_core_structures::xorb_object::constants::TARGET_CHUNK_SIZE;
use xet_core_structures::xorb_object::{
    Chunk, RawXorbData, SerializedXorbObject, deserialize_chunks,
};
use xet_data::deduplication::Chunker;

/// Everything this module writes lives under the repository's own prefix.
const XET_DIR: &str = "lfs/xet/";
const SHARD_KEY: &str = "lfs/xet/shard";
const XORB_COMPRESSION: &str = "lz4";

type Result<T> = std::result::Result<T, StoreError>;

fn xorb_key(hash: &MerkleHash) -> String {
    let hex = hash.hex();
    let (aa, bb) = (&hex[..2], &hex[2..4]);
    format!("{XET_DIR}xorbs/{aa}/{bb}/{hex}")
}

/// What a store call did, for metrics and tests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StoreStats {
    pub logical_bytes: u64,
    pub novel_bytes: u64,
    pub chunks: usize,
    pub novel_chunks: usize,
}

fn chunk_all(data: &Bytes) -> Vec<Chunk> {
    let mut chunker = Chunker::new(*TARGET_CHUNK_SIZE);
    let mut out = chunker.next_block_bytes(data, true);
    if let Some(c) = chunker.finish() {
        out.push(c);
    }
    out
}

async fn load_shard(store: &DynStore) -> Result<(MDBInMemoryShard, Option<Version>)> {
    match store.get_bytes(SHARD_KEY).await? {
        Some((meta, bytes)) => {
            let mut cursor = Cursor::new(bytes.as_ref());
            let shard = MDBInMemoryShard::from_reader(&mut cursor)
                .map_err(|e| StoreError::other(anyhow::anyhow!("xet: shard read: {e}")))?;
            Ok((shard, Some(meta.version)))
        }
        None => Ok((MDBInMemoryShard::default(), None)),
    }
}

async fn write_shard(store: &DynStore, shard: &MDBInMemoryShard, at: Option<Version>) -> Result<()> {
    let mut buf = Vec::new();
    MDBShardInfo::serialize_from(&mut buf, shard, None)
        .map_err(|e| StoreError::other(anyhow::anyhow!("xet: shard write: {e}")))?;
    let mode = match at {
        Some(v) => PutMode::Update(v),
        None => PutMode::Create,
    };
    store.put(SHARD_KEY, PutBody::Bytes(buf.into()), mode.into()).await?;
    Ok(())
}

/// Chunk `data`, store the chunks this repository has not seen, and record the
/// reconstruction under `file_hash`.
pub async fn store_object(store: &DynStore, file_hash: MerkleHash, data: Bytes) -> Result<StoreStats> {
    let chunks = chunk_all(&data);
    let (mut shard, version) = load_shard(store).await?;

    let mut stats = StoreStats {
        logical_bytes: data.len() as u64,
        chunks: chunks.len(),
        ..Default::default()
    };

    // Chunks already in the shard resolve to an existing xorb; the rest go into
    // one new xorb, so a file contributes at most one xorb per store call.
    let mut segments: Vec<FileDataSequenceEntry> = Vec::new();
    let mut novel: Vec<Chunk> = Vec::new();
    let mut novel_index: Vec<usize> = Vec::new();

    for (i, c) in chunks.iter().enumerate() {
        if let Some((_, entry)) = shard.chunk_hash_dedup_query(&[c.hash]) {
            segments.push(entry);
        } else {
            novel_index.push(i);
            novel.push(c.clone());
        }
    }

    if !novel.is_empty() {
        let xorb = RawXorbData::from_chunks(&novel, vec![novel.len()]);
        let hash = xorb.hash();
        // Chunks are compressed inside the xorb, so the stored bytes are smaller
        // than the novel bytes this reports.
        let serialized = SerializedXorbObject::from_xorb(xorb, false, XORB_COMPRESSION, 0)
            .map_err(|e| StoreError::other(anyhow::anyhow!("xet: xorb write: {e}")))?;
        let bytes = serialized.serialized_data;
        stats.novel_bytes = novel.iter().map(|c| c.data.len() as u64).sum();
        stats.novel_chunks = novel.len();
        let novel_len = novel.len();
        store
            .put(&xorb_key(&hash), PutBody::Bytes(bytes.into()), PutMode::Create.into())
            .await
            .map(|_| ())
            // A concurrent writer storing the identical xorb is not a conflict:
            // the hash is the content, so whoever won wrote the same bytes.
            .or_else(|e| match e {
                StoreError::PreconditionFailed { .. } => Ok(()),
                other => Err(other),
            })?;

        // The chunk-to-xorb lookup lives in the xorb block, so without this the
        // shard records the file but dedups nothing on the next store.
        let mut offset = 0u32;
        let entries: Vec<XorbChunkSequenceEntry> = novel
            .iter()
            .map(|c| {
                let e = XorbChunkSequenceEntry::new(c.hash, c.data.len(), offset);
                offset += c.data.len() as u32;
                e
            })
            .collect();
        shard
            .add_xorb_block(MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(hash, entries.len(), offset),
                chunks: entries,
            })
            .map_err(|e| StoreError::other(anyhow::anyhow!("xet: shard xorb: {e}")))?;

        let unpacked = stats.novel_bytes as usize;
        segments.push(FileDataSequenceEntry::new(hash, unpacked, 0usize, novel_len));
    }

    let info = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(file_hash, segments.len(), false, false),
        segments,
        verification: Vec::new(),
        metadata_ext: None,
    };
    shard
        .add_file_reconstruction_info(info)
        .map_err(|e| StoreError::other(anyhow::anyhow!("xet: shard add: {e}")))?;
    write_shard(store, &shard, version).await?;

    Ok(stats)
}

/// Rebuild an object from its chunks. `Ok(None)` when the shard has no record of it.
pub async fn load_object(store: &DynStore, file_hash: &MerkleHash) -> Result<Option<Bytes>> {
    let (shard, _) = load_shard(store).await?;
    let Some(info) = shard.get_file_reconstruction_info(file_hash) else {
        return Ok(None);
    };

    let mut out = BytesMut::new();
    for seg in &info.segments {
        let (_, bytes) = store
            .get_bytes(&xorb_key(&seg.xorb_hash))
            .await?
            .ok_or_else(|| StoreError::NotFound { key: xorb_key(&seg.xorb_hash) })?;
        let mut cursor = Cursor::new(bytes.as_ref());
        let (data, offsets) = deserialize_chunks(&mut cursor)
            .map_err(|e| StoreError::other(anyhow::anyhow!("xet: xorb read: {e}")))?;
        // offsets carries a leading 0, so chunk i spans offsets[i]..offsets[i + 1].
        let (a, b) = (seg.chunk_index_start as usize, seg.chunk_index_end as usize);
        let (from, to) = (offsets[a] as usize, offsets[b] as usize);
        out.extend_from_slice(&data[from..to]);
    }
    Ok(Some(out.freeze()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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

    fn mem() -> DynStore {
        Arc::new(walgit_store::memory::MemoryStore::new())
    }

    fn hash_of(b: &Bytes) -> MerkleHash {
        xet_core_structures::merklehash::compute_data_hash(b)
    }

    #[tokio::test]
    async fn round_trips_an_object() {
        let store = mem();
        let data = trajectory(40, 512);
        let h = hash_of(&data);
        store_object(&store, h, data.clone()).await.expect("store");
        assert_eq!(load_object(&store, &h).await.expect("load"), Some(data));
    }

    #[tokio::test]
    async fn missing_object_is_none() {
        let store = mem();
        let h = hash_of(&Bytes::from_static(b"absent"));
        assert_eq!(load_object(&store, &h).await.expect("load"), None);
    }

    #[tokio::test]
    async fn an_appended_turn_stores_only_the_tail() {
        let store = mem();
        // ~2 MiB, so the file spans tens of chunks. Below that there are too few
        // dedup units for an append to save much -- see the small-file test.
        let v1 = trajectory(4000, 512);
        let v2 = trajectory(4001, 512);

        let first = store_object(&store, hash_of(&v1), v1.clone()).await.expect("v1");
        assert_eq!(first.novel_bytes, first.logical_bytes, "nothing is shared yet");

        let second = store_object(&store, hash_of(&v2), v2.clone()).await.expect("v2");
        assert!(
            second.novel_bytes * 10 < second.logical_bytes,
            "appending one turn to {} bytes stored {} novel across {} chunks",
            second.logical_bytes,
            second.novel_bytes,
            second.chunks
        );

        // Both versions still reconstruct exactly.
        assert_eq!(load_object(&store, &hash_of(&v1)).await.unwrap(), Some(v1));
        assert_eq!(load_object(&store, &hash_of(&v2)).await.unwrap(), Some(v2));
    }

    /// A file of only a few chunks gains little: the chunk is the dedup unit, so
    /// an append rewrites a large fraction of them. Pinned so the limit is a
    /// recorded property rather than a surprise.
    #[tokio::test]
    async fn a_small_object_dedups_poorly() {
        let store = mem();
        let v1 = trajectory(400, 512);
        let v2 = trajectory(401, 512);
        store_object(&store, hash_of(&v1), v1).await.expect("v1");
        let second = store_object(&store, hash_of(&v2), v2.clone()).await.expect("v2");
        assert!(second.chunks < 8, "expected a handful of chunks, got {}", second.chunks);
        assert!(
            second.novel_bytes * 2 > second.logical_bytes / 2,
            "a few-chunk file should still store a large fraction"
        );
        assert_eq!(load_object(&store, &hash_of(&v2)).await.unwrap(), Some(v2));
    }

    #[tokio::test]
    async fn identical_content_adds_no_bytes() {
        let store = mem();
        let data = trajectory(200, 512);
        store_object(&store, hash_of(&data), data.clone()).await.expect("first");
        let again = store_object(&store, hash_of(&data), data).await.expect("second");
        assert_eq!(again.novel_bytes, 0);
        assert_eq!(again.novel_chunks, 0);
    }
}
