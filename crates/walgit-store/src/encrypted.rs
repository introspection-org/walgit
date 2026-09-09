//! A store decorator that transforms object bytes through a pluggable codec.
//!
//! This module contains **no cryptography**. It owns the container format, the
//! plaintext-range arithmetic and the round-trip discipline; a [`ChunkCodec`]
//! owns key management and the cipher. A downstream build can therefore supply a
//! codec backed by a KMS or a remote sealing service without walgit taking a
//! crypto dependency or an opinion about key custody.
//!
//! # Container v1
//!
//! Fixed-size chunks, so a plaintext offset maps to a ciphertext offset by
//! arithmetic rather than an index lookup:
//!
//! ```text
//! header | chunk 0 (CHUNK + tag_len) | chunk 1 | ... | tail (rem + tag_len)
//! ```
//!
//! Header, big-endian, `32 + codec_header_len` bytes:
//!
//! ```text
//! off  len  field
//!   0    8  magic "WGENCv1\0"
//!   8    2  container_version = 1
//!  10    4  chunk_size
//!  14    8  plaintext_len
//!  22    4  chunk_count
//!  26    2  tag_len              bytes the codec appends per chunk
//!  28    4  codec_header_len
//!  32    N  codec_header         opaque to the container: wrapped key, key id, ...
//! ```
//!
//! ## The header carries no MAC of its own
//!
//! It cannot: authenticating it would require a cipher, and this module has
//! none. Instead **the entire header is bound into every chunk's AAD**. Forging
//! `plaintext_len`, `chunk_count` or the wrapped key therefore fails the tag
//! check on every chunk rather than being detected by a separate step that could
//! be skipped. Truncating the object drops `chunk_count` out of agreement with
//! the AAD the remaining chunks were sealed under, so a short read fails too.
//!
//! ## AAD is a defined structure, not concatenated bytes
//!
//! Every variable-length field is length-prefixed and the order is fixed. Without
//! that, `object="a"` with header `"bc"` and `object="ab"` with header `"c"`
//! produce identical associated data, and a chunk can be replayed across objects
//! whose names and headers happen to concatenate alike. A codec passes these
//! bytes to its AEAD unchanged and must not reinterpret, truncate or extend them.
//!
//! ```text
//! "wgenc\x1f" | u16 container_version | u64 chunk_index      | u64 chunk_count
//!             | u64 plaintext_len     | u64 chunk_size       | u64 tag_len
//!             | u64 codec_header_len  | codec_header
//!             | u64 object_key_len    | object_key
//! ```
//!
//! # Why the chunk size is not a free parameter
//!
//! [`CHUNK`] equals `walgit_wal::remote::BLOCK_SIZE`. `read_at` floors every pack
//! read to that boundary, so at equality one plaintext block is exactly one
//! ciphertext chunk and a pack range read amplifies by nothing. At any other
//! value every block read straddles chunks and pays for bytes it discards.
//!
//! # Two properties a codec must preserve
//!
//! **Batching.** [`ChunkCodec::seal`] and [`ChunkCodec::open`] take a *slice* of
//! chunks. A range read spanning N blocks is one call, so a codec backed by a
//! network service makes one round trip rather than N — the same reason the
//! telemetry path seals rows in batches rather than one at a time.
//!
//! **In-place transformation.** [`Chunk::data`] is a mutable slice the codec
//! transforms in place, with the tag written to a separate [`Chunk::tag`].
//! Returning a fresh buffer per chunk measured ~2.5x slower; the signature is
//! what makes the slow shape awkward and the fast one natural.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use bytes::Bytes;
use futures::StreamExt;

use crate::{
    BoxStream, ByteStream, DynStore, GetOptions, GetResult, ObjectMeta, ObjectStore, PutBody,
    PutOptions, Result, StoreError, Version,
};

/// Plaintext bytes per chunk. Must equal `walgit_wal::remote::BLOCK_SIZE`.
pub const CHUNK: usize = 1024 * 1024;

const MAGIC: &[u8; 8] = b"WGENCv1\0";
const CONTAINER_VERSION: u16 = 1;
/// Bytes before `codec_header`.
const FIXED_HEADER: usize = 32;

/// One chunk presented to a codec: transformed in place, tag written alongside.
pub struct Chunk<'a> {
    /// Position within the object. Codecs deriving a nonce from position must
    /// treat this as sealed identity, not a hint — it is already inside `aad`.
    pub index: u64,
    /// Associated data, laid out as documented on this module. Pass to the AEAD
    /// unchanged.
    pub aad: &'a [u8],
    /// Transformed in place; the transformation does not change its length.
    pub data: &'a mut [u8],
    /// Authentication tag, exactly `overhead_per_chunk()` bytes.
    pub tag: &'a mut [u8],
}

/// Seals and opens object chunks. Implementations own key management entirely;
/// the container never sees a key.
#[async_trait::async_trait]
pub trait ChunkCodec: Send + Sync + 'static {
    /// Bytes appended per chunk (an authentication tag). Fixed for the lifetime
    /// of a codec: the container computes ciphertext offsets from it, so a codec
    /// that varied this would silently misaddress every existing object.
    fn overhead_per_chunk(&self) -> usize;

    /// Begin writing `object`; returns bytes the container stores in the header
    /// so the object can be reopened — typically a wrapped data key. Called once
    /// per object write, never per chunk: a codec wrapping a key through a remote
    /// service makes one call here, not one per megabyte.
    async fn begin_write(&self, object: &str) -> anyhow::Result<Vec<u8>>;

    /// Seal a batch in place, writing each chunk's tag.
    async fn seal(
        &self,
        object: &str,
        codec_header: &[u8],
        batch: &mut [Chunk<'_>],
    ) -> anyhow::Result<()>;

    /// Open a batch in place, verifying each tag. Must fail rather than yield
    /// unauthenticated bytes.
    async fn open(
        &self,
        object: &str,
        codec_header: &[u8],
        batch: &mut [Chunk<'_>],
    ) -> anyhow::Result<()>;
}

fn chunk_count(plaintext_len: usize) -> usize {
    plaintext_len.div_ceil(CHUNK).max(1)
}

/// The header fields the container and every AAD agree on.
#[derive(Clone)]
struct Header {
    plaintext_len: usize,
    chunk_count: usize,
    tag_len: usize,
    codec: Arc<Vec<u8>>,
}

impl Header {
    fn encode(&self) -> Result<Vec<u8>> {
        let field =
            |what: &str| StoreError::other(anyhow::anyhow!("wgenc header: {what} too large"));
        let mut h = Vec::with_capacity(FIXED_HEADER + self.codec.len());
        h.extend_from_slice(MAGIC);
        h.extend_from_slice(&CONTAINER_VERSION.to_be_bytes());
        h.extend_from_slice(
            &u32::try_from(CHUNK)
                .map_err(|_| field("chunk_size"))?
                .to_be_bytes(),
        );
        h.extend_from_slice(
            &u64::try_from(self.plaintext_len)
                .map_err(|_| field("plaintext_len"))?
                .to_be_bytes(),
        );
        h.extend_from_slice(
            &u32::try_from(self.chunk_count)
                .map_err(|_| field("chunk_count"))?
                .to_be_bytes(),
        );
        h.extend_from_slice(
            &u16::try_from(self.tag_len)
                .map_err(|_| field("tag_len"))?
                .to_be_bytes(),
        );
        h.extend_from_slice(
            &u32::try_from(self.codec.len())
                .map_err(|_| field("codec_header_len"))?
                .to_be_bytes(),
        );
        h.extend_from_slice(&self.codec);
        debug_assert_eq!(h.len(), FIXED_HEADER + self.codec.len());
        Ok(h)
    }

    /// Parses the fixed part. `codec_header_len` tells the caller how much more to
    /// read, so a codec whose wrapped key varies in size needs no probe.
    fn decode_fixed(b: &[u8]) -> Result<(usize, usize, usize, usize)> {
        let bad = |m: &str| StoreError::other(anyhow::anyhow!("wgenc container: {m}"));
        let at = |r: std::ops::Range<usize>| b.get(r).ok_or_else(|| bad("short header"));
        if at(0..8)? != MAGIC {
            return Err(bad("bad magic"));
        }
        let version = u16::from_be_bytes(at(8..10)?.try_into().map_err(|_| bad("version"))?);
        if version != CONTAINER_VERSION {
            return Err(bad(&format!("unsupported version {version}")));
        }
        let chunk_size = u32::from_be_bytes(at(10..14)?.try_into().map_err(|_| bad("chunk_size"))?);
        if chunk_size as usize != CHUNK {
            // Reading with a different block size would silently misalign every
            // range read, so refuse rather than serve shifted bytes.
            return Err(bad(&format!(
                "chunk_size {chunk_size} != this build's {CHUNK}"
            )));
        }
        let plaintext_len = usize::try_from(u64::from_be_bytes(
            at(14..22)?.try_into().map_err(|_| bad("plaintext_len"))?,
        ))
        .map_err(|_| bad("plaintext_len exceeds this platform's addressable range"))?;
        let count =
            u32::from_be_bytes(at(22..26)?.try_into().map_err(|_| bad("chunk_count"))?) as usize;
        let tag_len =
            u16::from_be_bytes(at(26..28)?.try_into().map_err(|_| bad("tag_len"))?) as usize;
        let codec_len = u32::from_be_bytes(
            at(28..32)?
                .try_into()
                .map_err(|_| bad("codec_header_len"))?,
        ) as usize;
        if count != chunk_count(plaintext_len) {
            return Err(bad("chunk_count disagrees with plaintext_len"));
        }
        Ok((plaintext_len, count, tag_len, codec_len))
    }
}

/// Associated data for one chunk. Every variable-length field is length-prefixed;
/// see the module docs for why that is load-bearing rather than tidy.
fn aad_for(object: &str, index: usize, header: &Header) -> Vec<u8> {
    let mut a = Vec::with_capacity(64 + header.codec.len() + object.len());
    a.extend_from_slice(b"wgenc\x1f");
    a.extend_from_slice(&CONTAINER_VERSION.to_be_bytes());
    a.extend_from_slice(&(index as u64).to_be_bytes());
    a.extend_from_slice(&(header.chunk_count as u64).to_be_bytes());
    a.extend_from_slice(&(header.plaintext_len as u64).to_be_bytes());
    a.extend_from_slice(&(CHUNK as u64).to_be_bytes());
    a.extend_from_slice(&(header.tag_len as u64).to_be_bytes());
    a.extend_from_slice(&(header.codec.len() as u64).to_be_bytes());
    a.extend_from_slice(&header.codec);
    a.extend_from_slice(&(object.len() as u64).to_be_bytes());
    a.extend_from_slice(object.as_bytes());
    a
}

/// Wraps a store so objects are sealed by `codec` before they reach it.
///
/// `signed_get_url`, `accel_target` and `compose` keep the trait's defaults —
/// unavailable and unsupported. None can serve authorized plaintext, and
/// concatenating containers is not concatenating the objects inside them.
pub struct EncryptedStore<C: ChunkCodec> {
    inner: DynStore,
    codec: C,
    /// `(key, version) -> header`. Objects under `wal/` are immutable, so caching
    /// is sound; without it every ranged read costs two store round trips.
    headers: Mutex<HashMap<(String, String), Header>>,
}

impl<C: ChunkCodec> EncryptedStore<C> {
    pub fn new(inner: DynStore, codec: C) -> Arc<Self> {
        Arc::new(EncryptedStore {
            inner,
            codec,
            headers: Mutex::new(HashMap::new()),
        })
    }

    fn header_len(h: &Header) -> usize {
        FIXED_HEADER + h.codec.len()
    }
    fn ct_off(h: &Header, i: usize) -> usize {
        Self::header_len(h) + i * (CHUNK + h.tag_len)
    }

    /// Reads the header, honouring the caller's conditions so `NotModified` and
    /// `PreconditionFailed` reach the caller unchanged. Returns the header and the
    /// backing version every subsequent read of this object is pinned to.
    async fn header(
        &self,
        key: &str,
        opts: &GetOptions,
    ) -> Result<std::result::Result<(Header, Version), Version>> {
        let probe = self
            .inner
            .get(
                key,
                GetOptions {
                    if_none_match: opts.if_none_match.clone(),
                    if_match: opts.if_match.clone(),
                    // One read that covers the fixed header plus a codec header of
                    // any plausible size; a larger one costs a second read, which is
                    // correct rather than fatal.
                    range: Some(0..(FIXED_HEADER + 512) as u64),
                },
            )
            .await?;
        let version = probe.version().clone();
        if matches!(probe, GetResult::NotModified { .. }) {
            return Ok(Err(version));
        }
        let cache_key = (key.to_owned(), version.as_str().to_owned());
        if let Some(h) = self.headers.lock().get(&cache_key) {
            return Ok(Ok((h.clone(), version)));
        }
        let (_, b) = probe
            .bytes()
            .await?
            .ok_or_else(|| StoreError::NotFound { key: key.into() })?;
        let (plaintext_len, count, tag_len, codec_len) = Header::decode_fixed(&b)?;
        if tag_len != self.codec.overhead_per_chunk() {
            return Err(StoreError::other(anyhow::anyhow!(
                "wgenc container: tag_len {tag_len} != this codec's {}",
                self.codec.overhead_per_chunk()
            )));
        }
        let codec_header = if let Some(slice) = b.get(FIXED_HEADER..FIXED_HEADER + codec_len) {
            slice.to_vec()
        } else {
            // Pinned to the same version, so the two reads cannot straddle generations.
            let r = self
                .inner
                .get(
                    key,
                    GetOptions {
                        if_match: Some(version.clone()),
                        range: Some(FIXED_HEADER as u64..(FIXED_HEADER + codec_len) as u64),
                        ..Default::default()
                    },
                )
                .await?;
            let (_, cb) = r
                .bytes()
                .await?
                .ok_or_else(|| StoreError::NotFound { key: key.into() })?;
            cb.to_vec()
        };
        let header = Header {
            plaintext_len,
            chunk_count: count,
            tag_len,
            codec: Arc::new(codec_header),
        };
        self.headers.lock().insert(cache_key, header.clone());
        Ok(Ok((header, version)))
    }
}

async fn collect(body: ByteStream) -> Result<Bytes> {
    let mut buf: Vec<u8> = Vec::new();
    let mut body = body;
    while let Some(chunk) = body.next().await {
        buf.extend_from_slice(&chunk?);
    }
    Ok(Bytes::from(buf))
}

#[async_trait::async_trait]
impl<C: ChunkCodec> ObjectStore for EncryptedStore<C> {
    fn backend(&self) -> &'static str {
        self.inner.backend()
    }

    async fn put(&self, key: &str, body: PutBody, opts: PutOptions) -> Result<ObjectMeta> {
        let plain = match body {
            PutBody::Bytes(b) => b,
            PutBody::Stream { stream, .. } => collect(stream).await?,
            PutBody::File(p) => Bytes::from(tokio::fs::read(p).await.map_err(StoreError::other)?),
        };
        let codec_header = self
            .codec
            .begin_write(key)
            .await
            .map_err(StoreError::other)?;
        let tag_len = self.codec.overhead_per_chunk();
        let count = chunk_count(plain.len());
        let header = Header {
            plaintext_len: plain.len(),
            chunk_count: count,
            tag_len,
            codec: Arc::new(codec_header),
        };

        // Lay the container out first, then seal every chunk in place in ONE codec
        // call: the batch is what lets a remote codec make a single round trip.
        let encoded = header.encode()?;
        let mut out = Vec::with_capacity(encoded.len() + plain.len() + count * tag_len);
        out.extend_from_slice(&encoded);
        for i in 0..count {
            let s = i * CHUNK;
            let e = ((i + 1) * CHUNK).min(plain.len());
            out.extend_from_slice(
                plain
                    .get(s..e)
                    .ok_or_else(|| StoreError::other(anyhow::anyhow!("chunk bounds")))?,
            );
            out.resize(out.len() + tag_len, 0);
        }
        let aads: Vec<Vec<u8>> = (0..count).map(|i| aad_for(key, i, &header)).collect();
        {
            let (_, mut rest) = out.split_at_mut(encoded.len());
            let mut batch = Vec::with_capacity(count);
            for (i, aad) in aads.iter().enumerate() {
                let body = CHUNK.min(header.plaintext_len - i * CHUNK);
                let (data, tail) = rest.split_at_mut(body);
                let (tag, tail) = tail.split_at_mut(tag_len);
                rest = tail;
                batch.push(Chunk {
                    index: i as u64,
                    aad,
                    data,
                    tag,
                });
            }
            self.codec
                .seal(key, &header.codec, &mut batch)
                .await
                .map_err(StoreError::other)?;
        }

        let mut meta = self
            .inner
            .put(key, PutBody::Bytes(Bytes::from(out)), opts)
            .await?;
        meta.size = header.plaintext_len as u64;
        Ok(meta)
    }

    async fn get(&self, key: &str, opts: GetOptions) -> Result<GetResult> {
        let (header, version) = match self.header(key, &opts).await? {
            Ok(v) => v,
            Err(version) => return Ok(GetResult::NotModified { version }),
        };
        let plen = header.plaintext_len;
        let requested = opts.range.clone().unwrap_or(0..plen as u64);
        let clamp = |v: u64| usize::try_from(v).unwrap_or(usize::MAX).min(plen);
        let start = clamp(requested.start);
        let end = clamp(requested.end);
        let meta = ObjectMeta {
            key: key.into(),
            size: plen as u64,
            version: version.clone(),
        };
        if start >= end {
            return Ok(GetResult::Object {
                meta,
                body: Box::pin(futures::stream::once(async { Ok(Bytes::new()) })),
            });
        }
        let (first, last) = (start / CHUNK, (end - 1) / CHUNK);
        let ct_start = Self::ct_off(&header, first);
        let ct_end = Self::ct_off(&header, last) + CHUNK.min(plen - last * CHUNK) + header.tag_len;
        // Pinned to the version the header came from: a multi-read range must not
        // straddle two generations of the object.
        let r = self
            .inner
            .get(
                key,
                GetOptions {
                    if_match: Some(version),
                    range: Some(ct_start as u64..ct_end as u64),
                    ..Default::default()
                },
            )
            .await?;
        let (_, ct) = r
            .bytes()
            .await?
            .ok_or_else(|| StoreError::NotFound { key: key.into() })?;

        // Compact tags out into one plaintext buffer, then open the whole span in a
        // single codec call. Two passes rather than four, and one round trip.
        let n = last - first + 1;
        let mut plainbuf: Vec<u8> = Vec::with_capacity(ct.len() - n * header.tag_len);
        let mut tags: Vec<u8> = Vec::with_capacity(n * header.tag_len);
        let mut bodies: Vec<usize> = Vec::with_capacity(n);
        let mut pos = 0usize;
        for i in first..=last {
            let body = CHUNK.min(plen - i * CHUNK);
            if pos + body + header.tag_len > ct.len() {
                return Err(StoreError::other(anyhow::anyhow!(
                    "wgenc container: truncated"
                )));
            }
            let oob = || StoreError::other(anyhow::anyhow!("wgenc container: truncated"));
            plainbuf.extend_from_slice(ct.get(pos..pos + body).ok_or_else(oob)?);
            tags.extend_from_slice(
                ct.get(pos + body..pos + body + header.tag_len)
                    .ok_or_else(oob)?,
            );
            bodies.push(body);
            pos += body + header.tag_len;
        }
        let aads: Vec<Vec<u8>> = (first..=last).map(|i| aad_for(key, i, &header)).collect();
        {
            let mut data_rest = plainbuf.as_mut_slice();
            let mut tag_rest = tags.as_mut_slice();
            let mut batch = Vec::with_capacity(n);
            for (slot, body) in bodies.iter().enumerate() {
                let (data, dt) = data_rest.split_at_mut(*body);
                let (tag, tt) = tag_rest.split_at_mut(header.tag_len);
                data_rest = dt;
                tag_rest = tt;
                batch.push(Chunk {
                    index: (first + slot) as u64,
                    aad: aads.get(slot).map_or(&[][..], Vec::as_slice),
                    data,
                    tag,
                });
            }
            self.codec
                .open(key, &header.codec, &mut batch)
                .await
                .map_err(StoreError::other)?;
        }
        let off = start - first * CHUNK;
        let slice = Bytes::from(plainbuf).slice(off..off + (end - start));
        Ok(GetResult::Object {
            meta,
            body: Box::pin(futures::stream::once(async move { Ok(slice) })),
        })
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        match self.header(key, &GetOptions::default()).await {
            Ok(Ok((h, version))) => Ok(Some(ObjectMeta {
                key: key.into(),
                size: h.plaintext_len as u64,
                version,
            })),
            Ok(Err(version)) => Ok(Some(ObjectMeta {
                key: key.into(),
                size: 0,
                version,
            })),
            Err(e) if e.is_not_found() => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn delete(&self, key: &str, if_version: Option<Version>) -> Result<()> {
        self.inner.delete(key, if_version).await
    }

    fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        // Sizes here are container lengths, not plaintext. Correcting them costs a
        // header read per entry unless the length is carried in object metadata —
        // an open question, and the reason this is not yet a shippable adapter.
        self.inner.list(prefix, start_after)
    }

    async fn list_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list_prefixes(prefix).await
    }
}
