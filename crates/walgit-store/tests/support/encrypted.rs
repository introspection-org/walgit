//! An example [`ChunkCodec`], showing what a real one has to do.
//!
//! Deliberately the simplest thing that is correct: AES-256-GCM under a key the
//! caller supplies. It is *not* a shippable adapter — the key is handed in
//! whole rather than wrapped by a KMS, so there is no key custody, rotation or
//! per-tenant isolation here at all.
//!
//! A production codec differs in exactly two places, both marked below:
//!
//! * `begin_write` mints a data key and wraps it through a key service, returning
//!   the wrapped blob for the container header. One call per object, never per
//!   chunk — the container's shape is what makes that natural.
//! * `seal`/`open` receive the whole batch, so a codec backed by a network
//!   service issues **one** request for N chunks. Unwrapping the header key is
//!   likewise once per object, and cacheable by `(object, key version)` because
//!   the header is immutable.
//!
//! The container binds object identity, chunk position, chunk count, total
//! length and this header into `chunk.aad`, so a codec needs no AAD scheme of
//! its own — and must not invent one.

use aes_gcm::Aes256Gcm;
use aes_gcm::aead::{AeadInOut, KeyInit};
use walgit_store::encrypted::{Chunk, ChunkCodec};

const TAG: usize = 16;

pub struct ExampleAesGcmCodec {
    key: [u8; 32],
}

impl ExampleAesGcmCodec {
    pub fn new(key: [u8; 32]) -> Self {
        ExampleAesGcmCodec { key }
    }
}

/// A counter nonce is safe only because a real codec mints a fresh key per
/// object, so no (key, nonce) pair ever repeats. This example reuses one key
/// across objects, which is precisely why it is an example: the AAD still
/// separates them, but nonce reuse across objects under one GCM key is a real
/// weakness and a production codec must not copy this shortcut.
fn nonce_for(index: u64) -> [u8; 12] {
    let mut nb = [0_u8; 12];
    nb[4..].copy_from_slice(&index.to_be_bytes());
    nb
}

#[async_trait::async_trait]
impl ChunkCodec for ExampleAesGcmCodec {
    fn overhead_per_chunk(&self) -> usize {
        TAG
    }

    async fn begin_write(&self, _object: &str) -> anyhow::Result<Vec<u8>> {
        // A production codec mints a DEK here and returns it wrapped by a key
        // service — one call per object. Nothing to store for a fixed key.
        Ok(Vec::new())
    }

    async fn seal(
        &self,
        _object: &str,
        _codec_header: &[u8],
        batch: &mut [Chunk<'_>],
    ) -> anyhow::Result<()> {
        // A remote codec sends the whole batch in one request here.
        let cipher = Aes256Gcm::new((&self.key).into());
        for chunk in batch.iter_mut() {
            let tag = cipher
                .encrypt_inout_detached(
                    &nonce_for(chunk.index).into(),
                    chunk.aad,
                    (&mut chunk.data[..]).into(),
                )
                .map_err(|_| anyhow::anyhow!("seal chunk {}", chunk.index))?;
            chunk.tag.copy_from_slice(&tag);
        }
        Ok(())
    }

    async fn open(
        &self,
        _object: &str,
        _codec_header: &[u8],
        batch: &mut [Chunk<'_>],
    ) -> anyhow::Result<()> {
        let cipher = Aes256Gcm::new((&self.key).into());
        for chunk in batch.iter_mut() {
            let mut tag = [0_u8; TAG];
            tag.copy_from_slice(chunk.tag);
            cipher
                .decrypt_inout_detached(
                    &nonce_for(chunk.index).into(),
                    chunk.aad,
                    (&mut chunk.data[..]).into(),
                    &tag.into(),
                )
                .map_err(|_| anyhow::anyhow!("chunk {} failed authentication", chunk.index))?;
        }
        Ok(())
    }
}
