# Xet as an LFS storage backend — evaluation

Context: **evaluation, not an implementation.** Whether content-defined chunking
([huggingface/xet-core](https://github.com/huggingface/xet-core)) is worth adding under `docs/LFS.md`'s
storage layer. Read with `LFS.md` §1, which is what this would sit beneath.

Today an LFS object is stored whole, sha256-addressed and immutable, at `lfs/objects/<aa>/<bb>/<oid>`.
Two versions of one file that differ by a byte are two complete copies. Xet splits content on
content-defined boundaries (~64 KiB target), hashes each chunk, and stores only chunks it has not seen.

## 1. Measured (`bench/xet-eval`)

Chunker: `xet_data::deduplication::Chunker` at the shipped `TARGET_CHUNK_SIZE` of **64 KiB**.
Base is pseudorandom, seeded, so runs are reproducible; "walgit" is the bytes stored for v2 today,
"xet" the bytes whose chunk hash was not already present.

| scenario (bytes stored for v2) | 1 MiB | 16 MiB | 128 MiB |
|---|---:|---:|---:|
| identical re-upload | 0% / 0% | 0% / 0% | 0% / 0% |
| 1 KiB edit mid-file | 100% / 9.2% | 100% / 0.3% | 100% / 0.1% |
| append 1% | 100% / 4.5% | 100% / 1.7% | 100% / 1.0% |
| prepend 1 KiB (shifts every offset) | 100% / 1.7% | 100% / 0.1% | 100% / 0.0% |
| sibling checkpoint (10% differs) | 100% / 11.7% | 100% / 10.4% | 100% / 10.0% |
| unrelated file | 100% / 100% | 100% / 100% | 100% / 100% |

The prepend row is the one fixed-size blocking could not do: content-defined boundaries resynchronise
after a shift, so a 1 KiB insert at offset 0 costs ~one chunk rather than the whole file.

**Cost is not what it looks like.** Chunking is *faster* than the sha256 walgit already computes:

| | 1 MiB | 16 MiB | 128 MiB |
|---|---:|---:|---:|
| sha256 (needed either way) | 200 MiB/s | 202 MiB/s | 201 MiB/s |
| xet chunk + hash alone | 1299 MiB/s | 1491 MiB/s | 1518 MiB/s |
| both, as they would actually run | 173 MiB/s | 178 MiB/s | 177 MiB/s |

LFS oids are sha256 by protocol, so Xet is **additive, never a replacement**: the real cost is the
**+13–15% CPU** of the combined column, not a 7× anything. Chunk metadata is ~0.075% of file size
(~48 B per chunk).

## 2. What this does and does not establish

It measures **dedup potential and chunker throughput**, nothing else. Not measured: xorb packing and
shard metadata as actually serialised, CAS lookup latency, network transfer, or any walgit integration.

Pseudorandom bases are the honest choice for *cross-version* dedup (a binary blob with an edit) but say
nothing about *within-file* redundancy, where real model weights andmedia would differ.

## 3. Open questions before any implementation

- **`xet-data` is not separable from the Hugging Face client.** Its features delegate to `xet-client/*`
  (`rustls-tls`, `native-tls`, `fd-track`), so depending on it pulls the CAS client and `xet-runtime`.
  Either that tree is acceptable behind a feature, or the chunker is reimplemented against
  `xet-core-structures` alone.
- **Where chunks live.** Xorbs and shards are objects like any other, so they can sit under the
  repository prefix, but that is a second address space beside `lfs/objects/` with its own lifetime.
- **The oid mapping.** The batch API stays sha256-addressed, so a reconstruction record must map oid →
  chunk list, and `GET objects/<oid>` reassembles.
- **Deletion.** Whole objects are independent today. Shared chunks are not: removing a repository or an
  object needs refcounting or a mark-and-sweep that `LFS.md` currently has no equivalent of.
- **Where it does nothing.** Unrelated files dedup at 0% and still pay the CPU and metadata. A repository
  of independent media gains nothing; one of model checkpoints or datasets gains most of the file.

## 4. If it proceeds

Behind its own `--features lfs-xet`, off by default, with `lfs.storage = "whole" | "xet"` per repository
so it can be adopted per workload rather than fleet-wide. `git-xet` is the LFS-compatible client for
interoperability testing.

Reproduce: `cd bench/xet-eval && cargo run --release`. The crate is excluded from the workspace, so a
normal build never resolves the Xet stack.
