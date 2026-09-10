# Xet as an LFS storage backend — evaluation

Context: **evaluation, not an implementation.** Whether content-defined chunking
([huggingface/xet-core](https://github.com/huggingface/xet-core)) is worth adding beneath `docs/LFS.md`'s
storage layer. Read with `LFS.md` §1.

Today an LFS object is stored whole and sha256-addressed, so it dedups only on an exact byte-for-byte
match: two versions differing by one byte are two complete copies. Xet cuts content on
content-defined boundaries (64 KiB target) and stores each distinct chunk once.

Reproduce: `cd bench/xet-eval && cargo run --release --bin <dedup|resource|revisions>`. The crate is
excluded from the workspace, so a normal build never resolves the Xet stack.

## 1. What it costs (`--bin resource`)

Chunking is ~3.5× faster than sha256, but LFS oids are sha256 **by protocol**, so Xet is additive,
never a replacement. The honest figure is running both:

| payload | wall | CPU | peak RSS |
|---|---:|---:|---:|
| 32 MiB | +12.6% | +12.5% | +0 MiB |
| 128 MiB | +13.7% | +13.8% | +0 MiB |
| 512 MiB | +13.5% | +13.0% | +1 MiB |

**+13–14% CPU, memory free.** ⚠️ This is a synthetic model of the hashing delta — `sha256(buf)` versus
`sha256 + chunk + hash + dedup-query` — **not** an end-to-end git-lfs measurement. It excludes HTTP,
the `ObjectStore` PUT and walgit's own handler. In a real push those dominate, so the end-to-end
fraction is smaller than 13%. Single-threaded; xet-core ships a parallel chunker not exercised here.

## 2. Where the benefit is, and is not (`--bin dedup`, 32 MiB payloads)

Bytes stored for a second version, *walgit / xet*:

| scenario | random | f32 tensor | repeated motif |
|---|---:|---:|---:|
| first upload (internal dedup) | 32.0 / 32.0 | 32.0 / 32.0 | 32.0 / **4.1** |
| 1 KiB edit mid-file | 32.0 / 0.1 | 32.0 / 0.1 | 32.0 / 0.1 |
| append 1% | 32.3 / 0.3 | 32.3 / 0.4 | 32.3 / 0.3 |
| prepend 1 KiB (shifts all) | 32.0 / 0.1 | 32.0 / 0.0 | 32.0 / 0.1 |
| fine-tune (10% rewritten) | 32.0 / 3.4 | 32.0 / 3.3 | 32.0 / 3.3 |
| unrelated file | 32.0 / 32.0 | 32.0 / 32.0 | 32.0 / 4.1 |
| sibling variant (70% shared) | 32.0 / 9.6 | 32.0 / 9.6 | 32.0 / 4.2 |

The prepend row is what fixed-size blocking cannot do: content-defined boundaries resynchronise after
a shift. Uniform zeros dedup 99.6% internally — no boundary ever fires, so the chunker cuts at
`MAX_CHUNK_SIZE` and every chunk is identical.

## 3. Cumulative cost over a history (`--bin revisions`)

The operator's real question is what a repository costs after N pushes.

| workload | after 12 revisions |
|---|---|
| CSV, append 5%/rev | 465 MiB → 51 MiB (**9.1×**) |
| CSV, append-only 1%/rev | 369 MiB → 33 MiB (**11.2×**) |
| CSV, append 5% **+ 1% of rows edited at random** | 465 MiB → 465 MiB (**1.0×**) |

⚠️ **Scattered edits defeat chunking completely.** The chunk is the dedup unit, so one changed byte
makes a whole 64 KiB chunk novel. At ~2000 CSV rows per chunk, editing 1% of rows at random dirties
*every* chunk. Localised edits dedup; sprinkled edits do not, and the file shape does not tell you
which you have.

**ATIF agent trajectories** ([harbor RFC 0001](https://github.com/harbor-framework/harbor/blob/main/rfcs/0001-trajectory-format.md))
are the standout case: one JSON root object rewritten **in full every agent turn**, so whole-object
storage keeps a complete copy per turn.

| session | final file | walgit | xet | ratio |
|---|---:|---:|---:|---:|
| 120 turns | 0.3 MiB (~4 chunks) | 16 MiB | 7 MiB | 2.1× |
| 300 turns | 2.4 MiB (~38 chunks) | 362 MiB | 19 MiB | **19.0×** |
| 600 turns | 9.5 MiB (~151 chunks) | 2854 MiB | 41 MiB | **69.9×** |

The ratio tracks **file size ÷ chunk size**. A file of only a few chunks has too few dedup units to
gain much; the benefit arrives once a file is tens of chunks. That is the single best predictor of
whether a workload benefits.

## 3a. GenAI conversations: the time dimension, not the corpus dimension (`--bin conversations`)

A corpus of long-running agent conversations shares a head — system prompt, tool definitions, agent
metadata — and diverges into unique turns. That sharing does **not** pay off:

| shared head | cross-conversation ratio |
|---|---:|
| 8 KiB | 1.00× |
| 32 KiB | 1.00× |
| 64 KiB | 1.40× |
| 512 KiB | 4.60× |

⚠️ **A shared region smaller than one chunk dedups at zero.** An 8–32 KiB prompt is swallowed into the
first chunk together with unique turn content, so that chunk differs per conversation. Real system
prompts sit squarely in that dead zone.

Shrinking the chunk target recovers some of it, but the ceiling is the shared *fraction*: 16 KiB
shared inside a 136 KiB conversation caps at 1.13× however it is chunked.

| chunk target | ratio | chunk metadata |
|---|---:|---:|
| 64 KiB | 1.00× | 0.03 MiB |
| 8 KiB | 1.01× | 0.16 MiB |
| 2 KiB | 1.12× | 0.57 MiB |

2 KiB chunks capture nearly the whole ceiling for 20× the metadata — a bad trade for 12%.

**The value for conversations is over time, not across the corpus.** One conversation rewritten or
appended each turn is the ATIF row above: 19× at 2.4 MiB, 69.9× at 9.5 MiB. Deduping a corpus of
independent conversations against each other is worth approximately nothing.

## 4. The library is pluggable in both directions

Neither path requires Hugging Face infrastructure:

| direction | seam | walgit implementation |
|---|---|---|
| write | `xet_data::deduplication::DeduplicationDataInterface` | `chunk_hash_dedup_query` → a chunk index; `register_new_xorb` → `ObjectStore::put`; `register_xorb_dependencies` → refcounting |
| read | `xet_client::cas_client::Client` | `upload_xorb`/`upload_shard`/`get_reconstruction`/`get_file_term_data` over `ObjectStore` |

`register_global_dedup_query`'s own doc says *"Simply return `Ok(())` to disable global dedup
queries"* — local-only dedup needs no remote CAS. `MemoryClient` is a working in-memory `Client` and
the template to copy. The chunker's source imports nothing from `xet-client`; the dependency is a
build-tree cost, not a runtime coupling.

## 5. Shape to build, if it proceeds

Hugging Face's own migration does **not** chunk on the upload path
([blog](https://huggingface.co/blog/migrating-the-hub-to-xet)): a non-Xet-aware client writes to LFS
storage normally, and a background worker migrates the object to Xet afterwards. Reads go through a
"Git LFS Bridge" that reconstructs the file from chunks and hands back a single presigned URL,
mimicking the LFS protocol. That keeps the +13–14% off the push path entirely.

1. **LFS write path unchanged** — no chunking on PUT, no added latency.
2. **Background migration** — a maintainer unit chunks stored objects, as `docs/INTEGRITY.md`'s repair
   unit already does other post-hoc work.
3. **Read via reconstruction** — `serve_via = "signed_url"` pointing at a reconstruction endpoint is
   exactly the Bridge pattern.
4. **Xet-aware clients later** get the native protocol, and with it the *bandwidth* win: only novel
   chunks cross the wire. walgit already parses the client's offered `transfers` list (`lfs.rs:22`)
   and answers only `"basic"` (`lfs.rs:201`), so the negotiation hook exists.

Behind `--features lfs-xet`, off by default, with `lfs.storage = "whole" | "xet"` per repository:
the workloads that gain and the workloads that gain nothing sit in the same fleet.

## 6. Open questions

- **Range.** `static_object.rs` implements the full conditional/range contract. Serving a reassembled
  file makes Range a computed seek across xorbs — doable from the reconstruction record, but it is
  the part of the contract that gets harder.
- **Deletion.** Whole objects are independent; shared chunks are not. `register_xorb_dependencies` is
  the hook, but refcounting or mark-and-sweep has no equivalent in `LFS.md` today.
- **The chunk index** is the one component walgit does not already have. The formats come from
  `xet-core-structures`.
- **Not measured:** xorb packing and shard metadata as actually serialised, CAS latency, network, or
  any walgit integration.
