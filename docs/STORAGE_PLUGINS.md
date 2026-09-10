# External storage plugins

An optional, operator-installed `cdylib` decorates the configured S3, GCS or memory
store. The stock build contains no encryption or application identity policy.
`examples/store-passthrough` is the complete no-op example. A downstream crate can
return any `ObjectStore` using `walgit_store_plugin::export_plugin!(factory)`.

Configure `[store.plugin]` with an absolute `library` path and an `options` table.
Every CLI storage operation uses the same constructor: HTTP serving, maintenance,
follow, events, imports (including direct import), compaction, WAL, settings and
repository administration. Initialization errors are fatal. With no plugin the
existing binaries and object formats are unchanged. `cache.store_mount` is refused
with a plugin because it bypasses the abstraction. Options are deployment config;
credentials should be passed by environment/secret reference, not serialized in
options or supplied by a repository/user.

The factory receives the raw backend with the configured global prefix applied
exactly once. The decorator sees **logical keys before that prefix**. A plugin
needing bucket/domain identity must receive a stable identifier explicitly. It
must not infer tenant authority from an unverified request.

## Checked Rust boundary and rationale

The interface uses [`abi_stable` 0.11.3](https://docs.rs/abi_stable/0.11.3/abi_stable/),
with a `RootModule`, `StableAbi` layouts, `sabi_trait` endpoints and owned
`RBox`/`RVec`/`RString` values. A normal Rust trait object is not a stable dynamic
library interface. This mechanism checks module versions and layouts at load time
and provides allocator-correct ownership/destruction across independently built
Rust libraries. It follows [Rotel's Rust processor SDK](https://github.com/streamfold/rotel/blob/v0.2.5/rotel_rust_processor_sdk/src/lib.rs)
and its async processor bridge. Safety and maintenance are the reasons for this
choice, rather than an assumed performance gain. This is a Rust plugin API;
plugins written in other languages would need a separate adapter.

`abi.rs` still has `repr(C)`/`extern "C"` declarations underneath the checked
interface. The SDK contains no hand-written raw-pointer dereferences, allocator
release callbacks or manual vtables. `lib_header_from_path` plus
`init_root_module` checks each library independently, as Rotel does; the singleton
root-module loader would accidentally reuse the first plugin for subsequent paths.
Both sides must target the same OS/architecture and compatible SDK/abi_stable
versions. Incompatible layouts fail before initialization; there is no unchecked
loading path. Implementation crates depend on abi_stable and export their async
factory with `walgit_store_plugin::export_plugin!`.

Operation metadata is bounded UTF-8 JSON described by `wire.rs`; pack bytes are
separate pull-based streams with at most 1 MiB per frame. PUT retains its explicit
logical length. LIST streams newline-delimited records. Reads report the whole
logical object size, including for ranges, and errors preserve not-found,
precondition, not-modified and retryable outcomes. Versions stay opaque. Known
Git/JSON/protobuf/plain/octet-stream MIME hints are forwarded; unknown advisory
hints are omitted instead of leaking per-request static strings.

Calls run on blocking executor threads and bridge to the module's own Tokio
runtime; application futures stay within their module. Streams are polled
serially, distinct calls may run concurrently, and response streams keep their
endpoint alive. Dropping a stream cancels without draining. Returned RVec frames
retain their owner through Bytes, avoiding an additional receiver-side copy.
Factory/request/poll panics are converted to errors; aborting panics or panicking
destructors can still terminate the process. Native plugins remain trusted code,
not a sandbox. abi_stable retains mappings until process exit; upgrades roll
processes, never hot-unload code.

Public interface changes follow abi_stable's layout/evolution rules. The generic
plugin preserves the existing ObjectStore trait and no-plugin CLI behavior. The
only functional example is pass-through; its deliberately incompatible test
module exists solely to exercise load-time rejection. No encryption or tenant
policy is implemented here.

## Decorator obligations

An adapter must preserve conditional publication/deletion, plaintext size/range
semantics, listing order and isolation, cancellation and error categories.
Transformers must not return raw signed URLs, acceleration targets or native
compose unless those paths preserve the logical object. Those methods already
default to unavailable/unsupported on `ObjectStore`. In particular, ciphertext
concatenation is not plaintext composition. Existing data requires an explicit
format/migration policy; plugin configuration is not a format migration.

There are no extra bucket requests in the pass-through path (before → after:
GET 1 → 1, PUT 1 → 1, manifest CAS 1 → 1). There is callback/thread-hop and
serialization overhead; transformed stores must publish their own measured
latency/throughput and request budgets.

## Verification

```sh
just test-plugin
```

The recipe selects `.so` on Linux and `.dylib` on macOS. The integration test loads a real shared library and checks
streaming/file uploads, ranges across frames, CAS races, conditional reads/deletes,
listing, prefix application, interrupted uploads and stream lifetime after the
store handle is dropped. Also run ordinary Git/maintenance tests through the
plugin before deploying a custom adapter.

### Local overhead measurement

A macOS arm64, test-profile `MemoryStore` probe (three alternating runs; median
of each run's mean full-GET latency) measured:

| Object bytes | Manual ABI prototype | abi_stable implementation |
|---|---:|---:|
| 1 KiB | 66.12 µs | 66.92 µs |
| 1 MiB | 127.93 µs | 97.84 µs |
| 8 MiB | 840.27 µs | 602.14 µs |

Small-object overhead is comparable in this sample. The new implementation also
avoids a receiver-side frame copy, so this does **not** isolate the cost of the
ABI library itself. These are local in-memory timings, not a production throughput
claim: no S3/GCS, Git, encryption or KMS is included. Safety and consistency decide
the implementation choice; measure the real adapter's workload separately.
The ignored `passthrough_overhead` test reproduces the current implementation's
probe with `WALGIT_TEST_PLUGIN` set, `--ignored --nocapture --test-threads=1`.
