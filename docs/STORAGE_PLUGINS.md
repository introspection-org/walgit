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

## V1 boundary

`walgit_store_plugin_v1` returns the version/size-checked C descriptor in
`walgit-store-plugin/src/abi.rs`. The ABI uses `repr(C)` structures, C function
pointers, fixed-width status fields, pointer-sized byte lengths and opaque
contexts. Both sides must target the same architecture/OS. No Rust trait object,
allocator ownership, future, string layout or unwind crosses the boundary. Rust
plugins use `crate-type = ["cdylib"]`; no Rust compiler-specific `dylib` ABI is used.

Request and result metadata are bounded UTF-8 JSON described by `wire.rs`; bytes
are separate, pull-based streams with at most 1 MiB per frame. JSON owns no pack
bytes. PUT takes an explicit logical length; bytes/file/stream inputs all become
streams at the boundary. LIST is streamed as newline-delimited metadata records.
Reads return the whole object's logical size even when their body is a range.
Errors preserve not-found, precondition, not-modified and retryable outcomes.
Backing versions remain opaque strings. MIME types are advisory hints; the SDK
forwards the known Git/JSON/protobuf/plain/octet-stream hints and drops unknown
hints rather than leaking per-request static strings.

Each allocation carries its allocating module's release callback. Each stream
has exactly one owner; release drops/cancels it without draining. Calls on
different stores/streams may run concurrently, while one stream is polled
serially. An endpoint outlives its calls and result streams. Create consumes the
inner endpoint on success and failure. Every exported callback catches unwinding
panics and reports failure. A panic-abort build can still terminate the process.

Callbacks are synchronous and run on blocking executor threads. The SDK bridges
to the module's own Tokio runtime, preserving bounded streaming and backpressure;
it never calls block_on from an async worker. Plugins are trusted native code,
not a sandbox. Libraries stay mapped until process exit to keep runtime/channel
teardown safe; upgrade by rolling the process, not hot-unloading a library.

ABI evolution uses a new versioned symbol for incompatible layouts or semantics.
Unknown operations fail, rather than falling back to the raw store. Test plugins
against the pinned core revision before upgrades. Load only immutable library
paths baked into the image or administrator-controlled read-only mounts.

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
