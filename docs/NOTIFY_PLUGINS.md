# External notify plugins

An optional, operator-installed `cdylib` tells the events bridge that a commit point
is durable. The stock build contains no broker client and no credential policy.
`examples/notify-memory` is the complete in-process example. A downstream crate
returns any `walgit_notify_plugin::NotifySource` using
`walgit_notify_plugin::export_notify_plugin!(factory)`. It must also declare
`crate-type = ["cdylib"]` and depend on `abi_stable` itself, matching the version this
crate pins — the same wart [STORAGE_PLUGINS.md](STORAGE_PLUGINS.md) documents, for the
same reason and with the same two non-fixes.

## Why a plugin rather than a built-in

GCS and S3 announce a finalized object for us, and `POST /_events/notify` receives it
(see [EVENTS.md](EVENTS.md)). A bucket that cannot do that leaves the sweep as the only
wake-up: correct, but minutes late.

Closing that gap in-tree would mean walgit carrying a broker client, and with it the
part that is genuinely hard — not publishing a string, but **authenticating**. A
managed Redis or Pub/Sub endpoint issues short-lived IAM credentials per connection;
a cluster moves its shards; a private endpoint terminates TLS against an internal CA.
That is deployment policy with a deployment's own release cycle, and it is exactly
what an operator already resolves elsewhere in their stack. So walgit moves object
names across a checked boundary and nothing else. GCP Pub/Sub, Redis pub/sub, SNS,
NATS and a plain queue are all the same twenty lines on the other side of it.

A **pull** subscription plugin also subsumes the push path it replaces: no ingress
route to expose, no token to hand a notifier, no signed body to verify.

## Configuration

```toml
[store.notify]
library = "/usr/local/lib/walgit/libmy_notify.so"   # absolute, operator-installed
options = {}                                        # transport-owned; never plaintext credentials
```

`serve` and `maintain` publish; the `events` role subscribes. One process loads one
transport and shares it, so a deployment that does both holds a single connection pair.
Initialization errors are fatal. `cache.store_mount` is refused alongside
`store.notify`, because a mount bypasses `ObjectStore` and therefore the announcement.

## The contract

```rust
#[async_trait]
pub trait NotifySource: Send + Sync {
    async fn publish(&self, key: &str) -> anyhow::Result<()>;
    async fn next(&self) -> anyhow::Result<Option<String>>;
}
```

- `key` is a full object name, prefix included — byte-identical to what a bucket
  notification carries, so the bridge resolves it by the same path.
- `publish` is called off the caller's path and its failures are logged, never
  propagated: a broker outage must not slow or fail a push.
- `next` blocks for the life of a subscription. `Ok(None)` ends it.
- **An error from `next` means messages were lost and the transport has already
  recovered.** The host reconciles by sweeping. Reporting before recovering reopens
  the gap underneath that sweep, which is the one ordering here that is easy to get
  wrong and invisible when wrong.

Methods are synchronous across the ABI and the SDK dispatches them on blocking
workers; no future crosses FFI. The endpoint owns the runtime its transport runs on.
Panics are caught and returned as errors — as in the storage SDK, that is not a
sandbox: native plugins are trusted code.

## Guarantees this does not need

Notifications are latency, never correctness. The sweep is what makes the bridge
correct, so a transport may drop messages, deliver duplicates, or deliver out of
order. A dropped message costs one sweep interval; `events_bridge_sweep_found_total`
is what says they stopped flowing, and `store_notify_gap_total` says the transport
noticed. Nothing here needs durability, replay, ordering or acknowledgement — which
is why a fire-and-forget broker is a sound choice.

## Verification

`just test-notify-plugin` builds `examples/notify-memory` and exercises the real
boundary: options the plugin rejects, a subscription that parks without taking the
caller's runtime with it, two loads proving one library is not one shared transport,
and a published key arriving on the other side.
