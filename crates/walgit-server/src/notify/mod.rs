//! Store notifications: how a finalized commit point reaches the events bridge
//! (`docs/EVENTS.md`) when the bucket cannot announce it itself.
//!
//! On GCS or S3 the *store* announces the object — a bucket notification names
//! the finalized `…/manifest.pb` and `POST /_events/notify` receives it. An
//! in-cluster bucket (`MinIO`, a self-hosted plane) usually has no such wiring,
//! and the bridge falls back to the sweep: correct, but minutes late.
//!
//! [`NotifyingStore`] restores the missing half in the same architectural
//! position. It decorates the store rather than hooking the five manifest
//! writes in `walgit-wal`, so the invariant holds unchanged: nothing on the
//! push path knows events exist. The push path calls `put`; the *store*
//! announces the object it just made durable, exactly as a bucket does.
//!
//! Notifications are latency, never correctness. Publishing is best-effort and
//! off the caller's path, a dropped message costs one sweep interval, and
//! `events_bridge_sweep_found_total` is what says they stopped flowing.
//!
//! walgit ships no transport. Which broker, how it authenticates and how it
//! routes are deployment policy, so they live in an operator-installed plugin
//! (`docs/NOTIFY_PLUGINS.md`) exactly as a storage capability does.

use std::sync::Arc;

use walgit_store::{
    AccelTarget, BoxStream, DynStore, GetOptions, GetResult, ObjectMeta, ObjectStore, PutBody,
    PutOptions, Result, Version,
};

/// A transport with two ends, because one process is rarely both: `serve` and
/// `maintain` [`publish`](Notify::publish) once an object is durable, and the
/// `events` role [`subscribe`](Notify::subscribe)s. Either end may be the
/// provided no-op — `publish` where the storage layer announces for us,
/// `subscribe` where wake-ups arrive over HTTP instead.
#[async_trait::async_trait]
pub trait Notify: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    /// `key` is the full object name including the store's global prefix, so
    /// it is byte-identical to what a bucket notification would carry.
    async fn publish(&self, key: &str) -> anyhow::Result<()> {
        let _ = key;
        Ok(())
    }

    /// Runs until the process ends, waking `on` for every key received. An
    /// implementation owns its own reconnection.
    async fn subscribe(&self, on: Arc<dyn Wake>) -> anyhow::Result<()> {
        let _ = on;
        std::future::pending::<()>().await;
        Ok(())
    }
}

/// What a subscription does with what it receives. Implemented by the bridge;
/// a test substitutes a recorder.
#[async_trait::async_trait]
pub trait Wake: Send + Sync + 'static {
    async fn finalized(&self, key: &str);
    /// Called after a subscription gap. Pub/sub has no replay, so a transport
    /// must not silently absorb its own reconnect: the keys published while it
    /// was away are simply gone, and waiting for the periodic sweep to notice
    /// is the staleness this whole module exists to remove.
    async fn reconcile(&self);
}

#[async_trait::async_trait]
impl Wake for Arc<crate::bridge::Bridge> {
    async fn finalized(&self, key: &str) {
        match self.object_finalized(key).await {
            Ok(Some(report)) => {
                tracing::debug!(repo = %report.repo, emitted = report.emitted, "notify: caught up");
            }
            Ok(None) => {}
            // At-least-once is the sweep's job, not the transport's: retrying
            // here would stall every later key behind one broken repo.
            Err(e) => tracing::warn!(error = %e, key, "notify: catch-up failed"),
        }
    }

    async fn reconcile(&self) {
        self.sweep().await;
    }
}

/// The configured transport, or `None` when unconfigured. Cached: `open_store`
/// and the subscriber both ask, and one process runs one transport — a second
/// load would publish into a connection nothing is reading.
pub async fn open(cfg: &walgit_config::Config) -> anyhow::Result<Option<Arc<dyn Notify>>> {
    static LOADED: tokio::sync::OnceCell<Option<Arc<dyn Notify>>> =
        tokio::sync::OnceCell::const_new();
    let Some(notify) = &cfg.store.notify else {
        return Ok(None);
    };
    anyhow::ensure!(
        notify.library.is_absolute(),
        "store.notify.library must be absolute"
    );
    let loaded = LOADED
        .get_or_try_init(|| async {
            let remote =
                walgit_notify_plugin::load(&notify.library, notify.options.clone()).await?;
            Ok::<_, anyhow::Error>(Some(Arc::new(remote) as Arc<dyn Notify>))
        })
        .await?;
    Ok(loaded.clone())
}

#[async_trait::async_trait]
impl Notify for walgit_notify_plugin::RemoteNotify {
    fn name(&self) -> &'static str {
        "plugin"
    }

    async fn publish(&self, key: &str) -> anyhow::Result<()> {
        walgit_notify_plugin::RemoteNotify::publish(self, key).await
    }

    async fn subscribe(&self, on: Arc<dyn Wake>) -> anyhow::Result<()> {
        loop {
            match walgit_notify_plugin::RemoteNotify::next(self).await {
                Ok(Some(key)) => {
                    metrics::counter!("store_notify_received_total", "transport" => "plugin")
                        .increment(1);
                    on.finalized(&key).await;
                }
                Ok(None) => return Ok(()),
                // The transport lost messages and has recovered; the keys it
                // dropped are gone, so the sweep is what turns that gap back
                // into delivered events.
                Err(e) => {
                    metrics::counter!("store_notify_gap_total", "transport" => "plugin")
                        .increment(1);
                    tracing::warn!(error = %e, "notify: transport gap, reconciling");
                    on.reconcile().await;
                }
            }
        }
    }
}

/// Announces every finalized `…/manifest.pb` on [`Notify`]. Everything else
/// passes straight through.
pub struct NotifyingStore {
    inner: DynStore,
    notify: Arc<dyn Notify>,
    /// The store's global prefix, re-applied to the announced name: the
    /// decorator sits above `Prefixed` and therefore sees logical keys, while
    /// the bridge strips the prefix exactly as it does from a bucket's own.
    prefix: String,
}

impl NotifyingStore {
    pub fn new(inner: DynStore, notify: Arc<dyn Notify>, prefix: String) -> Self {
        NotifyingStore {
            inner,
            notify,
            prefix,
        }
    }

    /// Off the caller's path in every sense: a spawned task, so a slow broker
    /// cannot add latency to a push, and a logged failure, so it cannot fail one.
    fn announce(&self, key: &str) {
        if !key.ends_with("/manifest.pb") {
            return;
        }
        let name = format!("{}{key}", self.prefix);
        let notify = self.notify.clone();
        tokio::spawn(async move {
            match notify.publish(&name).await {
                Ok(()) => metrics::counter!("store_notify_published_total",
                    "transport" => notify.name())
                .increment(1),
                Err(e) => {
                    metrics::counter!("store_notify_failed_total", "transport" => notify.name())
                        .increment(1);
                    tracing::warn!(error = %e, key = %name, "notify: publish failed");
                }
            }
        });
    }
}

#[async_trait::async_trait]
impl ObjectStore for NotifyingStore {
    fn backend(&self) -> &'static str {
        self.inner.backend()
    }
    fn is_prefixed(&self) -> bool {
        self.inner.is_prefixed()
    }
    async fn get(&self, key: &str, opts: GetOptions) -> Result<GetResult> {
        self.inner.get(key, opts).await
    }
    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        self.inner.head(key).await
    }
    async fn put(&self, key: &str, body: PutBody, opts: PutOptions) -> Result<ObjectMeta> {
        let meta = self.inner.put(key, body, opts).await?;
        self.announce(key);
        Ok(meta)
    }
    async fn delete(&self, key: &str, if_version: Option<Version>) -> Result<()> {
        self.inner.delete(key, if_version).await
    }
    fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix, start_after)
    }
    async fn list_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list_prefixes(prefix).await
    }
    async fn signed_get_url(&self, key: &str, ttl: std::time::Duration) -> Result<Option<String>> {
        self.inner.signed_get_url(key, ttl).await
    }
    async fn accel_target(&self, key: &str) -> Option<AccelTarget> {
        self.inner.accel_target(key).await
    }
    fn supports_compose(&self) -> bool {
        self.inner.supports_compose()
    }
    fn compose_is_native(&self) -> bool {
        self.inner.compose_is_native()
    }
    async fn compose(
        &self,
        dest: &str,
        sources: &[String],
        opts: PutOptions,
    ) -> Result<ObjectMeta> {
        // Composition writes an object like any other put; a composed manifest
        // must announce itself too.
        let meta = self.inner.compose(dest, sources, opts).await?;
        self.announce(dest);
        Ok(meta)
    }
}

/// Spawns the subscription when this instance is the bridge and a transport is
/// configured. Without a bridge there is nothing to wake.
pub fn spawn_subscriber(state: Arc<crate::AppState>) {
    let Some(bridge) = state.bridge.clone() else {
        return;
    };
    if state.cfg.store.notify.is_none() {
        return;
    }
    tokio::spawn(async move {
        match open(&state.cfg).await {
            Ok(Some(transport)) => spawn_subscriber_with(bridge, transport),
            Ok(None) => {}
            // `open_store` already resolved this at startup, so reaching here
            // means the library went away underneath us.
            Err(e) => tracing::error!(error = %e, "notify: subscriber not started"),
        }
    });
}

/// The spawn itself, for an embedder (or a test) holding its own transport.
pub fn spawn_subscriber_with(bridge: Arc<crate::bridge::Bridge>, transport: Arc<dyn Notify>) {
    tracing::info!(
        transport = transport.name(),
        "notify: subscribing for bridge wake-ups"
    );
    tokio::spawn(async move {
        if let Err(e) = transport.subscribe(Arc::new(bridge)).await {
            tracing::error!(error = %e, "notify: subscription stopped; sweep is now the only wake-up");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Recorder(Mutex<Vec<String>>);

    #[async_trait::async_trait]
    impl Notify for Arc<Recorder> {
        fn name(&self) -> &'static str {
            "test"
        }
        async fn publish(&self, key: &str) -> anyhow::Result<()> {
            self.0.lock().unwrap().push(key.to_string());
            Ok(())
        }
    }

    /// Only the commit point is announced, and it carries the global prefix so
    /// the bridge strips it exactly as it does a bucket notification's.
    #[tokio::test]
    async fn announces_prefixed_manifests_only() {
        let recorder = Arc::new(Recorder::default());
        let store = NotifyingStore::new(
            Arc::new(walgit_store::memory::MemoryStore::new()),
            Arc::new(recorder.clone()),
            "prefix/".into(),
        );
        for key in [
            "repos/t/r/manifest.pb",
            "repos/t/r/wal/000001.pb",
            "repos/t/r/events/cursor.json",
        ] {
            store
                .put(
                    key,
                    PutBody::Bytes(Bytes::from_static(b"x")),
                    PutOptions::default(),
                )
                .await
                .unwrap();
        }
        // The announcement is spawned, so it lands after the put returns.
        tokio::task::yield_now().await;
        assert_eq!(
            *recorder.0.lock().unwrap(),
            vec!["prefix/repos/t/r/manifest.pb".to_string()]
        );
    }

    /// A relative path would resolve against whatever directory the process
    /// happens to be in, which is not a thing an operator can reason about.
    #[tokio::test]
    async fn a_relative_library_is_refused_at_startup() {
        let mut cfg = walgit_config::Config::default();
        cfg.store.notify = Some(walgit_config::StoreNotifyConfig {
            library: "libwalgit_notify_memory.so".into(),
            options: serde_json::json!({}),
        });
        assert!(open(&cfg).await.is_err());
        assert!(
            open(&walgit_config::Config::default())
                .await
                .unwrap()
                .is_none()
        );
    }
}
