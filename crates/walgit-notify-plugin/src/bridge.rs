//! Both sides of the boundary: the host's handle on a plugin endpoint, and the
//! plugin's adapter from ordinary async Rust onto the synchronous ABI.
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use abi_stable::sabi_trait::TD_Opaque;
use abi_stable::std_types::{ROption, RResult, RString, RVec};
use tokio::runtime::{Handle, Runtime};

use crate::abi::{self, NotifyApi};

/// What a plugin implements: the transport, in ordinary async Rust.
#[async_trait::async_trait]
pub trait NotifySource: Send + Sync {
    async fn publish(&self, key: &str) -> anyhow::Result<()>;
    async fn next(&self) -> anyhow::Result<Option<String>>;
}

/// The host's handle. Endpoint calls block, so both run on blocking workers;
/// `next` in particular parks there for the life of a subscription. Cloning
/// shares one transport, which is how one deployment publishes from its store
/// and subscribes from its bridge.
#[derive(Clone)]
pub struct RemoteNotify {
    endpoint: Arc<abi::Notify>,
}

impl RemoteNotify {
    pub fn new(endpoint: abi::Notify) -> Self {
        RemoteNotify {
            endpoint: Arc::new(endpoint),
        }
    }

    pub async fn publish(&self, key: &str) -> anyhow::Result<()> {
        anyhow::ensure!(key.len() <= abi::MAX_KEY, "notified key exceeds limit");
        let endpoint = self.endpoint.clone();
        let key = RString::from(key);
        tokio::task::spawn_blocking(move || {
            catch_unwind(AssertUnwindSafe(|| endpoint.publish(key)))
                .map_err(|_| anyhow::anyhow!("notify plugin panicked"))?
                .into_result()
                .map_err(|error| anyhow::anyhow!("notify plugin publish failed: {error}"))
        })
        .await?
    }

    /// `Ok(None)` ends the subscription; an error is a recovered gap to
    /// reconcile, never a reason to stop reading.
    pub async fn next(&self) -> anyhow::Result<Option<String>> {
        let endpoint = self.endpoint.clone();
        tokio::task::spawn_blocking(move || {
            let next = catch_unwind(AssertUnwindSafe(|| endpoint.next()))
                .map_err(|_| anyhow::anyhow!("notify plugin panicked"))?
                .into_result()
                .map_err(|error| anyhow::anyhow!("notify plugin subscription failed: {error}"))?;
            Ok(next.into_option().map(Into::into))
        })
        .await?
    }
}

struct NotifyState {
    source: Box<dyn NotifySource>,
    runtime: Option<Runtime>,
}

impl Drop for NotifyState {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

impl NotifyState {
    /// A panic must not unwind across FFI and a shutting-down runtime must not
    /// be used: both become an error the host logs.
    fn guard<T>(
        &self,
        call: impl FnOnce(&dyn NotifySource, &Handle) -> anyhow::Result<T>,
    ) -> RResult<T, RString> {
        let Some(runtime) = self.runtime.as_ref() else {
            return RResult::RErr("notify plugin is shutting down".into());
        };
        match catch_unwind(AssertUnwindSafe(|| {
            call(self.source.as_ref(), runtime.handle())
        })) {
            Ok(Ok(value)) => RResult::ROk(value),
            Ok(Err(error)) => RResult::RErr(RString::from(error.to_string())),
            Err(_) => RResult::RErr("notify plugin panicked".into()),
        }
    }
}

impl NotifyApi for NotifyState {
    fn publish(&self, key: RString) -> RResult<(), RString> {
        self.guard(|source, handle| handle.block_on(source.publish(key.as_str())))
    }
    fn next(&self) -> RResult<ROption<RString>, RString> {
        self.guard(|source, handle| Ok(handle.block_on(source.next())?.map(RString::from).into()))
    }
}

/// The SDK factory boundary contains panics; no application future crosses FFI.
pub fn export<F, Fut, N>(config: RVec<u8>, factory: F) -> RResult<abi::Notify, RString>
where
    F: FnOnce(serde_json::Value) -> Fut,
    Fut: Future<Output = anyhow::Result<N>>,
    N: NotifySource + 'static,
{
    let result = catch_unwind(AssertUnwindSafe(|| {
        let options = serde_json::from_slice(&config);
        drop(config);
        let config: serde_json::Value = options?;
        // The transport's own runtime: a subscription outlives every call, so
        // it cannot borrow the host's.
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let source = runtime.block_on(factory(config))?;
        Ok::<_, anyhow::Error>(abi::NotifyApi_TO::from_value(
            NotifyState {
                source: Box::new(source),
                runtime: Some(runtime),
            },
            TD_Opaque,
        ))
    }));
    match result {
        Ok(result) => result.map_err(|e| RString::from(e.to_string())).into(),
        Err(_) => RResult::RErr("notify plugin initialization panicked".into()),
    }
}
