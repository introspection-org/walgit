#![allow(unsafe_code)]

use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use serde::Serialize;
use tokio::runtime::{Handle, Runtime};
use walgit_store::{
    AccelTarget, BoxStream, ByteStream, DynStore, GetOptions, GetResult, ObjectMeta, ObjectStore,
    PutBody, PutOptions, Result, StoreError, Version,
};

use crate::abi::{self, Buffer, Reply, Request, Slice};
use crate::wire::{Accel, Meta, Operation, ReadResult, WireError, put_options};

fn json(value: &impl Serialize) -> Result<Buffer> {
    let bytes = serde_json::to_vec(value).map_err(StoreError::other)?;
    if bytes.len() > abi::MAX_METADATA {
        return Err(StoreError::InvalidArgument(
            "plugin metadata exceeds limit".into(),
        ));
    }
    Ok(abi::own_buffer(bytes))
}

fn error_reply(error: StoreError) -> Reply {
    let error = WireError::from(error);
    let bytes = serde_json::to_vec(&error)
        .unwrap_or_else(|_| br#"{"error":"other","message":"plugin failure"}"#.to_vec());
    Reply {
        status: -1,
        metadata: abi::own_buffer(bytes),
        body: abi::Stream::default(),
    }
}

pub fn error_text(buffer: &Buffer) -> String {
    // SAFETY: A live V1 buffer owns len readable bytes until it is dropped.
    match unsafe {
        abi::read_slice(
            Slice {
                ptr: buffer.ptr,
                len: buffer.len,
            },
            abi::MAX_METADATA,
        )
    } {
        Ok(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        Err(_) => "invalid error response".into(),
    }
}

fn decode<T: serde::de::DeserializeOwned>(buffer: &Buffer) -> Result<T> {
    // SAFETY: A live V1 buffer owns len readable bytes until drop.
    let bytes = unsafe {
        abi::read_slice(
            Slice {
                ptr: buffer.ptr,
                len: buffer.len,
            },
            abi::MAX_METADATA,
        )
    }
    .map_err(StoreError::other)?;
    serde_json::from_slice(bytes).map_err(StoreError::other)
}

struct StreamState {
    body: ByteStream,
    pending: Bytes,
    runtime: Handle,
}

fn expose_stream(body: ByteStream, runtime: Handle) -> abi::Stream {
    abi::Stream {
        context: Box::into_raw(Box::new(StreamState {
            body,
            pending: Bytes::new(),
            runtime,
        }))
        .cast(),
        next: Some(next_stream),
        release: Some(release_stream),
    }
}

unsafe extern "C" fn next_stream(context: *mut c_void, out: *mut Buffer) -> i32 {
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: Context was allocated in expose_stream; polling is exclusive.
        let state = unsafe { &mut *context.cast::<StreamState>() };
        if state.pending.is_empty() {
            match state.runtime.block_on(state.body.next()) {
                Some(Ok(bytes)) => state.pending = bytes,
                Some(Err(e)) => return Err(e),
                None => return Ok(None),
            }
        }
        let size = state.pending.len().min(abi::MAX_FRAME);
        Ok(Some(state.pending.split_to(size).to_vec()))
    }));
    let (code, buffer) = match result {
        Ok(Ok(Some(bytes))) => (1, abi::own_buffer(bytes)),
        Ok(Ok(None)) => (0, Buffer::default()),
        Ok(Err(e)) => (-1, error_reply(e).metadata),
        Err(_) => (
            -1,
            error_reply(StoreError::other(anyhow::anyhow!("plugin stream panicked"))).metadata,
        ),
    };
    // SAFETY: V1 caller supplies a writable, uninitialized output Buffer.
    unsafe { out.write(buffer) };
    code
}

unsafe extern "C" fn release_stream(context: *mut c_void) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: Stream handle owns this allocation, with no poll in flight.
        drop(unsafe { Box::from_raw(context.cast::<StreamState>()) });
    }));
}

struct Endpoint(abi::Store);
impl Drop for Endpoint {
    fn drop(&mut self) {
        // SAFETY: Arc guarantees no calls or streams using the endpoint remain.
        unsafe { (self.0.release)(self.0.context) };
    }
}

// Holding endpoint keeps the producing store/runtime alive until stream release.
fn receive_stream(stream: abi::Stream, endpoint: Option<Arc<Endpoint>>) -> ByteStream {
    Box::pin(futures::stream::try_unfold(
        (stream, endpoint),
        |(stream, endpoint)| async move {
            tokio::task::spawn_blocking(move || {
                let Some(next) = stream.next else {
                    return Ok(None);
                };
                let mut buffer = Buffer::default();
                // SAFETY: The stream is owned, polled serially, and out is writable.
                let code = unsafe { next(stream.context, &raw mut buffer) };
                match code {
                    0 => Ok(None),
                    1 => {
                        // SAFETY: The returned buffer lives through this copy.
                        let bytes = unsafe {
                            abi::read_slice(
                                Slice {
                                    ptr: buffer.ptr,
                                    len: buffer.len,
                                },
                                abi::MAX_FRAME,
                            )
                        }
                        .map_err(StoreError::other)?;
                        Ok(Some((Bytes::copy_from_slice(bytes), (stream, endpoint))))
                    }
                    -1 => Err(StoreError::from(decode::<WireError>(&buffer)?)),
                    _ => Err(StoreError::other(anyhow::anyhow!(
                        "invalid plugin stream status"
                    ))),
                }
            })
            .await
            .map_err(StoreError::other)?
        },
    ))
}

struct StoreState {
    store: DynStore,
    runtime: Handle,
    owned_runtime: Option<Runtime>,
}
impl Drop for StoreState {
    fn drop(&mut self) {
        if let Some(runtime) = self.owned_runtime.take() {
            runtime.shutdown_background();
        }
    }
}

pub fn expose(store: DynStore, runtime: Handle, owned_runtime: Option<Runtime>) -> abi::Store {
    abi::Store {
        supports_compose: u8::from(store.supports_compose()),
        compose_is_native: u8::from(store.compose_is_native()),
        context: Box::into_raw(Box::new(StoreState {
            store,
            runtime,
            owned_runtime,
        }))
        .cast(),
        request: request_store,
        release: release_store,
    }
}

unsafe extern "C" fn request_store(context: *mut c_void, request: Request, out: *mut Reply) {
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: Context is a live StoreState; its fields support concurrent use.
        let state = unsafe { &*context.cast::<StoreState>() };
        // SAFETY: Request metadata is borrowed for this synchronous call.
        let bytes = unsafe { abi::read_slice(request.metadata, abi::MAX_METADATA) }
            .map_err(StoreError::other)?;
        let operation: Operation = serde_json::from_slice(bytes).map_err(StoreError::other)?;
        state
            .runtime
            .block_on(dispatch(state, operation, request.body))
    }));
    let reply = match result {
        Ok(Ok(reply)) => reply,
        Ok(Err(error)) => error_reply(error),
        Err(_) => error_reply(StoreError::other(anyhow::anyhow!(
            "storage plugin panicked"
        ))),
    };
    // SAFETY: V1 caller supplies an uninitialized writable Reply.
    unsafe { out.write(reply) };
}

unsafe extern "C" fn release_store(context: *mut c_void) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: Endpoint ownership releases this allocation exactly once.
        drop(unsafe { Box::from_raw(context.cast::<StoreState>()) });
    }));
}

async fn dispatch(state: &StoreState, operation: Operation, body: abi::Stream) -> Result<Reply> {
    let store = &state.store;
    let mut reply = Reply::default();
    reply.metadata = match operation {
        Operation::Get { key, options } => match store.get(&key, options.into()).await? {
            GetResult::NotModified { version } => json(&ReadResult::NotModified {
                version: version.to_string(),
            })?,
            GetResult::Object { meta, body } => {
                reply.body = expose_stream(body, state.runtime.clone());
                json(&ReadResult::Object { meta: meta.into() })?
            }
        },
        Operation::Head { key } => json(&store.head(&key).await?.map(Meta::from))?,
        Operation::Put {
            key,
            len,
            mode,
            immutable,
            content_type,
        } => {
            let meta = store
                .put(
                    &key,
                    PutBody::Stream {
                        len,
                        stream: receive_stream(body, None),
                    },
                    put_options(mode, immutable, content_type.as_deref()),
                )
                .await?;
            return Ok(Reply {
                metadata: json(&Meta::from(meta))?,
                ..Reply::default()
            });
        }
        Operation::Delete { key, version } => {
            store.delete(&key, version.map(Version::new)).await?;
            json(&())?
        }
        Operation::List {
            prefix,
            start_after,
        } => {
            let lines = store.list(&prefix, start_after.as_deref()).map(|item| {
                let mut bytes =
                    serde_json::to_vec(&Meta::from(item?)).map_err(StoreError::other)?;
                bytes.push(b'\n');
                Ok(Bytes::from(bytes))
            });
            reply.body = expose_stream(Box::pin(lines), state.runtime.clone());
            json(&())?
        }
        Operation::ListPrefixes { prefix } => json(&store.list_prefixes(&prefix).await?)?,
        Operation::SignedGetUrl { key, ttl_secs } => json(
            &store
                .signed_get_url(&key, Duration::from_secs(ttl_secs))
                .await?,
        )?,
        Operation::AccelTarget { key } => json(&store.accel_target(&key).await.map(|a| Accel {
            url: a.url,
            authorization: a.authorization,
        }))?,
        Operation::Compose {
            key,
            sources,
            mode,
            immutable,
            content_type,
        } => json(&Meta::from(
            store
                .compose(
                    &key,
                    &sources,
                    put_options(mode, immutable, content_type.as_deref()),
                )
                .await?,
        ))?,
    };
    Ok(reply)
}

#[derive(Clone)]
pub struct RemoteStore {
    endpoint: Arc<Endpoint>,
}
impl RemoteStore {
    pub fn new(store: abi::Store) -> Self {
        Self {
            endpoint: Arc::new(Endpoint(store)),
        }
    }

    async fn call(&self, operation: Operation, body: Option<ByteStream>) -> Result<Reply> {
        let metadata = serde_json::to_vec(&operation).map_err(StoreError::other)?;
        if metadata.len() > abi::MAX_METADATA {
            return Err(StoreError::InvalidArgument(
                "plugin metadata exceeds limit".into(),
            ));
        }
        let body = body.map_or_else(abi::Stream::default, |body| {
            expose_stream(body, Handle::current())
        });
        let endpoint = self.endpoint.clone();
        tokio::task::spawn_blocking(move || {
            let mut reply = Reply::default();
            let request = Request {
                metadata: metadata.as_slice().into(),
                body,
            };
            // SAFETY: Endpoint lives through the call; request transfers body.
            unsafe { (endpoint.0.request)(endpoint.0.context, request, &raw mut reply) };
            match reply.status {
                0 => Ok(reply),
                -1 => Err(StoreError::from(decode::<WireError>(&reply.metadata)?)),
                _ => Err(StoreError::other(anyhow::anyhow!(
                    "invalid storage plugin status"
                ))),
            }
        })
        .await
        .map_err(StoreError::other)?
    }
}

#[async_trait::async_trait]
impl ObjectStore for RemoteStore {
    fn backend(&self) -> &'static str {
        "plugin"
    }
    async fn get(&self, key: &str, options: GetOptions) -> Result<GetResult> {
        let reply = self
            .call(
                Operation::Get {
                    key: key.into(),
                    options: options.into(),
                },
                None,
            )
            .await?;
        Ok(match decode(&reply.metadata)? {
            ReadResult::NotModified { version } => GetResult::NotModified {
                version: Version::new(version),
            },
            ReadResult::Object { meta } => GetResult::Object {
                meta: meta.into(),
                body: receive_stream(reply.body, Some(self.endpoint.clone())),
            },
        })
    }
    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        let reply = self.call(Operation::Head { key: key.into() }, None).await?;
        Ok(decode::<Option<Meta>>(&reply.metadata)?.map(Into::into))
    }
    async fn put(&self, key: &str, body: PutBody, options: PutOptions) -> Result<ObjectMeta> {
        let (len, stream) = match body {
            PutBody::Bytes(bytes) => (bytes.len() as u64, walgit_store::util::once(bytes)),
            PutBody::Stream { len, stream } => (len, stream),
            PutBody::File(path) => (
                tokio::fs::metadata(&path)
                    .await
                    .map_err(StoreError::other)?
                    .len(),
                walgit_store::util::file_stream(path, None, abi::MAX_FRAME),
            ),
        };
        let reply = self
            .call(
                Operation::Put {
                    key: key.into(),
                    len,
                    mode: options.mode.into(),
                    immutable: options.immutable,
                    content_type: options.content_type.map(str::to_owned),
                },
                Some(stream),
            )
            .await?;
        Ok(decode::<Meta>(&reply.metadata)?.into())
    }
    async fn delete(&self, key: &str, version: Option<Version>) -> Result<()> {
        self.call(
            Operation::Delete {
                key: key.into(),
                version: version.map(|v| v.to_string()),
            },
            None,
        )
        .await?;
        Ok(())
    }
    fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let this = self.clone();
        let operation = Operation::List {
            prefix: prefix.into(),
            start_after: start_after.map(str::to_owned),
        };
        // Newline framing permits the byte stream to split a JSON record.
        Box::pin(
            futures::stream::once(async move {
                let reply = this.call(operation, None).await?;
                let body = receive_stream(reply.body, Some(this.endpoint.clone()));
                Ok::<_, StoreError>(futures::stream::try_unfold(
                    (body, Vec::new()),
                    |(mut body, mut pending)| async move {
                        loop {
                            if let Some(end) = pending.iter().position(|b| *b == b'\n') {
                                let line: Vec<_> = pending.drain(..=end).collect();
                                let meta: Meta =
                                    serde_json::from_slice(&line).map_err(StoreError::other)?;
                                return Ok(Some((meta.into(), (body, pending))));
                            }
                            if pending.len() > abi::MAX_METADATA {
                                return Err(StoreError::InvalidArgument(
                                    "plugin list record exceeds limit".into(),
                                ));
                            }
                            match body.next().await {
                                Some(chunk) => pending.extend_from_slice(&chunk?),
                                None if pending.is_empty() => return Ok(None),
                                None => {
                                    return Err(StoreError::other(anyhow::anyhow!(
                                        "truncated plugin listing"
                                    )));
                                }
                            }
                        }
                    },
                ))
            })
            .try_flatten(),
        )
    }
    async fn list_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        let reply = self
            .call(
                Operation::ListPrefixes {
                    prefix: prefix.into(),
                },
                None,
            )
            .await?;
        decode(&reply.metadata)
    }
    async fn signed_get_url(&self, key: &str, ttl: Duration) -> Result<Option<String>> {
        let reply = self
            .call(
                Operation::SignedGetUrl {
                    key: key.into(),
                    ttl_secs: ttl.as_secs(),
                },
                None,
            )
            .await?;
        decode(&reply.metadata)
    }
    async fn accel_target(&self, key: &str) -> Option<AccelTarget> {
        let reply = self
            .call(Operation::AccelTarget { key: key.into() }, None)
            .await
            .ok()?;
        decode::<Option<Accel>>(&reply.metadata)
            .ok()?
            .map(|a| AccelTarget {
                url: a.url,
                authorization: a.authorization,
            })
    }
    fn supports_compose(&self) -> bool {
        self.endpoint.0.supports_compose == 1
    }
    fn compose_is_native(&self) -> bool {
        self.endpoint.0.compose_is_native == 1
    }
    async fn compose(
        &self,
        key: &str,
        sources: &[String],
        options: PutOptions,
    ) -> Result<ObjectMeta> {
        let reply = self
            .call(
                Operation::Compose {
                    key: key.into(),
                    sources: sources.into(),
                    mode: options.mode.into(),
                    immutable: options.immutable,
                    content_type: options.content_type.map(str::to_owned),
                },
                None,
            )
            .await?;
        Ok(decode::<Meta>(&reply.metadata)?.into())
    }
}

/// Implementation behind `export_plugin!`. All panics stop at this C boundary.
/// # Safety
/// V1 create owns inner and must receive valid borrowed config and writable out/error.
pub unsafe fn export<F, Fut>(
    inner: abi::Store,
    config: Slice,
    out: *mut abi::Store,
    error: *mut Buffer,
    factory: F,
) -> i32
where
    F: FnOnce(DynStore, serde_json::Value) -> Fut,
    Fut: Future<Output = anyhow::Result<DynStore>>,
{
    let inner: DynStore = Arc::new(RemoteStore::new(inner));
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: V1 caller holds config readable until create returns.
        let bytes = unsafe { abi::read_slice(config, abi::MAX_METADATA) }?;
        let config = serde_json::from_slice(bytes)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        let store = runtime.block_on(factory(inner, config))?;
        Ok::<_, anyhow::Error>(expose(store, runtime.handle().clone(), Some(runtime)))
    }));
    match result {
        Ok(Ok(store)) => {
            // SAFETY: Success initializes caller's store output exactly once.
            unsafe { out.write(store) };
            0
        }
        error_result => {
            let message = match error_result {
                Ok(Err(e)) => e.to_string(),
                _ => "storage plugin initialization panicked".into(),
            };
            // SAFETY: Failure initializes caller's error output exactly once.
            unsafe { error.write(abi::own_buffer(message.into_bytes())) };
            -1
        }
    }
}
