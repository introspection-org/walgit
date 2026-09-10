//! Azure Blob Storage backend.
//!
//! Every mutation publishes through one conditional request: single-shot blobs
//! carry the condition on the upload, staged blobs carry it on the commit, so a
//! CAS never observes a half-written object.

use std::sync::Arc;

use async_trait::async_trait;
use azure_core::credentials::TokenCredential;
use azure_core::http::{Etag, StatusCode};
use azure_storage_blob::models::{
    BlobClientDeleteOptions, BlobClientDownloadOptions, BlobClientGetPropertiesOptions,
    BlobClientGetPropertiesResultHeaders, BlobContainerClientListBlobsOptions,
    BlockBlobClientCommitBlockListOptions, BlockBlobClientCommitBlockListResultHeaders,
    BlockBlobClientUploadOptions, BlockLookupList, HttpRange,
};
use azure_storage_blob::{BlobClient, BlobContainerClient};
use bytes::{Bytes, BytesMut};
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use url::Url;

use crate::{
    ByteStream, GetOptions, GetResult, ObjectMeta, ObjectStore, PutBody, PutMode, PutOptions,
    Result, StoreError, Version,
};

const MAX_PAGE: i32 = 5000;
const AZURE_API_VERSION: &str = "2021-08-06";
const STORAGE_SCOPE: &str = "https://storage.azure.com/.default";

pub struct AzureStore {
    container: Arc<BlobContainerClient>,
    container_url: Url,
    pipeline: azure_core::http::Pipeline,
    multipart_threshold: u64,
    multipart_part_size: u64,
    max_concurrent_blocks: usize,
}

impl AzureStore {
    pub fn new(cfg: &walgit_config::StoreConfig) -> anyhow::Result<Self> {
        let account = &cfg.azure.account;
        let endpoint = if cfg.azure.endpoint.is_empty() {
            anyhow::ensure!(
                !account.is_empty(),
                "azure: set store.azure.account or store.azure.endpoint"
            );
            format!("https://{account}.blob.core.windows.net")
        } else {
            cfg.azure.endpoint.clone()
        };
        anyhow::ensure!(!cfg.bucket.is_empty(), "azure: store.bucket names the container");

        let sas = std::env::var(&cfg.azure.sas_token_env).ok().filter(|t| !t.is_empty());
        let mut container_url = Url::parse(&endpoint)
            .map_err(|e| anyhow::anyhow!("azure: invalid endpoint {endpoint}: {e}"))?
            .join(&format!("{}/", cfg.bucket))
            .map_err(|e| anyhow::anyhow!("azure: invalid container {}: {e}", cfg.bucket))?;

        let credential = match &sas {
            Some(token) => {
                container_url.set_query(Some(token.trim_start_matches('?')));
                None
            }
            None => Some(default_credential()?),
        };

        let container =
            BlobContainerClient::new(container_url.clone(), credential.clone(), None)
                .map_err(|e| anyhow::anyhow!("azure: container client: {e}"))?;

        let mut per_try: Vec<Arc<dyn azure_core::http::policies::Policy>> = Vec::new();
        if let Some(token) = credential {
            per_try.push(Arc::new(
                azure_core::http::policies::auth::BearerTokenAuthorizationPolicy::new(
                    token,
                    vec![STORAGE_SCOPE],
                ),
            ));
        }
        let pipeline = azure_core::http::Pipeline::new(
            option_env!("CARGO_PKG_NAME"),
            option_env!("CARGO_PKG_VERSION"),
            azure_core::http::ClientOptions::default(),
            Vec::new(),
            per_try,
            None,
        );

        Ok(AzureStore {
            container: Arc::new(container),
            container_url,
            pipeline,
            multipart_threshold: cfg.multipart_threshold.as_u64(),
            multipart_part_size: cfg.multipart_part_size.as_u64(),
            max_concurrent_blocks: cfg.azure.max_concurrent_blocks.max(1),
        })
    }

    fn blob(&self, key: &str) -> BlobClient {
        self.container.blob_client(key)
    }
}

fn default_credential() -> anyhow::Result<Arc<dyn TokenCredential>> {
    if let Ok(c) = azure_identity::WorkloadIdentityCredential::new(None) {
        return Ok(c);
    }
    if let Ok(c) = azure_identity::ManagedIdentityCredential::new(None) {
        return Ok(c);
    }
    azure_identity::AzureCliCredential::new(None)
        .map(|c| c as Arc<dyn TokenCredential>)
        .map_err(|e| anyhow::anyhow!("azure: no usable credential: {e}"))
}

fn status_of(error: &azure_core::Error) -> Option<StatusCode> {
    match error.kind() {
        azure_core::error::ErrorKind::HttpResponse { status, .. } => Some(*status),
        _ => None,
    }
}

fn map_error(key: &str, error: &azure_core::Error) -> StoreError {
    match status_of(error) {
        Some(StatusCode::NotFound) => StoreError::NotFound { key: key.into() },
        Some(StatusCode::PreconditionFailed | StatusCode::Conflict) => {
            StoreError::PreconditionFailed {
                key: key.into(),
                current: None,
            }
        }
        Some(s) if s.is_server_error() || s == StatusCode::TooManyRequests => {
            StoreError::retryable(anyhow::anyhow!("azure: {key}: {error}"))
        }
        _ => StoreError::other(anyhow::anyhow!("azure: {key}: {error}")),
    }
}

fn version_of(etag: Option<Etag>) -> Version {
    Version::new(etag.map(|e| e.to_string()).unwrap_or_default())
}

fn etag(v: &Version) -> Etag {
    Etag::from(v.as_str())
}

#[async_trait]
impl ObjectStore for AzureStore {
    fn backend(&self) -> &'static str {
        "azure"
    }

    async fn get(&self, key: &str, opts: GetOptions) -> Result<GetResult> {
        let blob = self.blob(key);
        let options = BlobClientDownloadOptions {
            range: opts.range.clone().map(HttpRange::from),
            if_match: opts.if_match.as_ref().map(etag),
            if_none_match: opts.if_none_match.as_ref().map(etag),
            ..Default::default()
        };

        let result = match blob.download(Some(options)).await {
            Ok(r) => r,
            Err(e) if status_of(&e) == Some(StatusCode::NotModified) => {
                let version = opts.if_none_match.clone().unwrap_or_else(|| Version::new(""));
                return Ok(GetResult::NotModified { version });
            }
            Err(e) => return Err(map_error(key, &e)),
        };

        let version = version_of(result.properties.etag.clone());
        let size = result.properties.content_length.unwrap_or_default();
        let key_owned = key.to_owned();
        let body: ByteStream = result
            .body
            .map(move |chunk| chunk.map_err(|e| map_error(&key_owned, &e)))
            .boxed();

        Ok(GetResult::Object {
            meta: ObjectMeta {
                key: key.into(),
                size,
                version,
            },
            body,
        })
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        let blob = self.blob(key);
        match blob
            .get_properties(Some(BlobClientGetPropertiesOptions::default()))
            .await
        {
            Ok(r) => Ok(Some(ObjectMeta {
                key: key.into(),
                size: r.content_length().ok().flatten().unwrap_or_default(),
                version: version_of(r.etag().ok().flatten()),
            })),
            Err(e) if status_of(&e) == Some(StatusCode::NotFound) => Ok(None),
            Err(e) => Err(map_error(key, &e)),
        }
    }

    async fn put(&self, key: &str, body: PutBody, opts: PutOptions) -> Result<ObjectMeta> {
        let blob = self.blob(key);
        let block = blob.block_blob_client();
        match body {
            PutBody::Bytes(bytes) if bytes.len() as u64 <= self.multipart_threshold => {
                let len = bytes.len() as u64;
                let options = BlockBlobClientUploadOptions {
                    if_match: match &opts.mode {
                        PutMode::Update(v) => Some(etag(v)),
                        _ => None,
                    },
                    if_none_match: matches!(opts.mode, PutMode::Create)
                        .then(|| Etag::from("*")),
                    blob_content_type: opts.content_type.map(Into::into),
                    ..Default::default()
                };
                let result = block
                    .upload(bytes.into(), Some(options))
                    .await
                    .map_err(|e| map_error(key, &e))?;
                Ok(ObjectMeta {
                    key: key.into(),
                    size: len,
                    version: version_of(result.etag),
                })
            }
            other => self.put_staged(key, other, opts).await,
        }
    }

    async fn delete(&self, key: &str, if_version: Option<Version>) -> Result<()> {
        let blob = self.blob(key);
        let conditional = if_version.is_some();
        let options = BlobClientDeleteOptions {
            if_match: if_version.as_ref().map(etag),
            ..Default::default()
        };
        match blob.delete(Some(options)).await {
            Ok(_) => Ok(()),
            Err(e) if status_of(&e) == Some(StatusCode::NotFound) => {
                if conditional {
                    Err(StoreError::NotFound { key: key.into() })
                } else {
                    Ok(())
                }
            }
            Err(e) => Err(map_error(key, &e)),
        }
    }

    fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let container = self.container.clone();
        let prefix = prefix.to_owned();
        let start_after = start_after.map(str::to_owned);

        let options = BlobContainerClientListBlobsOptions {
            prefix: Some(prefix.clone()),
            maxresults: Some(MAX_PAGE),
            start_from: start_after.clone(),
            ..Default::default()
        };

        let pager = match container.list_blobs(Some(options)) {
            Ok(p) => p,
            Err(e) => {
                let err = map_error(&prefix, &e);
                return futures::stream::once(async move { Err(err) }).boxed();
            }
        };

        pager
            .into_pages()
            .map(move |page| {
                let prefix = prefix.clone();
                let start_after = start_after.clone();
                async move {
                    let page = page.map_err(|e| map_error(&prefix, &e))?;
                    let body: azure_storage_blob::models::ListBlobsResponse = page
                        .into_body()
                        .xml()
                        .map_err(|e| map_error(&prefix, &e))?;
                    let items = body
                        .blob_items
                        .into_iter()
                        .filter_map(|item| {
                            let name = item.name?;
                            // start_from is inclusive; walgit's start_after is not.
                            if start_after.as_deref() == Some(name.as_str()) {
                                return None;
                            }
                            let props = item.properties?;
                            Some(Ok(ObjectMeta {
                                key: name,
                                size: props.content_length.unwrap_or_default(),
                                version: version_of(props.etag),
                            }))
                        })
                        .collect::<Vec<_>>();
                    Ok::<_, StoreError>(items)
                }
            })
            .buffered(1)
            .map(|r| match r {
                Ok(items) => futures::stream::iter(items).left_stream(),
                Err(e) => futures::stream::once(async move { Err(e) }).right_stream(),
            })
            .flatten()
            .boxed()
    }

    async fn list_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        let mut marker: Option<String> = None;
        loop {
            let (mut prefixes, next) = self.list_delimited(prefix, marker.as_deref()).await?;
            out.append(&mut prefixes);
            match next {
                Some(m) if !m.is_empty() => marker = Some(m),
                _ => break,
            }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    fn supports_compose(&self) -> bool {
        true
    }

    fn compose_is_native(&self) -> bool {
        false
    }

    async fn compose(
        &self,
        dest: &str,
        sources: &[String],
        opts: PutOptions,
    ) -> Result<ObjectMeta> {
        let block = Arc::new(self.blob(dest).block_blob_client());
        let mut blocks = Vec::with_capacity(sources.len());
        let mut total = 0u64;

        for (index, source) in sources.iter().enumerate() {
            let meta = self
                .head(source)
                .await?
                .ok_or_else(|| StoreError::NotFound { key: source.into() })?;
            let id = block_id(index);
            let url = self.container_url.join(source).map_err(|e| {
                StoreError::other(anyhow::anyhow!("azure: compose source {source}: {e}"))
            })?;
            block
                .stage_block_from_url(&id, meta.size, url.to_string(), None)
                .await
                .map_err(|e| map_error(dest, &e))?;
            total += meta.size;
            blocks.push(id);
        }

        self.commit(dest, &block, blocks, total, &opts).await
    }
}

impl AzureStore {
    /// `delimiter` has no generated binding and `ListBlobsResponse` cannot hold
    /// `BlobPrefix`, so the delimited listing goes straight to the REST operation
    /// over the SDK's own pipeline (auth, retry and telemetry still apply).
    async fn list_delimited(
        &self,
        prefix: &str,
        marker: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>)> {
        let mut url = self.container_url.clone();
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("restype", "container");
            q.append_pair("comp", "list");
            q.append_pair("delimiter", "/");
            q.append_pair("prefix", prefix);
            if let Some(m) = marker.filter(|m| !m.is_empty()) {
                q.append_pair("marker", m);
            }
        }

        let mut request = azure_core::http::Request::new(url, azure_core::http::Method::Get);
        request.insert_header("x-ms-version", AZURE_API_VERSION);

        let response = self
            .pipeline
            .send(&azure_core::http::Context::new(), &mut request, None)
            .await
            .map_err(|e| map_error(prefix, &e))?;

        let body = response.into_body();

        parse_blob_prefixes(&body).map_err(|e| {
            StoreError::other(anyhow::anyhow!("azure: {prefix}: malformed listing: {e}"))
        })
    }

    async fn put_staged(&self, key: &str, body: PutBody, opts: PutOptions) -> Result<ObjectMeta> {
        let blob = self.blob(key);
        let block = blob.block_blob_client();

        let chunks = into_chunks(body, self.multipart_part_size).await?;
        let total: u64 = chunks.iter().map(|c| c.len() as u64).sum();
        let blocks: Vec<Vec<u8>> = (0..chunks.len()).map(block_id).collect();

        let block = Arc::new(block);
        futures::stream::iter(blocks.clone().into_iter().zip(chunks))
            .map(|(id, chunk)| {
                let block = block.clone();
                async move {
                    let len = chunk.len() as u64;
                    block
                        .stage_block(&id, len, chunk.into(), None)
                        .await
                        .map_err(|e| map_error(key, &e))
                }
            })
            .buffer_unordered(self.max_concurrent_blocks)
            .try_collect::<Vec<_>>()
            .await?;

        self.commit(key, &block, blocks, total, &opts).await
    }

    /// Staged blocks are invisible until committed, so the condition rides on the
    /// commit alone — a CAS never observes a half-written object.
    async fn commit(
        &self,
        key: &str,
        block: &azure_storage_blob::BlockBlobClient,
        blocks: Vec<Vec<u8>>,
        total: u64,
        opts: &PutOptions,
    ) -> Result<ObjectMeta> {
        let lookup = BlockLookupList {
            latest: Some(blocks),
            ..Default::default()
        };
        let options = BlockBlobClientCommitBlockListOptions {
            if_match: match &opts.mode {
                PutMode::Update(v) => Some(etag(v)),
                _ => None,
            },
            if_none_match: matches!(opts.mode, PutMode::Create).then(|| Etag::from("*")),
            blob_content_type: opts.content_type.map(Into::into),
            ..Default::default()
        };
        let result = block
            .commit_block_list(lookup.try_into().map_err(|e| {
                StoreError::other(anyhow::anyhow!("azure: {key}: block list: {e}"))
            })?, Some(options))
            .await
            .map_err(|e| map_error(key, &e))?;

        Ok(ObjectMeta {
            key: key.into(),
            size: total,
            version: version_of(result.etag().ok().flatten()),
        })
    }
}

fn block_id(index: usize) -> Vec<u8> {
    format!("{index:08}").into_bytes()
}

fn parse_blob_prefixes(body: &[u8]) -> anyhow::Result<(Vec<String>, Option<String>)> {
    use quick_xml::events::Event;

    let mut reader = quick_xml::Reader::from_reader(body);
    reader.config_mut().trim_text(true);

    let mut prefixes = Vec::new();
    let mut next_marker = None;
    let mut in_blob_prefix = false;
    let mut in_name = false;
    let mut in_next_marker = false;
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf)? {
            Event::Start(e) => match e.local_name().as_ref() {
                b"BlobPrefix" => in_blob_prefix = true,
                b"Name" if in_blob_prefix => in_name = true,
                b"NextMarker" => in_next_marker = true,
                _ => {}
            },
            Event::End(e) => match e.local_name().as_ref() {
                b"BlobPrefix" => in_blob_prefix = false,
                b"Name" => in_name = false,
                b"NextMarker" => in_next_marker = false,
                _ => {}
            },
            Event::Text(t) => {
                if in_name {
                    prefixes.push(t.decode()?.into_owned());
                } else if in_next_marker {
                    let m = t.decode()?.into_owned();
                    if !m.is_empty() {
                        next_marker = Some(m);
                    }
                }
            }
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    Ok((prefixes, next_marker))
}

async fn into_chunks(body: PutBody, part: u64) -> Result<Vec<Bytes>> {
    let part = usize::try_from(part.max(1)).unwrap_or(usize::MAX);
    let mut out = Vec::new();
    match body {
        PutBody::Bytes(mut bytes) => {
            while !bytes.is_empty() {
                let n = part.min(bytes.len());
                out.push(bytes.split_to(n));
            }
        }
        PutBody::Stream { mut stream, .. } => {
            let mut acc = BytesMut::new();
            while let Some(chunk) = stream.next().await {
                acc.extend_from_slice(&chunk?);
                while acc.len() >= part {
                    out.push(acc.split_to(part).freeze());
                }
            }
            if !acc.is_empty() {
                out.push(acc.freeze());
            }
        }
        PutBody::File(path) => {
            let bytes = tokio::fs::read(&path)
                .await
                .map_err(|e| StoreError::other(anyhow::anyhow!("azure: read {}: {e}", path.display())))?;
            let mut bytes = Bytes::from(bytes);
            while !bytes.is_empty() {
                let n = part.min(bytes.len());
                out.push(bytes.split_to(n));
            }
        }
    }
    if out.is_empty() {
        out.push(Bytes::new());
    }
    Ok(out)
}
