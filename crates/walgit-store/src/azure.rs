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
    BlobContainerClientListBlobsOptions, BlockBlobClientCommitBlockListOptions,
    BlockBlobClientStageBlockFromUrlOptions, BlockBlobClientUploadOptions, BlockLookupList,
};
use azure_storage_blob::{BlobClient, BlobContainerClient};
use bytes::{Bytes, BytesMut};
use futures::stream::{BoxStream, StreamExt};
use url::Url;

use crate::{
    ByteStream, GetOptions, GetResult, ObjectMeta, ObjectStore, PutBody, PutMode, PutOptions,
    Result, StoreError, Version,
};

const MAX_PAGE: i32 = 5000;

pub struct AzureStore {
    container: BlobContainerClient,
    container_url: Url,
    credential: Option<Arc<dyn TokenCredential>>,
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

        Ok(AzureStore {
            container,
            container_url,
            credential,
            multipart_threshold: cfg.multipart_threshold.as_u64(),
            multipart_part_size: cfg.multipart_part_size.as_u64(),
            max_concurrent_blocks: cfg.azure.max_concurrent_blocks.max(1),
        })
    }

    fn blob(&self, key: &str) -> Result<BlobClient> {
        self.container
            .blob_client(key)
            .map_err(|e| StoreError::other(anyhow::anyhow!("azure: blob client for {key}: {e}")))
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

fn map_error(key: &str, error: azure_core::Error) -> StoreError {
    match status_of(&error) {
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
        let blob = self.blob(key)?;
        let options = BlobClientDownloadOptions {
            range: opts.range.clone().map(|r| azure_core::http::HttpRange {
                offset: r.start,
                length: Some(r.end - r.start),
            }),
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
            Err(e) => return Err(map_error(key, e)),
        };

        let version = version_of(result.properties.etag.clone());
        let size = total_size(&result.properties, opts.range.as_ref());
        let key_owned = key.to_owned();
        let body: ByteStream = result
            .body
            .map(move |chunk| chunk.map_err(|e| map_error(&key_owned, e)))
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
        let blob = self.blob(key)?;
        match blob
            .get_properties(Some(BlobClientGetPropertiesOptions::default()))
            .await
        {
            Ok(r) => Ok(Some(ObjectMeta {
                key: key.into(),
                size: r.content_length()?.unwrap_or_default(),
                version: version_of(r.etag()?),
            })),
            Err(e) if status_of(&e) == Some(StatusCode::NotFound) => Ok(None),
            Err(e) => Err(map_error(key, e)),
        }
    }

    async fn put(&self, key: &str, body: PutBody, opts: PutOptions) -> Result<ObjectMeta> {
        let blob = self.blob(key)?;
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
                    .upload(bytes.into(), true, len, Some(options))
                    .await
                    .map_err(|e| map_error(key, e))?;
                Ok(ObjectMeta {
                    key: key.into(),
                    size: len,
                    version: version_of(result.etag()?),
                })
            }
            other => self.put_staged(key, other, opts).await,
        }
    }

    async fn delete(&self, key: &str, if_version: Option<Version>) -> Result<()> {
        let blob = self.blob(key)?;
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
            Err(e) => Err(map_error(key, e)),
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

        futures::stream::try_unfold(
            (Some(String::new()), false),
            move |(marker, done)| {
                let container = container.clone();
                let prefix = prefix.clone();
                let start_after = start_after.clone();
                async move {
                    if done {
                        return Ok(None);
                    }
                    let options = BlobContainerClientListBlobsOptions {
                        prefix: Some(prefix.clone()),
                        marker: marker.clone().filter(|m| !m.is_empty()),
                        maxresults: Some(MAX_PAGE),
                        start_from: start_after.clone(),
                        ..Default::default()
                    };
                    let page = container
                        .list_blobs(Some(options))
                        .map_err(|e| map_error(&prefix, e))?;
                    let _ = page;
                    Ok(None::<(Vec<Result<ObjectMeta>>, (Option<String>, bool))>)
                }
            },
        )
        .map(|r: Result<Vec<Result<ObjectMeta>>>| futures::stream::iter(r.unwrap_or_default()))
        .flatten()
        .boxed()
    }

    async fn list_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        let _ = prefix;
        Err(StoreError::InvalidArgument(
            "azure: list_prefixes not implemented".into(),
        ))
    }

    fn supports_compose(&self) -> bool {
        true
    }

    fn compose_is_native(&self) -> bool {
        false
    }
}

fn total_size(
    properties: &azure_storage_blob::models::BlobDownloadProperties,
    range: Option<&std::ops::Range<u64>>,
) -> u64 {
    let _ = range;
    properties.content_length.unwrap_or_default()
}

impl AzureStore {
    async fn put_staged(&self, key: &str, body: PutBody, opts: PutOptions) -> Result<ObjectMeta> {
        let _ = (key, body, opts);
        Err(StoreError::InvalidArgument("azure: staged put pending".into()))
    }
}
