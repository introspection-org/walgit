//! Load a checked Rust notify transport, or export one.
//!
//! walgit ships no transport and depends on no client library for one. A
//! deployment whose bucket cannot notify the events bridge installs a shared
//! library that can — GCP Pub/Sub, Redis pub/sub, NATS, or whatever it already
//! runs — and walgit moves object names across a checked boundary.
pub mod abi;
mod bridge;

use abi_stable::library::lib_header_from_path;
use anyhow::{Context, Result};
use std::path::Path;

pub use abi_stable;
pub use bridge::{NotifySource, RemoteNotify, export};

/// Load administrator-selected native code with checked module/type layouts.
/// Like the storage SDK, load each path separately; the root-module singleton
/// convenience loader would incorrectly reuse the first module for every path.
/// `abi_stable` retains mappings until process exit. Hot unloading is unsupported.
pub async fn load(path: &Path, config: serde_json::Value) -> Result<RemoteNotify> {
    let path = path.to_owned();
    let config = serde_json::to_vec(&config)?;
    anyhow::ensure!(
        config.len() <= abi::MAX_METADATA,
        "plugin options exceed limit"
    );
    tokio::task::spawn_blocking(move || {
        let header = lib_header_from_path(&path).context("loading notify plugin")?;
        let module: abi::PluginRef = header
            .init_root_module()
            .context("checking notify plugin ABI")?;
        let endpoint = (module.create())(config.into())
            .into_result()
            .map_err(|error| anyhow::anyhow!("notify plugin initialization failed: {error}"))?;
        Ok::<RemoteNotify, anyhow::Error>(RemoteNotify::new(endpoint))
    })
    .await
    .context("notify plugin loader task failed")?
}

/// Export an async factory `serde_json::Value -> Result<impl NotifySource>`.
/// The implementation crate must depend on `abi_stable` 0.11, like this SDK.
#[macro_export]
macro_rules! export_notify_plugin {
    ($factory:path) => {
        #[allow(unsafe_code)]
        #[abi_stable::export_root_module]
        pub fn get_library() -> $crate::abi::PluginRef {
            use $crate::abi_stable::prefix_type::PrefixTypeTrait;
            struct Factory;
            impl Factory {
                extern "C" fn create(
                    config: $crate::abi_stable::std_types::RVec<u8>,
                ) -> $crate::abi_stable::std_types::RResult<
                    $crate::abi::Notify,
                    $crate::abi_stable::std_types::RString,
                > {
                    $crate::export(config, $factory)
                }
            }
            $crate::abi::Plugin {
                create: Factory::create,
            }
            .leak_into_prefix()
        }
    };
}
