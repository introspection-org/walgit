//! Load a trusted storage cdylib, or export an external `ObjectStore` decorator.
//! The ABI is C-compatible; the Rust `ObjectStore` trait stays inside each module.
#![allow(unsafe_code)]

pub mod abi;
mod bridge;
mod wire;

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use libloading::Library;
use walgit_store::DynStore;

pub use bridge::export;

/// Load a library selected by deployment configuration, never by a request.
/// Libraries remain mapped until process exit: channels and runtime teardown may
/// execute module code after the last request. Hot unload/reload is unsupported.
pub async fn load(path: &Path, config: serde_json::Value, inner: DynStore) -> Result<DynStore> {
    let path = path.to_owned();
    let runtime = tokio::runtime::Handle::current();
    tokio::task::spawn_blocking(move || {
        // SAFETY: The operator has selected trusted executable code for this
        // process. Versioned entry and size are checked before creating a store.
        let library = unsafe { Library::new(&path) }.context("loading storage plugin")?;
        // SAFETY: V1 symbol must have the published C signature.
        let entry = unsafe {
            library.get::<unsafe extern "C" fn() -> *const abi::Plugin>(b"walgit_store_plugin_v1\0")
        }
        .context("storage plugin has no V1 entry point")?;
        // SAFETY: Calling the documented entry point of the trusted library.
        let descriptor = unsafe { entry() };
        anyhow::ensure!(!descriptor.is_null(), "null storage plugin descriptor");
        // SAFETY: Every entry must expose the fixed two-u32 header, including
        // incompatible descriptors. Do not read the function table until checked.
        let header = unsafe { &*descriptor.cast::<abi::PluginHeader>() };
        anyhow::ensure!(
            header.abi_version == abi::ABI_VERSION,
            "unsupported storage plugin ABI"
        );
        anyhow::ensure!(
            usize::try_from(header.struct_size)? == std::mem::size_of::<abi::Plugin>(),
            "storage plugin ABI size mismatch"
        );
        // SAFETY: The trusted descriptor now declares the exact V1 layout.
        let descriptor = unsafe { &*descriptor };
        let create = descriptor.create;
        // Only the shared code mapping is retained. Stores, streams and buffers
        // still have explicit ownership and are destroyed normally.
        let _mapped = Box::leak(Box::new(library));
        let host = bridge::expose(inner, runtime, None);
        let config = serde_json::to_vec(&config)?;
        let mut out = std::mem::MaybeUninit::<abi::Store>::uninit();
        let mut error = abi::Buffer::default();
        // SAFETY: Inputs live through create; create consumes host even on error.
        let status = unsafe {
            create(
                host,
                config.as_slice().into(),
                out.as_mut_ptr(),
                &raw mut error,
            )
        };
        anyhow::ensure!(
            status == 0,
            "storage plugin initialization failed: {}",
            bridge::error_text(&error)
        );
        // SAFETY: A successful V1 create initializes out.
        let endpoint = unsafe { out.assume_init() };
        Ok::<DynStore, anyhow::Error>(Arc::new(bridge::RemoteStore::new(endpoint)))
    })
    .await
    .context("storage plugin loader task failed")?
}

/// Export a factory async function `(DynStore, serde_json::Value) -> Result<DynStore>`.
/// The factory receives the configured backend with its global prefix already
/// applied. Keys visible to the decorator are logical, unprefixed object keys.
#[macro_export]
macro_rules! export_plugin {
    ($factory:path) => {
        #[allow(unsafe_code)]
        unsafe extern "C" fn walgit_plugin_create(
            inner: $crate::abi::Store,
            config: $crate::abi::Slice,
            out: *mut $crate::abi::Store,
            error: *mut $crate::abi::Buffer,
        ) -> i32 {
            // SAFETY: Forward the V1 loader's documented ownership contract.
            unsafe { $crate::export(inner, config, out, error, $factory) }
        }
        #[allow(unsafe_code)]
        #[unsafe(no_mangle)]
        pub extern "C" fn walgit_store_plugin_v1() -> *const $crate::abi::Plugin {
            static PLUGIN: $crate::abi::Plugin = $crate::abi::Plugin {
                abi_version: $crate::abi::ABI_VERSION,
                struct_size: std::mem::size_of::<$crate::abi::Plugin>() as u32,
                create: walgit_plugin_create,
            };
            &PLUGIN
        }
    };
}
