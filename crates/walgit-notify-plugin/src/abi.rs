//! Checked Rust plugin boundary. Ownership and layouts are provided by `abi_stable`.
// These allowances apply only to generated ABI glue, not hand-written unsafe code.
#![allow(
    unsafe_code,
    non_local_definitions,
    clippy::used_underscore_binding,
    clippy::expl_impl_clone_on_copy
)]

use abi_stable::std_types::{RBox, ROption, RResult, RString, RVec};
use abi_stable::{
    StableAbi, library::RootModule, package_version_strings, sabi_trait, sabi_types::VersionStrings,
};

pub const MAX_KEY: usize = 4 * 1024;
pub const MAX_METADATA: usize = 16 * 1024 * 1024;

/// A concurrent endpoint, kept alive until the subscription ends. Methods are
/// synchronous; the SDK handles blocking dispatch and panic errors.
#[sabi_trait]
pub trait NotifyApi: Send + Sync {
    /// Named as a bucket notification names it: full object name, prefix included.
    fn publish(&self, key: RString) -> RResult<(), RString>;
    /// Blocks for the next announcement; `RNone` ends the subscription. An
    /// error means messages were lost AND the transport has already recovered
    /// — reporting first reopens the gap under the host's reconciliation.
    #[sabi(last_prefix_field)]
    fn next(&self) -> RResult<ROption<RString>, RString>;
}
pub type Notify = NotifyApi_TO<'static, RBox<()>>;

#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix(prefix_ref = PluginRef)))]
pub struct Plugin {
    #[sabi(last_prefix_field)]
    pub create: extern "C" fn(RVec<u8>) -> RResult<Notify, RString>,
}

impl RootModule for PluginRef {
    abi_stable::declare_root_module_statics! {PluginRef}
    const BASE_NAME: &'static str = "walgit_notify_plugin";
    const NAME: &'static str = "walgit_notify_plugin";
    const VERSION_STRINGS: VersionStrings = package_version_strings!();
}
