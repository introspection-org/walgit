//! Version 1 C ABI. See `docs/STORAGE_PLUGINS.md` for ownership and threading.
//! No Rust allocation, trait object, future or panic crosses this boundary.
#![allow(unsafe_code)]

use std::ffi::c_void;

pub const ABI_VERSION: u32 = 1;
pub const MAX_FRAME: usize = 1024 * 1024;
pub const MAX_METADATA: usize = 16 * 1024 * 1024;

/// Borrowed for the duration of a call. Null is valid only when len is zero.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Slice {
    pub ptr: *const u8,
    pub len: usize,
}

impl From<&[u8]> for Slice {
    fn from(bytes: &[u8]) -> Self {
        Self {
            ptr: bytes.as_ptr(),
            len: bytes.len(),
        }
    }
}

/// Owned bytes. The receiver calls this allocation's release function once.
#[repr(C)]
pub struct Buffer {
    pub ptr: *mut u8,
    pub len: usize,
    pub release: Option<unsafe extern "C" fn(*mut u8, usize)>,
}

impl Default for Buffer {
    fn default() -> Self {
        Self {
            ptr: std::ptr::null_mut(),
            len: 0,
            release: None,
        }
    }
}

/// Owned, pull-based stream. next: 0 = EOF, 1 = bytes, -1 = JSON `WireError`.
/// next is serialized per stream; distinct streams may be called concurrently.
/// release must cancel/drop the stream and never consume its remaining bytes.
#[repr(C)]
pub struct Stream {
    pub context: *mut c_void,
    pub next: Option<unsafe extern "C" fn(*mut c_void, *mut Buffer) -> i32>,
    pub release: Option<unsafe extern "C" fn(*mut c_void)>,
}

impl Default for Stream {
    fn default() -> Self {
        Self {
            context: std::ptr::null_mut(),
            next: None,
            release: None,
        }
    }
}

#[repr(C)]
pub struct Request {
    pub metadata: Slice,
    /// Ownership transfers to `request()`,  even when the operation fails.
    pub body: Stream,
}

#[repr(C)]
#[derive(Default)]
pub struct Reply {
    /// 0 = success; -1 = metadata contains `WireError`. Other values are invalid.
    pub status: i32,
    pub metadata: Buffer,
    pub body: Stream,
}

/// An owned object-store endpoint. `request()` is concurrent and synchronous;
/// callers must run it off their async executor. release follows all calls and
/// all response streams. Every function must contain its own panic boundary.
#[repr(C)]
pub struct Store {
    pub context: *mut c_void,
    pub request: unsafe extern "C" fn(*mut c_void, Request, *mut Reply),
    pub release: unsafe extern "C" fn(*mut c_void),
    pub supports_compose: u8,
    pub compose_is_native: u8,
}

#[repr(C)]
pub struct PluginHeader {
    pub abi_version: u32,
    pub struct_size: u32,
}

#[repr(C)]
pub struct Plugin {
    pub abi_version: u32,
    pub struct_size: u32,
    /// Consumes inner on success AND failure. Config is borrowed JSON.
    /// On success writes out; on failure writes error and leaves out untouched.
    pub create: unsafe extern "C" fn(Store, Slice, *mut Store, *mut Buffer) -> i32,
}

// SAFETY: V1 endpoints must support concurrent calls; their opaque context is
// never dereferenced by callers. Stream ownership enforces serialized polling.
unsafe impl Send for Store {}
// SAFETY: Same concurrent request contract as Send.
unsafe impl Sync for Store {}
// SAFETY: Stream handles transfer ownership; each handle is polled serially.
unsafe impl Send for Stream {}
// SAFETY: Owned buffer and its release function may move together across threads.
unsafe impl Send for Buffer {}

/// # Safety
/// The caller guarantees the slice addresses len readable bytes for this call.
pub unsafe fn read_slice<'a>(slice: Slice, max: usize) -> anyhow::Result<&'a [u8]> {
    anyhow::ensure!(slice.len <= max, "plugin message exceeds limit");
    if slice.len == 0 {
        return Ok(&[]);
    }
    anyhow::ensure!(!slice.ptr.is_null(), "null plugin message");
    // SAFETY: Required readable range is the caller's V1 contract.
    Ok(unsafe { std::slice::from_raw_parts(slice.ptr, slice.len) })
}

unsafe extern "C" fn release_buffer(ptr: *mut u8, len: usize) {
    // SAFETY: Only called once for the boxed slice produced by own_buffer.
    drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, len)) });
}

pub fn own_buffer(bytes: Vec<u8>) -> Buffer {
    let bytes = bytes.into_boxed_slice();
    let len = bytes.len();
    Buffer {
        ptr: Box::into_raw(bytes).cast(),
        len,
        release: Some(release_buffer),
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            // SAFETY: Buffer owns its allocation and uses its allocating module.
            unsafe { release(self.ptr, self.len) };
        }
    }
}

impl Drop for Stream {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            // SAFETY: This handle owns its context; no poll overlaps drop.
            unsafe { release(self.context) };
        }
    }
}
