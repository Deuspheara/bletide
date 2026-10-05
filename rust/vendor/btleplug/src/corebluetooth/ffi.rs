#![allow(non_camel_case_types)]
use std::os::raw::{c_char, c_void};

pub type dispatch_object_s = c_void;
pub type dispatch_queue_t = *mut dispatch_object_s;
pub type dispatch_queue_attr_t = *const dispatch_object_s;

pub const DISPATCH_QUEUE_SERIAL: dispatch_queue_attr_t = 0 as dispatch_queue_attr_t;

unsafe extern "C" {
    fn dispatch_sync_f(
        queue: dispatch_queue_t,
        context: *mut c_void,
        work: unsafe extern "C" fn(*mut c_void),
    );
    #[cfg(test)]
    pub(super) fn dispatch_async_f(
        queue: dispatch_queue_t,
        context: *mut c_void,
        work: unsafe extern "C" fn(*mut c_void),
    );
    pub fn dispatch_release(object: dispatch_queue_t);
    pub fn dispatch_queue_create(
        label: *const c_char,
        attr: dispatch_queue_attr_t,
    ) -> dispatch_queue_t;
}

// TODO: Do we need to link to AppKit here?
#[cfg_attr(target_os = "macos", link(name = "AppKit", kind = "framework"))]
unsafe extern "C" {}

// The queue created through this raw C API is not managed by Objective-C ARC.
// Keep its creation reference until the owning CoreBluetooth state drops.
pub(super) struct OwnedQueue(pub(super) dispatch_queue_t);
impl OwnedQueue {
    // Called by the object-owning thread, never from this serial callback queue.
    // The receiver must be closed first so a callback blocked on send can finish.
    pub(super) fn drain(&self) {
        unsafe extern "C" fn barrier(_: *mut c_void) {}
        unsafe { dispatch_sync_f(self.0, std::ptr::null_mut(), barrier) };
    }
}
impl Drop for OwnedQueue {
    fn drop(&mut self) {
        unsafe {
            dispatch_release(self.0);
        }
    }
}
