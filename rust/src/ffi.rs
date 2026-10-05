//! Small C ABI. Input bytes are copied before returning to Dart.
use crate::{
    codec::Error,
    engine,
    event::{PortSink, Post},
};
use std::sync::Arc;

fn boundary(f: impl FnOnce() -> Result<u64, Error>) -> i64 {
    match crate::callback_boundary::invoke(f, || Error::new(18, "Native boundary panicked")) {
        Ok(value) => value as i64,
        Err(error) => -(error.code as i64),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn bletide_abi_version() -> i64 {
    boundary(|| Ok(2))
}

/// # Safety
/// `post` must be NativeApi.postCObject, valid for the lifetime of the Dart VM.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bletide_open(port: i64, post: Option<Post>) -> i64 {
    boundary(|| {
        let post = post.ok_or_else(|| Error::new(18, "Missing native port function"))?;
        if port <= 0 {
            return Err(Error::new(16, "Invalid native port"));
        }
        engine::open(Arc::new(PortSink { port, post }))
    })
}

/// Internal hardware-free driver, compiled only for contract tests.
/// # Safety
/// `post` must be NativeApi.postCObject for the lifetime of the Dart VM.
#[cfg(feature = "test-support")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bletide_open_fixture(port: i64, post: Option<Post>) -> i64 {
    boundary(|| {
        let post = post.ok_or_else(|| Error::new(18, "Missing native port function"))?;
        if port <= 0 {
            return Err(Error::new(16, "Invalid native port"));
        }
        engine::open_fixture(Arc::new(PortSink { port, post }))
    })
}

/// # Safety
/// If length is nonzero, payload must reference that many readable bytes for
/// this synchronous call. No pointer is retained after the function returns.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bletide_command(
    handle: u64,
    operation: u32,
    payload: *const u8,
    length: u32,
    timeout_ms: u32,
) -> i64 {
    boundary(|| {
        if length > 1_048_576 || (length != 0 && payload.is_null()) {
            return Err(Error::new(16, "Invalid payload"));
        }
        let bytes = if length == 0 {
            Vec::new()
        } else {
            // SAFETY: the caller guarantees readable input; bounds were checked above.
            unsafe { std::slice::from_raw_parts(payload, length as usize).to_vec() }
        };
        engine::submit(handle, operation, bytes, timeout_ms)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn bletide_cancel(handle: u64, request: u64) -> i64 {
    boundary(|| engine::cancel(handle, request).map(|()| 0))
}

#[unsafe(no_mangle)]
pub extern "C" fn bletide_close(handle: u64) -> i64 {
    boundary(|| engine::close(handle))
}

// Internal debug ABI; not exported by the public Dart library.
#[cfg(any(debug_assertions, feature = "test-support"))]
#[unsafe(no_mangle)]
pub extern "C" fn bletide_resource_counts() -> i64 {
    boundary(|| {
        let (engines, requests) = engine::counts();
        Ok(((engines as u64) << 32) | requests as u64)
    })
}

// Internal counters: 0 adapter workers, 1 connection workers, 2 notification
// streams, 3 subscriptions. They are absent from consumer release libraries.
#[cfg(any(debug_assertions, feature = "test-support"))]
#[unsafe(no_mangle)]
pub extern "C" fn bletide_worker_resource_count(kind: u32) -> i64 {
    boundary(|| {
        crate::resources::total(kind)
            .ok_or_else(|| crate::codec::Error::new(16, "Unknown resource counter"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn malformed_input_and_unknown_handles_are_controlled() {
        // SAFETY: null inputs are explicitly rejected before dereference.
        unsafe {
            assert_eq!(bletide_open(0, None), -18);
            assert_eq!(bletide_command(0, 1, std::ptr::null(), 1, 1), -16);
            assert_eq!(bletide_command(0, 1, std::ptr::null(), 0, 1), -17);
        }
        assert_eq!(bletide_cancel(0, 0), 0);
        assert_eq!(bletide_close(0), 0);
    }
    #[test]
    fn payload_limit_rejects_oversize_before_engine_lookup() {
        let bytes = vec![0u8; 1_048_577];
        // SAFETY: both declared lengths fit the live allocation. The oversized
        // call must fail validation; the limit-sized call reaches handle lookup.
        unsafe {
            assert_eq!(bletide_command(0, 1, bytes.as_ptr(), 1_048_577, 1), -16);
            assert_eq!(bletide_command(0, 1, bytes.as_ptr(), 1_048_576, 1), -17);
        }
    }
    #[test]
    fn unwind_is_contained() {
        assert_eq!(boundary(|| panic!("test panic")), -18);
    }
    #[test]
    fn c_export_policy_contains_a_panicking_payload_destructor() {
        struct Payload;
        impl Drop for Payload {
            fn drop(&mut self) {
                panic!("payload destructor failure");
            }
        }
        extern "C" fn export_entry() -> i64 {
            boundary(|| std::panic::panic_any(Payload))
        }
        assert_eq!(export_entry(), -18);
    }
}
