//! The sole native-to-Dart path. Ordinary typed data is copied by the VM
//! before PostCObject returns, including on failed delivery. Nothing is retained.
use std::sync::Arc;

#[repr(C)]
#[derive(Clone, Copy)]
struct TypedData {
    kind: i32,
    length: isize,
    data: *const u8,
}

#[repr(C)]
union Value {
    typed: TypedData,
    // Dart_CObject's largest union member is ExternalTypedData (five words).
    // This storage also preserves the alignment of the other union members.
    storage: [usize; 5],
    integer: i64,
    double: f64,
}

#[repr(C)]
pub(crate) struct CObject {
    kind: i32,
    value: Value,
}

pub(crate) type Post = unsafe extern "C" fn(i64, *mut CObject) -> bool;

pub(crate) trait EventSink: Send + Sync {
    fn send(&self, event: &[u8]) -> bool;
}

pub(crate) type Sink = Arc<dyn EventSink>;

pub(crate) struct PortSink {
    pub port: i64,
    pub post: Post,
}

impl EventSink for PortSink {
    fn send(&self, event: &[u8]) -> bool {
        let mut object = CObject {
            kind: 7, // Dart_CObject_kTypedData
            value: Value {
                typed: TypedData {
                    kind: 2, // Dart_TypedData_kUint8
                    length: event.len() as isize,
                    data: event.as_ptr(),
                },
            },
        };
        // SAFETY: post is NativeApi.postCObject, valid for the VM lifetime.
        // The object and byte slice remain live for this synchronous call.
        // kTypedData copies bytes; Rust retains ownership on either result.
        unsafe { (self.post)(self.port, &mut object) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static COPIED: Mutex<Vec<u8>> = Mutex::new(Vec::new());
    unsafe extern "C" fn copy(_port: i64, object: *mut CObject) -> bool {
        // SAFETY: the test calls this only through PortSink with a live object.
        let object = unsafe { &*object };
        assert_eq!(object.kind, 7);
        // SAFETY: PortSink initialized the typed union member.
        let typed = unsafe { object.value.typed };
        assert_eq!(typed.kind, 2);
        // SAFETY: PortSink guarantees this slice for the duration of the call.
        *COPIED.lock().unwrap() =
            unsafe { std::slice::from_raw_parts(typed.data, typed.length as usize).to_vec() };
        false // Exercise destination unavailable, with no transferred allocation.
    }

    #[test]
    fn failed_delivery_retains_no_native_memory() {
        let sink = PortSink {
            port: 1,
            post: copy,
        };
        let bytes = vec![0, 255, 128];
        assert!(!sink.send(&bytes));
        drop(bytes);
        assert_eq!(*COPIED.lock().unwrap(), vec![0, 255, 128]);
    }

    #[test]
    fn cobject_layout_matches_dart_header() {
        assert_eq!(
            std::mem::offset_of!(CObject, value),
            std::mem::align_of::<Value>()
        );
        assert_eq!(
            std::mem::size_of::<CObject>(),
            std::mem::align_of::<Value>() + std::mem::size_of::<Value>()
        );
        assert_eq!(
            std::mem::size_of::<TypedData>(),
            3 * std::mem::size_of::<usize>()
        );
    }
}
