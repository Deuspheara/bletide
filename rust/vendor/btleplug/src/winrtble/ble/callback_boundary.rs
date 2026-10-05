//! Catch unwinding and safely release caught panic payloads.
use std::panic::{AssertUnwindSafe, catch_unwind};

pub(crate) fn invoke<T, E>(
    callback: impl FnOnce() -> Result<T, E>,
    panic_error: impl FnOnce() -> E,
) -> Result<T, E> {
    recover(catch_unwind(AssertUnwindSafe(callback)), || {
        Err(panic_error())
    })
}

/// Recover a caught synchronous or asynchronous panic without unguarded payload disposal.
pub(crate) fn recover<T>(result: std::thread::Result<T>, panic_value: impl FnOnce() -> T) -> T {
    match result {
        Ok(value) => value,
        Err(payload) => {
            // A caught panic payload can have a panicking destructor. Guard its
            // disposal too; the ordinary secondary string payload is released.
            if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload)))
                && let Err(_unrecoverable) = catch_unwind(AssertUnwindSafe(|| drop(secondary)))
            {
                // Repeatedly panicking destructors are unrecoverable. Abort
                // without unwinding through the foreign ABI or leaking a
                // retained payload into an otherwise continuing process.
                std::process::abort();
            }
            panic_value()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn repeated_payload_destructor_panics_abort_in_a_child_process() {
        const PROBE: &str = "OPENBLE_PANIC_DISPOSAL_PROBE";
        if std::env::var_os(PROBE).is_some() {
            struct First;
            struct Second;
            impl Drop for First {
                fn drop(&mut self) {
                    std::panic::panic_any(Second);
                }
            }
            impl Drop for Second {
                fn drop(&mut self) {
                    panic!("second panic payload destructor failed");
                }
            }
            let _ = invoke(|| -> Result<(), ()> { std::panic::panic_any(First) }, || ());
            // Returning would make this child test succeed and fail the parent
            // assertion. The unrecoverable branch must terminate the process.
            return;
        }
        let module = module_path!().split_once("::").unwrap().1;
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &format!("{module}::repeated_payload_destructor_panics_abort_in_a_child_process"),
                "--nocapture",
            ])
            .env(PROBE, "1")
            .output()
            .unwrap();
        assert!(
            !output.status.success(),
            "Fatal disposal unexpectedly returned"
        );
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("second panic payload destructor failed")
        );
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(output.status.signal(), Some(6), "Expected SIGABRT");
        }
    }

    #[test]
    fn success_and_original_error_are_preserved() {
        assert_eq!(invoke(|| Ok::<_, &str>(42), || "panic"), Ok(42));
        assert_eq!(
            invoke(|| Err::<(), _>("query failed"), || "panic"),
            Err("query failed")
        );
    }

    #[test]
    fn panic_is_caught_before_returning_across_a_foreign_abi() {
        extern "C" fn foreign_callback() -> i32 {
            match invoke(
                || -> Result<(), ()> { panic!("foreign callback failure") },
                || (),
            ) {
                Ok(()) => 0,
                Err(()) => -1,
            }
        }
        assert_eq!(foreign_callback(), -1);
    }

    #[test]
    fn panic_payload_destructor_cannot_unwind_through_the_foreign_abi() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        struct Payload(Arc<AtomicUsize>);
        impl Drop for Payload {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
                panic!("panic payload destructor failure");
            }
        }
        extern "C" fn foreign_callback(count: &Arc<AtomicUsize>) -> i32 {
            match invoke(
                || -> Result<(), ()> {
                    std::panic::panic_any(Payload(count.clone()));
                },
                || (),
            ) {
                Ok(()) => 0,
                Err(()) => -1,
            }
        }
        let count = Arc::new(AtomicUsize::new(0));
        assert_eq!(foreign_callback(&count), -1);
        assert_eq!(count.load(Ordering::Relaxed), 1);
        assert_eq!(Arc::strong_count(&count), 1);
    }

    #[test]
    fn callback_panic_returns_an_error_and_drops_owned_guards() {
        let owner = Mutex::new(0);
        let result = invoke(
            || -> Result<(), &str> {
                let mut guard = owner.lock().unwrap();
                *guard = 1;
                panic!("injected callback failure");
            },
            || "callback panicked",
        );
        assert_eq!(result, Err("callback panicked"));
        // The guard must have unwound before the boundary returns; poison is
        // explicit here rather than mistaking it for a permanently held lock.
        match owner.try_lock() {
            Err(std::sync::TryLockError::Poisoned(error)) => assert_eq!(*error.into_inner(), 1),
            _ => panic!("callback guard was not released and poisoned"),
        }
    }
}
