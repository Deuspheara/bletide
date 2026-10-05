//! Notify the owner of a failed foreign callback without changing its result.
use super::callback_boundary::{invoke, recover};
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Cache the first native cause even when there are no receivers.
pub(crate) fn record_first_failure<E: Clone>(
    failure: &tokio::sync::watch::Sender<Option<E>>,
    error: &E,
) {
    failure.send_if_modified(|cause| {
        if cause.is_some() {
            return false;
        }
        *cause = Some(error.clone());
        true
    });
}

/// Observe an already-cached failure as well as one arriving during setup.
pub(crate) async fn wait_for_failure<E: Clone>(
    mut failure: tokio::sync::watch::Receiver<Option<E>>,
) -> Option<E> {
    loop {
        let cached = failure.borrow_and_update().clone();
        if cached.is_some() {
            return cached;
        }
        if failure.changed().await.is_err() {
            return None;
        }
    }
}

/// Report callback failure to its owner while preserving the foreign return value.
pub(crate) fn invoke_reported<T, E>(
    callback: impl FnOnce() -> Result<T, E>,
    panic_error: impl FnOnce() -> E,
    report_error: impl FnOnce(&E),
) -> Result<T, E> {
    let result = invoke(callback, panic_error);
    if let Err(error) = &result {
        // Reporting is itself callback code. Its panic must neither escape the
        // ABI nor replace the original native failure.
        recover(
            catch_unwind(AssertUnwindSafe(|| report_error(error))),
            || (),
        );
    }
    result
}

#[cfg(test)]
mod tests {
    use super::super::callback_gate::CallbackGate;
    use super::*;
    use std::{
        future::Future,
        sync::Mutex,
        task::{Context, Waker},
    };

    #[tokio::test]
    async fn first_failure_is_cached_without_receivers_and_wakes_pending_setup() {
        let (failure, _) = tokio::sync::watch::channel(None);
        record_first_failure(&failure, &-7);
        record_first_failure(&failure, &-8);
        assert_eq!(wait_for_failure(failure.subscribe()).await, Some(-7));
        let (next, receiver) = tokio::sync::watch::channel(None);
        let pending = wait_for_failure(receiver);
        tokio::pin!(pending);
        assert!(
            pending
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        record_first_failure(&failure, &-9);
        assert!(
            pending
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        record_first_failure(&next, &-10);
        assert_eq!(pending.await, Some(-10));
    }

    #[tokio::test]
    async fn closed_failure_owner_releases_the_setup_waiter() {
        let (failure, receiver) = tokio::sync::watch::channel(None::<i32>);
        let pending = wait_for_failure(receiver);
        tokio::pin!(pending);
        assert!(
            pending
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(failure);
        assert_eq!(pending.await, None);
    }

    #[test]
    fn retired_callback_failure_keeps_foreign_cause_without_failing_a_new_owner() {
        let old = CallbackGate::new();
        let next = CallbackGate::new();
        let reports = Mutex::new(Vec::new());
        let report = |gate: &CallbackGate, cause| {
            invoke_reported(
                || Err::<(), _>(cause),
                || -1,
                |error| gate.publish(|| reports.lock().unwrap().push(*error)),
            )
        };
        assert_eq!(report(&old, -7), Err(-7));
        old.retire();
        assert_eq!(report(&old, -8), Err(-8));
        assert_eq!(report(&next, -9), Err(-9));
        assert_eq!(*reports.lock().unwrap(), [-7, -9]);
    }

    #[test]
    fn reported_callback_preserves_native_error_and_leaves_success_silent() {
        let reported = Mutex::new(Vec::new());
        assert_eq!(
            invoke_reported(
                || Ok::<_, i32>(42),
                || -1,
                |_| panic!("successful callback reported failure")
            ),
            Ok(42)
        );
        let result = invoke_reported(
            || Err::<(), _>(-2147024891_i32),
            || -1,
            |error| reported.lock().unwrap().push(*error),
        );
        assert_eq!(result, Err(-2147024891_i32));
        assert_eq!(*reported.lock().unwrap(), [-2147024891_i32]);
    }

    #[test]
    fn reported_panic_and_reporting_panic_cannot_escape_or_replace_cause() {
        let reported = Mutex::new(Vec::new());
        let result = invoke_reported(
            || -> Result<(), i32> { panic!("native callback panic") },
            || -1,
            |error| reported.lock().unwrap().push(*error),
        );
        assert_eq!(result, Err(-1));
        assert_eq!(*reported.lock().unwrap(), [-1]);
        extern "C" fn probe() -> i32 {
            invoke_reported(|| Err::<(), _>(-7), || -1, |_| panic!("reporting panic")).unwrap_err()
        }
        assert_eq!(probe(), -7);
    }
}
