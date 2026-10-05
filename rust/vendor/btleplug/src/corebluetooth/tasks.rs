//! Owned Apple event tasks. The adapter closes registration, aborts and joins
//! every task before acknowledging shutdown; Drop is only cancellation fallback.
use crate::{Error, Result};
use std::{future::Future, sync::Mutex};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::task::JoinHandle;

#[derive(Debug, Default)]
pub(super) struct Tasks(Mutex<State>);
#[derive(Debug, Default)]
struct State {
    closed: bool,
    handles: Vec<JoinHandle<()>>,
    failure: Option<String>,
}
fn task_failure(error: tokio::task::JoinError) -> Option<String> {
    if error.is_cancelled() {
        return None;
    }
    let message = format!("Apple event task failed: {error}");
    Some(if error.is_panic() {
        super::callback_boundary::recover(Err(error.into_panic()), || message)
    } else {
        message
    })
}
impl Tasks {
    pub(super) fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) {
        let mut state = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        if state.closed {
            // Registration after the close acknowledgement must not create
            // another task that would need a second shutdown to join it.
            return;
        }
        // Completed event tasks must not accumulate across peripheral cycles.
        // Poll with a no-op waker; a still-running handle remains owned.
        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        let mut failure = None;
        state
            .handles
            .retain_mut(|handle| match Pin::new(handle).poll(&mut context) {
                Poll::Ready(Err(error)) => {
                    if let Some(message) = task_failure(error) {
                        failure.get_or_insert(message);
                    }
                    false
                }
                Poll::Ready(_) => false,
                Poll::Pending => true,
            });
        if state.failure.is_none() {
            state.failure = failure;
        }
        // Reap failures before starting work so a newly spawned handle is never
        // left outside the owner's registry by a failed earlier join.
        state.handles.push(tokio::spawn(future));
    }
    pub(super) fn abort(&self) {
        let mut state = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        state.closed = true;
        for handle in &state.handles {
            handle.abort();
        }
    }
    pub(super) async fn close(&self) -> Result<()> {
        self.abort();
        // Keep handles in the owner while polling. Cancelling this close future
        // must not detach aborted tasks or lose a failure already observed.
        // Adapter::shutdown serializes callers with its existing closing mutex.
        std::future::poll_fn(|context| {
            let mut state = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
            let mut failure = None;
            state
                .handles
                .retain_mut(|handle| match Pin::new(handle).poll(context) {
                    Poll::Ready(Err(error)) => {
                        if let Some(message) = task_failure(error) {
                            failure.get_or_insert(message);
                        }
                        false
                    }
                    Poll::Ready(_) => false,
                    Poll::Pending => true,
                });
            if state.failure.is_none() {
                state.failure = failure;
            }
            if state.handles.is_empty() {
                Poll::Ready(match state.failure.take() {
                    Some(message) => Err(Error::RuntimeError(message)),
                    None => Ok(()),
                })
            } else {
                Poll::Pending
            }
        })
        .await
    }
}
impl Drop for Tasks {
    fn drop(&mut self) {
        self.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct Live(Arc<AtomicUsize>);
    struct PanickingPayload(Arc<AtomicUsize>);
    impl Drop for PanickingPayload {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("Apple join payload destructor failed");
        }
    }
    impl Drop for Live {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    #[tokio::test]
    async fn close_disposes_panicking_join_payload_and_joins_remaining_tasks() {
        use futures::FutureExt;
        let tasks = Tasks::default();
        let drops = Arc::new(AtomicUsize::new(0));
        let captured = drops.clone();
        let (release, gate) = tokio::sync::oneshot::channel();
        let (panicking, entered) = tokio::sync::oneshot::channel();
        tasks.spawn(async move {
            gate.await.unwrap();
            panicking.send(()).unwrap();
            std::panic::panic_any(PanickingPayload(captured));
        });
        let live = Arc::new(AtomicUsize::new(1));
        let resource = Live(live.clone());
        let (started, running) = tokio::sync::oneshot::channel();
        tasks.spawn(async move {
            let _resource = resource;
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        running.await.unwrap();
        release.send(()).unwrap();
        entered.await.unwrap();
        assert!(tasks.0.lock().unwrap().handles[0].is_finished());
        let result = std::panic::AssertUnwindSafe(tasks.close())
            .catch_unwind()
            .await;
        assert!(matches!(result, Ok(Err(Error::RuntimeError(_)))));
        assert_eq!(live.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(Arc::strong_count(&drops), 1);
        assert!(tasks.0.lock().unwrap().handles.is_empty());
        tasks.close().await.unwrap();
    }

    #[tokio::test]
    async fn registration_reaps_panicking_payload_without_losing_new_task() {
        let tasks = Tasks::default();
        let drops = Arc::new(AtomicUsize::new(0));
        let captured = drops.clone();
        let (panicking, entered) = tokio::sync::oneshot::channel();
        tasks.spawn(async move {
            panicking.send(()).unwrap();
            std::panic::panic_any(PanickingPayload(captured));
        });
        entered.await.unwrap();
        assert!(tasks.0.lock().unwrap().handles[0].is_finished());
        let live = Arc::new(AtomicUsize::new(1));
        let resource = Live(live.clone());
        let (started, running) = tokio::sync::oneshot::channel();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tasks.spawn(async move {
                let _resource = resource;
                started.send(()).unwrap();
                std::future::pending::<()>().await;
            });
        }));
        assert!(result.is_ok());
        running.await.unwrap();
        assert!(matches!(tasks.close().await, Err(Error::RuntimeError(_))));
        assert_eq!(live.load(Ordering::SeqCst), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(Arc::strong_count(&drops), 1);
        assert!(tasks.0.lock().unwrap().handles.is_empty());
    }
    #[tokio::test]
    async fn cancelled_close_retains_handles_and_failure_until_retry_joins_them() {
        let tasks = Tasks::default();
        let (failed, observed) = tokio::sync::oneshot::channel();
        tasks.spawn(async move {
            failed.send(()).unwrap();
            panic!("retained Apple task failure");
        });
        observed.await.unwrap();
        assert!(tasks.0.lock().unwrap().handles[0].is_finished());
        let live = Arc::new(AtomicUsize::new(1));
        let resource = Live(live.clone());
        let (started, running) = tokio::sync::oneshot::channel();
        tasks.spawn(async move {
            let _resource = resource;
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        running.await.unwrap();
        let mut close = Box::pin(tasks.close());
        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        assert!(close.as_mut().poll(&mut context).is_pending());
        drop(close);
        assert_eq!(tasks.0.lock().unwrap().handles.len(), 1);
        let error = tasks.close().await.unwrap_err();
        assert!(
            matches!(error, Error::RuntimeError(message) if message.contains("retained Apple task failure"))
        );
        assert_eq!(live.load(Ordering::SeqCst), 0);
        assert!(tasks.0.lock().unwrap().handles.is_empty());
        tasks.close().await.unwrap();
    }

    #[tokio::test]
    async fn close_joins_running_and_late_registered_tasks() {
        let tasks = Tasks::default();
        let live = Arc::new(AtomicUsize::new(1));
        let resource = Live(live.clone());
        let (started, running) = tokio::sync::oneshot::channel();
        tasks.spawn(async move {
            let _resource = resource;
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        running.await.unwrap();
        tasks.close().await.unwrap();
        assert_eq!(live.load(Ordering::SeqCst), 0);
        live.fetch_add(1, Ordering::SeqCst);
        let resource = Live(live.clone());
        tasks.spawn(async move {
            let _resource = resource;
            std::future::pending::<()>().await;
        });
        assert_eq!(live.load(Ordering::SeqCst), 0);
        assert!(tasks.0.lock().unwrap().handles.is_empty());
        tasks.close().await.unwrap();
        assert_eq!(live.load(Ordering::SeqCst), 0);
        tasks.close().await.unwrap();
    }
    #[test]
    fn closed_registration_drops_future_without_starting_a_runtime_task() {
        let tasks = Tasks::default();
        tasks.abort();
        let live = Arc::new(AtomicUsize::new(1));
        let resource = Live(live.clone());
        tasks.spawn(async move {
            let _resource = resource;
            panic!("a closed owner must never poll a newly registered future");
        });
        assert_eq!(live.load(Ordering::SeqCst), 0);
        assert!(tasks.0.lock().unwrap().handles.is_empty());
    }
    #[tokio::test]
    async fn completed_tasks_are_reaped_during_repeated_registration() {
        let tasks = Tasks::default();
        for _ in 0..100 {
            let (finished, wait) = tokio::sync::oneshot::channel();
            tasks.spawn(async move {
                finished.send(()).unwrap();
            });
            wait.await.unwrap();
            assert!(tasks.0.lock().unwrap().handles.len() <= 2);
        }
        tasks.close().await.unwrap();
        assert!(tasks.0.lock().unwrap().handles.is_empty());
    }
}
