//! Pending subscription setup owns its synchronous handler cleanup.
pub(crate) struct SetupCleanup<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> SetupCleanup<F> {
    pub(crate) fn new(cleanup: F) -> Self {
        Self(Some(cleanup))
    }

    pub(crate) fn commit(mut self) {
        drop(self.0.take());
    }
}

impl<F: FnOnce()> Drop for SetupCleanup<F> {
    fn drop(&mut self) {
        if let Some(cleanup) = self.0.take() {
            cleanup();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[tokio::test]
    async fn cancelled_setup_retires_handler_once_and_retry_can_commit() {
        let retired = Arc::new(AtomicUsize::new(0));
        for round in 0..100 {
            let owner = retired.clone();
            let (started, observed) = tokio::sync::oneshot::channel();
            let mut setup = Box::pin(async move {
                let _cleanup = SetupCleanup::new(move || {
                    owner.fetch_add(1, Ordering::SeqCst);
                });
                started.send(()).unwrap();
                std::future::pending::<()>().await;
            });
            assert!(matches!(
                futures::poll!(setup.as_mut()),
                std::task::Poll::Pending
            ));
            observed.await.unwrap();
            drop(setup);
            assert_eq!(retired.load(Ordering::SeqCst), round + 1);
            let owner = retired.clone();
            SetupCleanup::new(move || {
                owner.fetch_add(1, Ordering::SeqCst);
            })
            .commit();
            assert_eq!(retired.load(Ordering::SeqCst), round + 1);
        }
    }

    #[test]
    fn failed_setup_preserves_primary_error_and_retains_failed_removal_for_retry() {
        for _ in 0..100 {
            let mut token = Some(17);
            let mut active = true;
            let mut removals = 0;
            let result: Result<(), &'static str> = (|| {
                let _cleanup = SetupCleanup::new(|| {
                    active = false;
                    removals += 1;
                    // Failed native removal retains token ownership.
                });
                Err("original setup error")
            })();
            assert_eq!(result, Err("original setup error"));
            assert!(!active);
            assert_eq!(token, Some(17));
            assert_eq!(removals, 1);
            {
                let _retry = SetupCleanup::new(|| {
                    token = None;
                    removals += 1;
                });
            }
            assert_eq!(token, None);
            assert_eq!(removals, 2);
        }
    }
}
