//! Match removal is driven inside the same owned future as D-Bus I/O.
//! Dropping that future drops queued/running cleanup; no cleanup task is spawned.
use futures::{StreamExt, stream::FuturesUnordered};
use std::{future::Future, pin::Pin};
use tokio::sync::{mpsc, watch};

const CAPACITY: usize = 64;
const CONCURRENT: usize = 16;
type Removal = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;

/// A canceled AddMatch may already be accepted remotely. Retain its reply
/// future and remove only after success, so a refused setup cannot remove
/// another owner's identical rule. Both phases run inside the owned resource.
pub(crate) async fn remove_after_setup(
    setup: impl Future<Output = Result<(), String>>,
    removal: impl Future<Output = Result<(), String>>,
) -> Result<(), String> {
    setup.await?;
    removal.await
}

#[derive(Clone)]
pub(crate) struct CleanupSender {
    sender: mpsc::Sender<Removal>,
    failure: watch::Sender<Option<String>>,
}

pub(crate) struct Cleanup {
    receiver: mpsc::Receiver<Removal>,
    failure: watch::Receiver<Option<String>>,
    record: watch::Sender<Option<String>>,
}

pub(crate) fn queue() -> (CleanupSender, Cleanup) {
    let (sender, receiver) = mpsc::channel(CAPACITY);
    let (failure, observed) = watch::channel(None);
    (
        CleanupSender {
            sender,
            failure: failure.clone(),
        },
        Cleanup {
            receiver,
            failure: observed,
            record: failure,
        },
    )
}

impl CleanupSender {
    pub(crate) fn failure(&self) -> Option<String> {
        self.failure.borrow().clone()
    }
    pub(crate) fn record_failure(&self, message: &str) {
        self.failure.send_if_modified(|failure| {
            if failure.is_some() {
                return false;
            }
            *failure = Some(message.to_owned());
            true
        });
    }
    pub(crate) fn push(&self, removal: impl Future<Output = Result<(), String>> + Send + 'static) {
        match self.sender.try_send(Box::pin(removal)) {
            Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.record_failure("D-Bus match cleanup queue overflow");
            }
        }
        // A closed receiver means the owned transport has already stopped.
        // Callers remove local callbacks before enqueueing; connection Drop
        // releases server-side matches once its remaining references disappear.
    }
}

impl Cleanup {
    async fn run(mut self) -> Result<(), String> {
        let mut active = FuturesUnordered::new();
        let mut accepting = true;
        loop {
            if let Some(failure) = self.failure.borrow().clone() {
                return Err(failure);
            }
            if !accepting && active.is_empty() {
                return Ok(());
            }
            tokio::select! {
                _ = self.failure.changed() => {}
                removal = self.receiver.recv(), if accepting && active.len() < CONCURRENT => {
                    match removal {
                        Some(removal) => active.push(removal),
                        None => accepting = false,
                    }
                }
                Some(result) = active.next(), if !active.is_empty() => {
                    if let Err(message) = result {
                        self.record.send_if_modified(|failure| {
                            if failure.is_some() { return false; }
                            *failure = Some(message.clone());
                            true
                        });
                        return Err(message);
                    }
                }
            }
        }
    }
}

/// None means every session/stream sender was dropped and cleanup finished.
pub(crate) async fn drive<T>(
    transport: impl Future<Output = T>,
    cleanup: Cleanup,
) -> Result<Option<T>, String> {
    tokio::pin!(transport);
    tokio::select! {
        result = &mut transport => Ok(Some(result)),
        result = cleanup.run() => result.map(|()| None),
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
    impl Drop for Live {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn canceled_match_waits_for_ack_before_removing_its_rule() {
        let (sender, cleanup) = queue();
        let (acknowledge, reply) = tokio::sync::oneshot::channel();
        let (started, waiting) = tokio::sync::oneshot::channel();
        let removed = Arc::new(AtomicUsize::new(0));
        let count = removed.clone();
        sender.push(remove_after_setup(
            async move {
                started.send(()).unwrap();
                reply.await.unwrap()
            },
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ));
        let owner = tokio::spawn(drive(std::future::pending::<()>(), cleanup));
        waiting.await.unwrap();
        assert_eq!(removed.load(Ordering::SeqCst), 0);
        acknowledge.send(Ok(())).unwrap();
        drop(sender);
        assert_eq!(owner.await.unwrap(), Ok(None));
        assert_eq!(removed.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn refused_setup_preserves_error_without_removing_another_owners_rule() {
        let (sender, cleanup) = queue();
        let removed = Arc::new(AtomicUsize::new(0));
        let count = removed.clone();
        sender.push(remove_after_setup(
            async { Err("AddMatch access denied".into()) },
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        ));
        assert_eq!(
            drive(std::future::pending::<()>(), cleanup).await,
            Err("AddMatch access denied".into())
        );
        assert_eq!(removed.load(Ordering::SeqCst), 0);
        assert_eq!(sender.failure(), Some("AddMatch access denied".into()));
    }

    #[tokio::test]
    async fn transport_abort_releases_pending_setup_and_unstarted_removal() {
        let (sender, cleanup) = queue();
        let live = Arc::new(AtomicUsize::new(2));
        let setup = Live(live.clone());
        let removal = Live(live.clone());
        let (started, waiting) = tokio::sync::oneshot::channel();
        sender.push(remove_after_setup(
            async move {
                let _setup = setup;
                started.send(()).unwrap();
                std::future::pending::<Result<(), String>>().await
            },
            async move {
                let _removal = removal;
                panic!("removal must not run before setup succeeds")
            },
        ));
        let owner = tokio::spawn(drive(std::future::pending::<()>(), cleanup));
        waiting.await.unwrap();
        owner.abort();
        assert!(owner.await.unwrap_err().is_cancelled());
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn transport_owner_abort_drops_running_and_queued_removals() {
        let (sender, cleanup) = queue();
        let live = Arc::new(AtomicUsize::new(0));
        let (started, running) = tokio::sync::oneshot::channel();
        live.fetch_add(1, Ordering::SeqCst);
        let first = Live(live.clone());
        sender.push(async move {
            let _first = first;
            started.send(()).unwrap();
            std::future::pending().await
        });
        let owner = tokio::spawn(drive(std::future::pending::<()>(), cleanup));
        running.await.unwrap();
        for _ in 0..CAPACITY {
            live.fetch_add(1, Ordering::SeqCst);
            let resource = Live(live.clone());
            sender.push(async move {
                let _resource = resource;
                std::future::pending().await
            });
        }
        owner.abort();
        assert!(owner.await.unwrap_err().is_cancelled());
        assert_eq!(live.load(Ordering::SeqCst), 0);
        assert!(sender.sender.is_closed());
        assert_eq!(sender.failure(), None);
    }

    #[tokio::test]
    async fn last_sender_drop_drains_removals_then_releases_transport() {
        let (sender, cleanup) = queue();
        let live = Arc::new(AtomicUsize::new(1));
        let transport = Live(live.clone());
        let (release, gate) = tokio::sync::oneshot::channel();
        let (started, running) = tokio::sync::oneshot::channel();
        sender.push(async move {
            started.send(()).unwrap();
            gate.await.unwrap();
            Ok(())
        });
        let owner = tokio::spawn(drive(
            async move {
                let _transport = transport;
                std::future::pending::<()>().await
            },
            cleanup,
        ));
        running.await.unwrap();
        drop(sender);
        assert!(!owner.is_finished());
        release.send(()).unwrap();
        assert_eq!(owner.await.unwrap(), Ok(None));
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn removal_failure_preserves_cause_and_drops_remaining_work() {
        let (sender, cleanup) = queue();
        let live = Arc::new(AtomicUsize::new(1));
        let resource = Live(live.clone());
        sender.push(async move {
            let _resource = resource;
            std::future::pending().await
        });
        sender.push(async { Err("RemoveMatch access denied".into()) });
        assert_eq!(
            drive(std::future::pending::<()>(), cleanup).await,
            Err("RemoveMatch access denied".into())
        );
        assert_eq!(live.load(Ordering::SeqCst), 0);
        assert!(sender.sender.is_closed());
        assert_eq!(sender.failure(), Some("RemoveMatch access denied".into()));
    }

    #[tokio::test]
    async fn overflow_is_bounded_and_reported_without_polling_dropped_work() {
        let (sender, cleanup) = queue();
        let live = Arc::new(AtomicUsize::new(0));
        for _ in 0..CAPACITY + 10 {
            live.fetch_add(1, Ordering::SeqCst);
            let resource = Live(live.clone());
            sender.push(async move {
                let _resource = resource;
                panic!("overflowed work must not run");
            });
        }
        assert_eq!(live.load(Ordering::SeqCst), CAPACITY);
        assert_eq!(
            sender.failure(),
            Some("D-Bus match cleanup queue overflow".into())
        );
        assert_eq!(
            drive(std::future::pending::<()>(), cleanup).await,
            Err("D-Bus match cleanup queue overflow".into())
        );
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn closed_registration_drops_future_without_spawning_a_task() {
        let (sender, cleanup) = queue();
        drop(cleanup);
        let live = Arc::new(AtomicUsize::new(1));
        let resource = Live(live.clone());
        sender.push(async move {
            let _resource = resource;
            panic!("closed work must not run");
        });
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cancellation_retains_overflow_registered_before_resource_poll() {
        let (sender, cleanup) = queue();
        for _ in 0..=CAPACITY {
            sender.push(std::future::pending());
        }
        let owner = tokio::spawn(drive(std::future::pending::<()>(), cleanup));
        owner.abort();
        assert!(owner.await.unwrap_err().is_cancelled());
        assert_eq!(
            sender.failure(),
            Some("D-Bus match cleanup queue overflow".into())
        );
        assert!(sender.sender.is_closed());
    }

    #[tokio::test]
    async fn transport_failure_drops_removal_work() {
        let (sender, cleanup) = queue();
        let live = Arc::new(AtomicUsize::new(1));
        let resource = Live(live.clone());
        sender.push(async move {
            let _resource = resource;
            std::future::pending().await
        });
        assert_eq!(
            drive(async { "bus disconnected" }, cleanup).await,
            Ok(Some("bus disconnected"))
        );
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }
}
