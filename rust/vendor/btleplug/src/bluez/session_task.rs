//! The session owns the actual D-Bus resource task, never a wrapper around an
//! already spawned task. Explicit close retains the handle until it is joined.
use crate::{Error, Result};
use futures::{Stream, StreamExt};
use std::{future::Future, pin::Pin, sync::Mutex, task::Poll};
use tokio::{
    sync::{Mutex as AsyncMutex, watch},
    task::JoinHandle,
};

#[allow(dead_code)]
#[path = "../winrtble/ble/callback_boundary.rs"]
mod callback_boundary;

#[derive(Debug)]
pub(crate) struct SessionTask {
    handle: Mutex<Option<JoinHandle<std::result::Result<(), String>>>>,
    closing: AsyncMutex<()>,
    failure: Mutex<Option<String>>,
    stopped: watch::Sender<Option<String>>,
}

// Captured before spawning, so abort-before-first-poll still wakes event streams.
struct Completion(watch::Sender<Option<String>>);
impl Drop for Completion {
    fn drop(&mut self) {
        let unfinished = self.0.borrow().is_none();
        if unfinished {
            self.0.send_replace(Some("D-Bus transport stopped".into()));
        }
    }
}

impl SessionTask {
    pub(crate) fn spawn(
        resource: impl Future<Output = std::result::Result<(), String>> + Send + 'static,
    ) -> Self {
        let (stopped, _) = watch::channel(None);
        let completion = Completion(stopped.clone());
        Self {
            handle: Mutex::new(Some(tokio::spawn(async move {
                let result = resource.await;
                completion.0.send_replace(Some(match &result {
                    Ok(()) => "D-Bus transport ended".into(),
                    Err(message) => message.clone(),
                }));
                drop(completion);
                result
            }))),
            closing: AsyncMutex::new(()),
            failure: Mutex::new(None),
            stopped,
        }
    }

    /// Poll transport completion alongside events in the caller's stream.
    /// A sticky failure wins over queued values, is delivered once, then ends.
    pub(crate) fn monitor<S>(
        &self,
        events: S,
    ) -> impl Stream<Item = std::result::Result<S::Item, String>> + Send + use<S>
    where
        S: Stream + Send + 'static,
        S::Item: Send,
    {
        futures::stream::unfold(
            (Some(Box::pin(events)), self.stopped.subscribe()),
            |(events, mut stopped)| async move {
                let mut events = events?;
                loop {
                    let message = stopped.borrow().clone();
                    if let Some(message) = message {
                        drop(events);
                        return Some((Err(message), (None, stopped)));
                    }
                    tokio::select! {
                        biased;
                        changed = stopped.changed() => {
                            if changed.is_err() {
                                drop(events);
                                return Some((Err("D-Bus transport owner dropped".into()), (None, stopped)));
                            }
                        }
                        value = events.next() => {
                            return value.map(|value| (Ok(value), (Some(events), stopped)));
                        }
                    }
                }
            },
        )
    }

    pub(crate) fn notification_results<S>(
        &self,
        values: S,
    ) -> impl Stream<Item = Result<S::Item>> + Send + use<S>
    where
        S: Stream + Send + 'static,
        S::Item: Send,
    {
        self.monitor(values)
            .map(|result| result.map_err(Error::RuntimeError))
    }

    /// Race an initialization/setup wait against the actual owned resource.
    /// Dropping this future retires its observer and operation without a task.
    pub(crate) async fn operation<T>(
        &self,
        operation: impl Future<Output = Result<T>> + Send,
    ) -> Result<T>
    where
        T: Send,
    {
        let mut stopped = self.stopped.subscribe();
        tokio::pin!(operation);
        loop {
            if let Some(message) = stopped.borrow().clone() {
                return Err(Error::RuntimeError(message));
            }
            tokio::select! {
                biased;
                changed = stopped.changed() => {
                    if changed.is_err() {
                        return Err(Error::RuntimeError("D-Bus transport owner dropped".into()));
                    }
                }
                result = &mut operation => return result,
            }
        }
    }

    pub(crate) async fn close(&self) -> Result<()> {
        let _closing = self.closing.lock().await;
        std::future::poll_fn(|context| {
            let mut owned = self.handle.lock().unwrap_or_else(|p| p.into_inner());
            let Some(handle) = owned.as_mut() else {
                return Poll::Ready(());
            };
            handle.abort();
            let result = match Pin::new(handle).poll(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result,
            };
            *owned = None;
            let failure = match result {
                Ok(Ok(())) => None,
                Ok(Err(message)) => Some(message),
                Err(error) if error.is_cancelled() => None,
                Err(error) => {
                    let message = format!("D-Bus resource task failed: {error}");
                    Some(if error.is_panic() {
                        callback_boundary::recover(Err(error.into_panic()), || message)
                    } else {
                        message
                    })
                }
            };
            if let Some(message) = failure {
                *self.failure.lock().unwrap_or_else(|p| p.into_inner()) = Some(message);
            }
            Poll::Ready(())
        })
        .await;
        match self
            .failure
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
        {
            Some(message) => Err(Error::RuntimeError(message.clone())),
            None => Ok(()),
        }
    }
}

impl Drop for SessionTask {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.get_mut().unwrap_or_else(|p| p.into_inner()) {
            handle.abort();
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

    struct Live(Arc<AtomicUsize>);
    impl Drop for Live {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    async fn running() -> (SessionTask, Arc<AtomicUsize>) {
        let live = Arc::new(AtomicUsize::new(1));
        let resource = Live(live.clone());
        let (started, ready) = tokio::sync::oneshot::channel();
        let owner = SessionTask::spawn(async move {
            let _resource = resource;
            started.send(()).unwrap();
            std::future::pending().await
        });
        ready.await.unwrap();
        (owner, live)
    }

    #[tokio::test]
    async fn transport_failure_interrupts_setup_and_releases_partial_ownership() {
        let (failure, fail) = tokio::sync::oneshot::channel();
        let owner = Arc::new(SessionTask::spawn(async move {
            fail.await.unwrap();
            Err("D-Bus failed during setup".into())
        }));
        let live = Arc::new(AtomicUsize::new(1));
        let registration = Live(live.clone());
        let (started, waiting) = tokio::sync::oneshot::channel();
        let task_owner = owner.clone();
        let setup = tokio::spawn(async move {
            task_owner
                .operation(async move {
                    let _registration = registration;
                    started.send(()).unwrap();
                    std::future::pending::<Result<()>>().await
                })
                .await
        });
        waiting.await.unwrap();
        failure.send(()).unwrap();
        assert!(
            matches!(tokio::time::timeout(std::time::Duration::from_secs(2), setup).await.unwrap().unwrap(),
            Err(Error::RuntimeError(message)) if message == "D-Bus failed during setup")
        );
        assert_eq!(live.load(Ordering::SeqCst), 0);
        assert_eq!(owner.stopped.receiver_count(), 0);
        assert!(owner.close().await.is_err());
    }

    #[tokio::test]
    async fn known_transport_failure_prevents_setup_poll() {
        let owner = SessionTask::spawn(async { Err("D-Bus already stopped".into()) });
        let mut stopped = owner.stopped.subscribe();
        stopped.changed().await.unwrap();
        let polls = AtomicUsize::new(0);
        assert!(matches!(owner.operation(async {
            polls.fetch_add(1, Ordering::SeqCst);
            Ok(7)
        }).await, Err(Error::RuntimeError(message)) if message == "D-Bus already stopped"));
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        assert!(owner.close().await.is_err());
    }

    #[tokio::test]
    async fn canceled_setup_releases_observer_and_allows_immediate_retry() {
        let (owner, transport) = running().await;
        let live = Arc::new(AtomicUsize::new(1));
        let registration = Live(live.clone());
        let mut setup = Box::pin(owner.operation(async move {
            let _registration = registration;
            std::future::pending::<Result<()>>().await
        }));
        assert!(futures::poll!(setup.as_mut()).is_pending());
        assert_eq!(owner.stopped.receiver_count(), 1);
        drop(setup);
        assert_eq!(owner.stopped.receiver_count(), 0);
        assert_eq!(live.load(Ordering::SeqCst), 0);
        assert_eq!(transport.load(Ordering::SeqCst), 1);
        assert_eq!(owner.operation(async { Ok(9) }).await.unwrap(), 9);
        owner.close().await.unwrap();
        assert_eq!(transport.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn setup_error_preserves_original_variant_without_poisoning_transport() {
        let (owner, transport) = running().await;
        assert!(matches!(
            owner
                .operation(async { Err::<(), _>(Error::PermissionDenied) })
                .await,
            Err(Error::PermissionDenied)
        ));
        assert_eq!(owner.operation(async { Ok(11) }).await.unwrap(), 11);
        assert_eq!(owner.stopped.receiver_count(), 0);
        assert_eq!(transport.load(Ordering::SeqCst), 1);
        owner.close().await.unwrap();
        assert_eq!(transport.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn notification_failure_preserves_cause_and_retires_buffered_registration() {
        use crate::api::ValueNotification;
        use futures::FutureExt;
        let (failure, fail) = tokio::sync::oneshot::channel();
        let owner = SessionTask::spawn(async move {
            fail.await.unwrap();
            Err("D-Bus notification transport lost".into())
        });
        let registrations = Arc::new(AtomicUsize::new(1));
        let registration = Live(registrations.clone());
        let (mut sender, receiver) = futures::channel::mpsc::channel(2);
        let values = receiver.map(move |value| {
            let _keep_alive = &registration;
            value
        });
        let mut stream = Box::pin(owner.notification_results(values));
        assert!(stream.next().now_or_never().is_none());
        let first = ValueNotification {
            uuid: uuid::Uuid::from_u128(1),
            service_uuid: uuid::Uuid::from_u128(2),
            value: vec![1],
        };
        sender.try_send(first.clone()).unwrap();
        assert!(matches!(stream.next().await, Some(Ok(value)) if value == first));
        sender
            .try_send(ValueNotification {
                value: vec![2],
                ..first
            })
            .unwrap();
        let mut stopped = owner.stopped.subscribe();
        failure.send(()).unwrap();
        stopped.changed().await.unwrap();
        assert!(
            matches!(tokio::time::timeout(std::time::Duration::from_secs(2), stream.next()).await.unwrap(),
            Some(Err(Error::RuntimeError(message))) if message == "D-Bus notification transport lost")
        );
        assert_eq!(registrations.load(Ordering::SeqCst), 0);
        assert!(stream.next().await.is_none());
        let mut late =
            Box::pin(owner.notification_results(futures::stream::pending::<ValueNotification>()));
        assert!(matches!(late.next().await,
            Some(Err(Error::RuntimeError(message))) if message == "D-Bus notification transport lost"));
        assert!(late.next().await.is_none());
        assert!(owner.close().await.is_err());
    }

    #[tokio::test]
    async fn repeated_notification_failure_joins_transport_and_returns_to_baseline() {
        use crate::api::ValueNotification;
        for _ in 0..100 {
            let live = Arc::new(AtomicUsize::new(1));
            let resource = Live(live.clone());
            let owner = SessionTask::spawn(async move {
                let _resource = resource;
                Err("D-Bus notification cleanup fixture".into())
            });
            let mut stream = Box::pin(
                owner.notification_results(futures::stream::pending::<ValueNotification>()),
            );
            assert!(
                matches!(tokio::time::timeout(std::time::Duration::from_secs(2), stream.next()).await.unwrap(),
                Some(Err(Error::RuntimeError(message))) if message == "D-Bus notification cleanup fixture")
            );
            assert!(stream.next().await.is_none());
            assert!(owner.close().await.is_err());
            assert!(owner.handle.lock().unwrap().is_none());
            assert_eq!(live.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn idle_and_late_streams_receive_the_transport_cause_once() {
        let (failure, fail) = tokio::sync::oneshot::channel();
        let owner = SessionTask::spawn(async move {
            fail.await.unwrap();
            Err("system bus connection lost".into())
        });
        let mut first = Box::pin(owner.monitor(futures::stream::pending::<u8>()));
        let mut second = Box::pin(owner.monitor(futures::stream::pending::<u8>()));
        failure.send(()).unwrap();
        for stream in [&mut first, &mut second] {
            assert_eq!(
                tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
                    .await
                    .unwrap(),
                Some(Err("system bus connection lost".into()))
            );
            assert_eq!(stream.next().await, None);
        }
        let mut late = Box::pin(owner.monitor(futures::stream::iter([1_u8])));
        assert_eq!(
            late.next().await,
            Some(Err("system bus connection lost".into()))
        );
        assert_eq!(late.next().await, None);
        assert!(owner.close().await.is_err());
    }

    #[tokio::test]
    async fn failure_retires_event_registration_before_delivery() {
        let registrations = Arc::new(AtomicUsize::new(1));
        let registration = Live(registrations.clone());
        let events = futures::stream::pending::<u8>().map(move |value| {
            let _keep_alive = &registration;
            value
        });
        let owner = SessionTask::spawn(async { Err("transport failed".into()) });
        let mut stream = Box::pin(owner.monitor(events));
        assert_eq!(stream.next().await, Some(Err("transport failed".into())));
        assert_eq!(registrations.load(Ordering::SeqCst), 0);
        // Keep the stream itself alive: terminal delivery must retire ownership.
        assert!(owner.close().await.is_err());
        assert_eq!(stream.next().await, None);
    }

    #[tokio::test]
    async fn monitor_delivers_values_and_survives_cancelled_wait() {
        use futures::FutureExt;
        let (owner, live) = running().await;
        let (mut sender, receiver) = futures::channel::mpsc::channel(1);
        let mut stream = Box::pin(owner.monitor(receiver));
        assert!(stream.next().now_or_never().is_none());
        sender.try_send(7_u8).unwrap();
        assert_eq!(stream.next().await, Some(Ok(7)));
        drop(sender);
        assert_eq!(stream.next().await, None);
        owner.close().await.unwrap();
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn abort_before_first_poll_wakes_a_retained_event_stream() {
        let live = Arc::new(AtomicUsize::new(1));
        let resource = Live(live.clone());
        let owner = SessionTask::spawn(async move {
            let _resource = resource;
            std::future::pending().await
        });
        let mut stream = Box::pin(owner.monitor(futures::stream::pending::<u8>()));
        // Current-thread runtime cannot poll the resource before this abort.
        owner.close().await.unwrap();
        assert_eq!(live.load(Ordering::SeqCst), 0);
        assert_eq!(
            stream.next().await,
            Some(Err("D-Bus transport stopped".into()))
        );
        assert_eq!(stream.next().await, None);
    }

    #[tokio::test]
    async fn dropping_owner_wakes_pending_stream_and_releases_resource() {
        let (owner, live) = running().await;
        let mut stream = Box::pin(owner.monitor(futures::stream::pending::<u8>()));
        drop(owner);
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
                .await
                .unwrap(),
            Some(Err("D-Bus transport stopped".into()))
        );
        assert_eq!(live.load(Ordering::SeqCst), 0);
        assert_eq!(stream.next().await, None);
    }

    #[tokio::test]
    async fn close_joins_resource_and_allows_a_fresh_session() {
        for _ in 0..100 {
            let (owner, live) = running().await;
            owner.close().await.unwrap();
            assert_eq!(live.load(Ordering::SeqCst), 0);
            assert!(owner.handle.lock().unwrap().is_none());
            owner.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn cancelled_close_retains_resource_until_retry_joins() {
        use futures::FutureExt;
        let (owner, live) = running().await;
        assert!(owner.close().now_or_never().is_none());
        assert!(owner.handle.lock().unwrap().is_some());
        owner.close().await.unwrap();
        assert_eq!(live.load(Ordering::SeqCst), 0);
        assert!(owner.handle.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn completed_transport_error_survives_repeated_close() {
        let (sent, received) = tokio::sync::oneshot::channel();
        let owner = SessionTask::spawn(async move {
            sent.send(()).unwrap();
            Err("system bus connection lost".into())
        });
        received.await.unwrap();
        for _ in 0..2 {
            assert!(
                matches!(owner.close().await, Err(Error::RuntimeError(message))
                if message == "system bus connection lost")
            );
        }
    }

    #[tokio::test]
    async fn concurrent_closes_join_the_same_resource() {
        let (owner, live) = running().await;
        let (first, second) = tokio::join!(owner.close(), owner.close());
        first.unwrap();
        second.unwrap();
        assert_eq!(live.load(Ordering::SeqCst), 0);
        assert!(owner.handle.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn panicking_resource_payload_is_disposed_and_reported() {
        struct Payload(Arc<AtomicUsize>);
        impl Drop for Payload {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
                panic!("D-Bus panic payload destructor failed");
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let payload = Payload(drops.clone());
        let (sent, received) = tokio::sync::oneshot::channel();
        let owner = SessionTask::spawn(async move {
            sent.send(()).unwrap();
            std::panic::panic_any(payload);
        });
        received.await.unwrap();
        assert!(
            matches!(owner.close().await, Err(Error::RuntimeError(message))
            if message.contains("D-Bus resource task failed"))
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(Arc::strong_count(&drops), 1);
        assert!(owner.handle.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn last_owner_drop_aborts_resource() {
        struct Released(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for Released {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }
        let (released, dropped) = tokio::sync::oneshot::channel();
        let (started, ready) = tokio::sync::oneshot::channel();
        let resource = Released(Some(released));
        let owner = SessionTask::spawn(async move {
            let _resource = resource;
            started.send(()).unwrap();
            std::future::pending().await
        });
        ready.await.unwrap();
        drop(owner);
        tokio::time::timeout(std::time::Duration::from_secs(1), dropped)
            .await
            .unwrap()
            .unwrap();
    }
}
