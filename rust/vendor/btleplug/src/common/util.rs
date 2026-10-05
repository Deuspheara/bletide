// btleplug Source Code File
//
// Copyright 2020 Nonpolynomial. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.

use crate::api::ValueNotification;
use futures::stream::{Stream, StreamExt};
use std::pin::Pin;
use tokio::sync::broadcast::Receiver;
use tokio_stream::wrappers::BroadcastStream;

#[allow(dead_code)]
pub fn notifications_stream_from_broadcast_receiver(
    receiver: Receiver<ValueNotification>,
) -> Pin<Box<dyn Stream<Item = ValueNotification> + Send>> {
    notifications_stream_from_results(BroadcastStream::new(receiver))
}

#[allow(dead_code)]
pub fn notification_results_from_broadcast_receiver(
    receiver: Receiver<ValueNotification>,
) -> Pin<Box<dyn Stream<Item = crate::Result<ValueNotification>> + Send>> {
    notification_results_from_stream(BroadcastStream::new(receiver).map(|result| {
        result
            .map_err(|error| crate::Error::RuntimeError(format!("Notification broadcast: {error}")))
    }))
}

/// Observe a sticky callback failure alongside values. A fresh receiver belongs
/// to one connection generation; replacing its sender never clears an old cause.
#[cfg(any(target_os = "windows", test))]
pub(crate) fn notification_results_with_callback_failure<S, F>(
    stream: S,
    failure: tokio::sync::watch::Receiver<Option<F>>,
    convert: impl Fn(F) -> crate::Error + Send + 'static,
) -> Pin<Box<dyn Stream<Item = crate::Result<ValueNotification>> + Send>>
where
    S: Stream<Item = crate::Result<ValueNotification>> + Send + 'static,
    F: Clone + Send + Sync + 'static,
{
    notification_results_from_stream(futures::stream::unfold(
        (Box::pin(stream), failure, convert),
        |(mut stream, mut failure, convert)| async move {
            loop {
                // Known failure takes precedence over buffered values.
                let cached = failure.borrow_and_update().clone();
                if let Some(cause) = cached {
                    return Some((Err(convert(cause)), (stream, failure, convert)));
                }
                tokio::select! {
                    biased;
                    changed = failure.changed() => {
                        if changed.is_err() {
                            return None;
                        }
                    }
                    value = stream.next() => {
                        let value = value?;
                        let cached = failure.borrow_and_update().clone();
                        let value = match cached {
                            Some(cause) => Err(convert(cause)),
                            None => value,
                        };
                        return Some((value, (stream, failure, convert)));
                    }
                }
            }
        },
    ))
}

pub(crate) fn notification_results_from_stream<S, E>(
    stream: S,
) -> Pin<Box<dyn Stream<Item = Result<ValueNotification, E>> + Send>>
where
    S: Stream<Item = Result<ValueNotification, E>> + Send + 'static,
    E: Send + 'static,
{
    Box::pin(
        futures::stream::unfold(Some(Box::pin(stream)), |source| async move {
            let mut source = source?;
            let result = source.next().await?;
            let next = if result.is_err() { None } else { Some(source) };
            Some((result, next))
        })
        .fuse(),
    )
}

// The public btleplug stream has no error item. Terminate it on the first
// transport/overflow error so its owner observes loss and can disconnect.
// Filtering errors would silently resume after missing notification data.
pub(crate) fn notifications_stream_from_results<S, E>(
    stream: S,
) -> Pin<Box<dyn Stream<Item = ValueNotification> + Send>>
where
    S: Stream<Item = Result<ValueNotification, E>> + Send + 'static,
    E: std::fmt::Debug + Send + 'static,
{
    Box::pin(stream.scan((), |_, result| {
        if let Err(error) = &result {
            log::error!("Notification stream ended after transport error: {error:?}");
        }
        futures::future::ready(result.ok())
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use uuid::Uuid;

    fn notification(service: u128) -> ValueNotification {
        ValueNotification {
            service_uuid: Uuid::from_u128(service),
            uuid: Uuid::from_u128(3),
            value: vec![0, 255],
        }
    }

    #[tokio::test]
    async fn callback_failure_is_sticky_before_listening_and_precedes_queued_values() {
        let (failure, _) = tokio::sync::watch::channel(None);
        failure.send_replace(Some("native decode failed".to_string()));
        let mut result = notification_results_with_callback_failure(
            stream::iter([Ok(notification(1)), Ok(notification(2))]),
            failure.subscribe(),
            crate::Error::RuntimeError,
        );
        assert_eq!(
            result.next().await.unwrap().unwrap_err().to_string(),
            "Runtime Error: native decode failed"
        );
        assert!(result.next().await.is_none());
    }

    #[tokio::test]
    async fn callback_failure_wakes_an_idle_stream_and_is_terminal() {
        let (failure, receiver) = tokio::sync::watch::channel(None);
        let owner = Arc::new(());
        let retired = Arc::downgrade(&owner);
        let source = stream::unfold(owner, |owner| async move {
            futures::future::pending::<()>().await;
            Some((Ok(notification(1)), owner))
        });
        let mut result = notification_results_with_callback_failure(
            source,
            receiver,
            crate::Error::RuntimeError,
        );
        let next = result.next();
        tokio::pin!(next);
        assert!(futures::poll!(&mut next).is_pending());
        failure.send_replace(Some("native idle failure".to_string()));
        assert!(
            next.await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("native idle failure")
        );
        assert!(
            retired.upgrade().is_none(),
            "Terminal failure must release the pending source immediately"
        );
        assert!(result.next().await.is_none());
    }

    #[tokio::test]
    async fn reconnect_keeps_old_failure_and_new_notifications_independent() {
        let (old, receiver) = tokio::sync::watch::channel(None);
        old.send_replace(Some("old generation".to_string()));
        let mut result = notification_results_with_callback_failure(
            stream::pending(),
            receiver,
            crate::Error::RuntimeError,
        );
        // The callback retains its old sender; replacing the owner's channel
        // must never erase an error or affect a new generation's values.
        let (new, receiver) = tokio::sync::watch::channel(None::<String>);
        let mut next = notification_results_with_callback_failure(
            stream::iter([Ok(notification(2))]),
            receiver,
            crate::Error::RuntimeError,
        );
        assert!(
            result
                .next()
                .await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("old generation")
        );
        old.send_replace(Some("late old failure".to_string()));
        assert_eq!(
            next.next().await.unwrap().unwrap().service_uuid,
            Uuid::from_u128(2)
        );
        assert!(new.borrow().is_none());
        assert!(result.next().await.is_none());
    }

    #[tokio::test]
    async fn result_stream_preserves_terminal_cause_without_polling_after_error() {
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = polls.clone();
        let source = stream::iter([
            Ok(notification(1)),
            Err(crate::Error::RuntimeError("JNI callback lost".into())),
            Ok(notification(2)),
        ])
        .inspect(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        let mut result = notification_results_from_stream(source);
        assert_eq!(result.next().await.unwrap().unwrap().value, [0, 255]);
        assert!(
            result
                .next()
                .await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("JNI callback lost")
        );
        assert!(result.next().await.is_none());
        assert!(result.next().await.is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn result_broadcast_preserves_overflow_count_and_then_ends() {
        let (sender, receiver) = tokio::sync::broadcast::channel(2);
        for service in 1..=5 {
            sender.send(notification(service)).unwrap();
        }
        let mut result = notification_results_from_broadcast_receiver(receiver);
        let error = result.next().await.unwrap().unwrap_err().to_string();
        assert!(error.contains("lagged by 3"), "{error}");
        assert!(result.next().await.is_none());
    }

    #[tokio::test]
    async fn valid_notifications_preserve_service_identity_and_binary_payload() {
        let result = notifications_stream_from_results(stream::iter([
            Ok::<_, crate::Error>(notification(1)),
            Ok(notification(2)),
        ]))
        .collect::<Vec<_>>()
        .await;
        assert_eq!(result.len(), 2);
        for (index, value) in result.iter().enumerate() {
            assert_eq!(value.service_uuid, Uuid::from_u128(index as u128 + 1));
            assert_eq!(value.uuid, Uuid::from_u128(3));
            assert_eq!(value.value, [0, 255]);
        }
    }

    #[tokio::test]
    async fn transport_error_is_terminal_and_never_polls_later_values() {
        for prefix in 0..=1 {
            let polls = Arc::new(AtomicUsize::new(0));
            let observed = polls.clone();
            let mut input = Vec::new();
            if prefix == 1 {
                input.push(Ok(notification(1)));
            }
            input.push(Err(crate::Error::RuntimeError(
                "Controlled JNI failure".into(),
            )));
            input.push(Ok(notification(2)));
            let source = stream::iter(input).inspect(move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
            });
            let mut result = notifications_stream_from_results(source);
            if prefix == 1 {
                assert_eq!(
                    result.next().await.unwrap().service_uuid,
                    Uuid::from_u128(1)
                );
            }
            assert!(result.next().await.is_none());
            assert!(result.next().await.is_none());
            assert_eq!(polls.load(Ordering::SeqCst), prefix + 1);
        }
    }

    #[tokio::test]
    async fn broadcast_overflow_terminates_instead_of_skipping_lost_values() {
        let (sender, receiver) = tokio::sync::broadcast::channel(2);
        for service in 1..=3 {
            sender.send(notification(service)).unwrap();
        }
        let mut stream = notifications_stream_from_broadcast_receiver(receiver);
        assert!(stream.next().await.is_none());
        sender.send(notification(4)).unwrap();
        assert!(stream.next().await.is_none());
    }
}
