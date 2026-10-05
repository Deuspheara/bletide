//! One owned worker per physical connection. A cancelled OS operation invalidates
//! this generation and disconnects before any later request can touch the device.
use crate::{
    codec::{Error, Reader, event},
    engine::{Command, Engine, finish},
};
mod native;
use btleplug::api::ValueNotification;
use futures_util::{FutureExt, Stream, StreamExt};
#[cfg(any(test, feature = "test-support"))]
pub(crate) use native::encode_services;
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio::sync::{mpsc, watch};

pub(crate) type Notifications =
    Pin<Box<dyn Stream<Item = Result<ValueNotification, Error>> + Send>>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConnectionState {
    Connecting,
    Connected,
    Closing,
    Closed,
}
pub(crate) trait Driver: Send {
    fn connect(&mut self) -> impl Future<Output = Result<(), Error>> + Send;
    fn disconnect(&mut self) -> impl Future<Output = Result<(), Error>> + Send;
    fn notifications(&mut self) -> impl Future<Output = Result<Notifications, Error>> + Send;
    fn operate(
        &mut self,
        operation: u32,
        payload: &[u8],
    ) -> impl Future<Output = Result<Vec<u8>, Error>> + Send;
}
pub(crate) trait Device: Clone + Send + Sync + 'static {
    type Driver: Driver;
    fn driver(self) -> Self::Driver;
    fn probe_connected(&self) -> impl Future<Output = Result<bool, Error>> + Send;
}
pub(crate) struct Slot<P> {
    pub device_id: String,
    pub commands: mpsc::Sender<Command>,
    pub stop: watch::Sender<u32>,
    pub state: watch::Receiver<ConnectionState>,
    pub peripheral: P,
    pub closing: Vec<Command>,
    pub connect_cancel: watch::Receiver<bool>,
}
pub(crate) fn prepare<P>(
    device_id: String,
    peripheral: P,
    connect_cancel: watch::Receiver<bool>,
) -> (
    Slot<P>,
    mpsc::Receiver<Command>,
    watch::Receiver<u32>,
    watch::Sender<ConnectionState>,
) {
    let (commands, receiver) = mpsc::channel(1024);
    let (stop, stopped) = watch::channel(0);
    let (state, states) = watch::channel(ConnectionState::Connecting);
    (
        Slot {
            device_id,
            commands,
            stop,
            state: states,
            peripheral,
            closing: Vec::new(),
            connect_cancel,
        },
        receiver,
        stopped,
        state,
    )
}
pub(crate) async fn run<P: Device>(
    engine: Arc<Engine>,
    generation: u64,
    peripheral: P,
    connect: Command,
    commands: mpsc::Receiver<Command>,
    stop: watch::Receiver<u32>,
    state: watch::Sender<ConnectionState>,
) -> Result<(), Error> {
    worker(
        peripheral.driver(),
        engine,
        generation,
        connect,
        commands,
        stop,
        state,
    )
    .await
}
async fn disconnect(driver: &mut impl Driver) -> Result<(), Error> {
    let mut operation = Box::pin(std::panic::AssertUnwindSafe(driver.disconnect()).catch_unwind());
    // The timeout owns only a pin reference. Keep the actual future until its
    // destructor has run inside the explicit cleanup guard, including timeout.
    let result = match tokio::time::timeout(Duration::from_secs(5), &mut operation).await {
        Ok(result) => crate::callback_boundary::recover(result, || {
            Err(Error::new(18, "Connection cleanup panicked"))
        }),
        Err(_) => Err(Error::new(9, "Connection cleanup timed out")),
    };
    let disposed = crate::callback_boundary::invoke(
        || {
            drop(operation);
            Ok(())
        },
        || Error::new(18, "Connection cleanup future disposal panicked"),
    );
    result.and(disposed)
}

fn emit(engine: &Engine, kind: u32, generation: u64, payload: Vec<u8>) {
    if !engine.sink.send(&event(kind, generation, Ok(payload))) {
        engine.stop.send_replace(true);
    }
}
fn emit_notification_error(engine: &Engine, generation: u64, error: Error) {
    if !engine.sink.send(&event(6, generation, Err(error))) {
        engine.stop.send_replace(true);
    }
}
fn emit_notification(engine: &Engine, generation: u64, value: ValueNotification) {
    let mut payload = Vec::with_capacity(32 + value.value.len());
    payload.extend_from_slice(value.service_uuid.as_bytes());
    payload.extend_from_slice(value.uuid.as_bytes());
    payload.extend_from_slice(&value.value);
    emit(engine, 6, generation, payload);
}

async fn next_notification(
    notifications: &mut Option<Notifications>,
) -> Option<Result<ValueNotification, Error>> {
    match notifications {
        Some(values) => crate::callback_boundary::recover(
            std::panic::AssertUnwindSafe(values.next())
                .catch_unwind()
                .await,
            || Some(Err(Error::new(18, "Notification stream panicked"))),
        ),
        None => std::future::pending().await,
    }
}

async fn worker(
    mut driver: impl Driver,
    engine: Arc<Engine>,
    generation: u64,
    connect: Command,
    mut commands: mpsc::Receiver<Command>,
    mut stop: watch::Receiver<u32>,
    state: watch::Sender<ConnectionState>,
) -> Result<(), Error> {
    let _worker_resource = engine
        .resources
        .track(crate::resources::Kind::ConnectionWorker);
    let mut cancel = connect.request.cancel.subscribe();
    let mut notification_resource = None;
    let mut notifications = None;
    let mut initialization_cleanup = Ok(());
    let connected: Result<Vec<u8>, Error> = if *cancel.borrow() {
        Err(Error::new(10, "Connect cancelled"))
    } else if *stop.borrow() != 0 {
        Err(Error::new(*stop.borrow(), "Connection closing"))
    } else {
        let mut initializing = Box::pin(
            std::panic::AssertUnwindSafe(async {
                driver.connect().await?;
                notifications = Some(driver.notifications().await?);
                notification_resource = Some(
                    engine
                        .resources
                        .track(crate::resources::Kind::NotificationStream),
                );
                Ok(Vec::new())
            })
            .catch_unwind(),
        );
        let result = tokio::select! { biased;
            _ = stop.changed() => Err(Error::new(*stop.borrow(), "Connection closing")),
            _ = cancel.changed() => Err(Error::new(10, "Connect cancelled")),
            _ = tokio::time::sleep_until(connect.deadline) => Err(Error::new(9, "Connect timed out")),
            result = &mut initializing => crate::callback_boundary::recover(result, || Err(Error::new(18, "Connect panicked"))),
        };
        // Cancellation drops the real owned future inside the guard before
        // disconnect borrows the driver again. Keep cleanup failure observable
        // without replacing a cancellation/deadline/native error that won first.
        initialization_cleanup = crate::callback_boundary::invoke(
            || {
                drop(initializing);
                Ok(())
            },
            || Error::new(18, "Connection initialization future cleanup panicked"),
        );
        match &initialization_cleanup {
            Ok(()) => result,
            Err(error) => result.and(Err(error.clone())),
        }
    };
    if let Err(error) = connected {
        state.send_replace(ConnectionState::Closing);
        let cleanup = disconnect(&mut driver).await;
        finish(&engine, &connect, Err(error));
        state.send_replace(ConnectionState::Closed);
        return cleanup.and(initialization_cleanup);
    }
    state.send_replace(ConnectionState::Connected);
    finish(&engine, &connect, Ok(generation.to_le_bytes().to_vec()));
    let mut subscriptions = std::collections::BTreeMap::new();
    loop {
        if *stop.borrow() != 0 {
            break;
        }
        tokio::select! { biased;
            _ = stop.changed() => break,
            command = commands.recv() => {
                let Some(command) = command else { break };
                if !command.request.start() { continue; }
                // Rediscovery can replace OS characteristic objects while their
                // notification callbacks still belong to this generation.
                // Match the browser backend: release subscriptions first.
                if command.operation == 40 && !subscriptions.is_empty() {
                    finish(&engine, &command, Err(Error::new(16, "Release notifications before rediscovery")));
                    continue;
                }
                let mut cancel = command.request.cancel.subscribe();
                let payload = match command.payload.get(8..) {
                    Some(payload) => payload,
                    None => {
                        finish(&engine, &command, Err(Error::new(16, "Missing connection generation")));
                        continue;
                    }
                };
                let mut interrupted = false;
                let mut deferred_notifications = Vec::new();
                let pending_subscription = if command.operation == 44 {
                    let mut reader = Reader::new(payload);
                    match (reader.uuid(), reader.uuid()) {
                        (Ok(service), Ok(uuid)) => Some((service, uuid)),
                        _ => None,
                    }
                } else {
                    None
                };
                let result = if *cancel.borrow() {
                    Err(Error::new(10, "Request cancelled"))
                } else {
                    // Own the pinned future so disposal is guarded too. Dropping
                    // a Pin<&mut _> would only drop the reference, leaving the
                    // actual future to unwind outside this cleanup boundary.
                    let mut operation = Box::pin(std::panic::AssertUnwindSafe(driver.operate(command.operation, payload)).catch_unwind());
                    let deadline = tokio::time::sleep_until(command.deadline);
                    tokio::pin!(deadline);
                    let result = loop {
                        tokio::select! { biased;
                            _ = stop.changed() => { interrupted = true; break Err(Error::new(*stop.borrow(), "Connection closing")); },
                            _ = cancel.changed() => { interrupted = true; break Err(Error::new(10, "GATT request cancelled")); },
                            _ = &mut deadline => { interrupted = true; break Err(Error::new(9, "GATT request timed out")); },
                            result = &mut operation => break crate::callback_boundary::recover(result, || { interrupted = true; Err(Error::new(18, "GATT operation panicked")) }),
                            value = next_notification(&mut notifications) => {
                                let Some(value) = value else { interrupted = true; break Err(Error::new(8, "Notification transport ended during GATT operation")); };
                                let value = match value {
                                    Ok(value) => value,
                                    Err(error) => {
                                        emit_notification_error(&engine, generation, error.clone());
                                        interrupted = true;
                                        break Err(error);
                                    }
                                };
                                if subscriptions.contains_key(&(value.service_uuid, value.uuid)) {
                                    emit_notification(&engine, generation, value);
                                } else if pending_subscription == Some((value.service_uuid, value.uuid)) {
                                    if deferred_notifications.len() == 1024 {
                                        interrupted = true;
                                        break Err(Error::new(8, "Notification backlog overflowed during subscription setup"));
                                    }
                                    deferred_notifications.push(value);
                                }
                            }
                        }
                    };
                    let disposed = crate::callback_boundary::invoke(
                        || { drop(operation); Ok(()) },
                        || Error::new(18, "GATT future cleanup panicked"),
                    );
                    match disposed {
                        Ok(()) => result,
                        Err(error) => {
                            interrupted = true;
                            emit_notification_error(&engine, generation, error.clone());
                            // Preserve cancellation/deadline/native failure if
                            // one already won; cleanup failure overrides success.
                            result.and(Err(error))
                        }
                    }
                };
                if result.is_ok() && matches!(command.operation, 44 | 45) {
                    let mut reader = Reader::new(payload);
                    if let (Ok(service), Ok(uuid)) = (reader.uuid(), reader.uuid()) {
                        if command.operation == 44 { subscriptions.entry((service, uuid)).or_insert_with(|| engine.resources.track(crate::resources::Kind::Subscription)); } else { subscriptions.remove(&(service, uuid)); }
                    }
                }
                // A lost adapter or permission cannot support further GATT work
                // on this physical generation, even before its state event arrives.
                if matches!(&result, Err(error) if matches!(error.code, 1 | 2 | 3 | 8)) { interrupted = true; }
                let succeeded = result.is_ok();
                finish(&engine, &command, result);
                if succeeded {
                    for value in deferred_notifications {
                        emit_notification(&engine, generation, value);
                    }
                }
                if interrupted { break; }
            }
            value = next_notification(&mut notifications) => {
                let Some(value) = value else { break };
                let value = match value { Ok(value) => value, Err(error) => { emit_notification_error(&engine, generation, error); break; } };
                if subscriptions.contains_key(&(value.service_uuid, value.uuid)) {
                    emit_notification(&engine, generation, value);
                }
            }
        }
    }
    state.send_replace(ConnectionState::Closing);
    commands.close();
    while let Some(command) = commands.recv().await {
        if command.request.start() {
            finish(
                &engine,
                &command,
                Err(Error::new(
                    if *stop.borrow() == 17 { 17 } else { 8 },
                    "Connection generation ended",
                )),
            );
        }
    }
    let result = disconnect(&mut driver).await;
    state.send_replace(ConnectionState::Closed);
    emit(&engine, 5, generation, Vec::new());
    result
}

#[cfg(test)]
#[path = "connection/tests.rs"]
mod tests;

#[cfg(test)]
mod notification_policy_tests {
    use super::native::notification_compatibility;
    #[test]
    fn empty_and_zero_are_strict_and_only_one_selects_compatibility() {
        for payload in [&[][..], &[0][..]] {
            assert_eq!(notification_compatibility(payload), Ok(false));
        }
        assert_eq!(notification_compatibility(&[1]), Ok(true));
        for payload in [&[2][..], &[1, 0][..], &[0, 1][..]] {
            assert!(notification_compatibility(payload).is_err());
        }
    }
}
