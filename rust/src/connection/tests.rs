use super::native::{lookup_characteristic, require};
use super::*;
use crate::{
    engine::{testing_command, testing_engine},
    event::EventSink,
};
use btleplug::api::{CharPropFlags, Characteristic, Descriptor};
use std::collections::BTreeSet;
use tokio::{sync::oneshot, time::Instant};
struct ChannelSink(mpsc::UnboundedSender<Vec<u8>>);
impl EventSink for ChannelSink {
    fn send(&self, value: &[u8]) -> bool {
        self.0.send(value.to_vec()).is_ok()
    }
}
#[derive(Clone, Copy)]
enum PanicStage {
    Connect,
    Gatt,
    Disconnect,
}
struct PanicPayload(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for PanicPayload {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        panic!("injected payload destructor failure");
    }
}
struct PanickingDriver {
    stage: PanicStage,
    drops: Arc<std::sync::atomic::AtomicUsize>,
}
impl Driver for PanickingDriver {
    async fn connect(&mut self) -> Result<(), Error> {
        if matches!(self.stage, PanicStage::Connect) {
            std::panic::panic_any(PanicPayload(self.drops.clone()));
        }
        Ok(())
    }
    async fn disconnect(&mut self) -> Result<(), Error> {
        if matches!(self.stage, PanicStage::Disconnect) {
            std::panic::panic_any(PanicPayload(self.drops.clone()));
        }
        Ok(())
    }
    async fn notifications(&mut self) -> Result<Notifications, Error> {
        Ok(Box::pin(futures_util::stream::pending()))
    }
    async fn operate(&mut self, _: u32, _: &[u8]) -> Result<Vec<u8>, Error> {
        if matches!(self.stage, PanicStage::Gatt) {
            std::panic::panic_any(PanicPayload(self.drops.clone()));
        }
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn worker_releases_panicking_payload_and_finishes_cleanup() {
    for stage in [
        PanicStage::Connect,
        PanicStage::Gatt,
        PanicStage::Disconnect,
    ] {
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (sink, mut events) = mpsc::unbounded_channel();
        let (engine, _) = testing_engine(Arc::new(ChannelSink(sink)));
        let resources = engine.resources.clone();
        let (commands, receiver) = mpsc::channel(1);
        let mut gatt = testing_command(41, Instant::now() + Duration::from_secs(10));
        gatt.payload = 123u64.to_le_bytes().to_vec();
        commands.send(gatt).await.unwrap();
        drop(commands);
        let (stop, stopped) = watch::channel(0);
        let (state, states) = watch::channel(ConnectionState::Connecting);
        let connect = testing_command(30, Instant::now() + Duration::from_secs(10));
        assert!(connect.request.start());
        let task = tokio::spawn(worker(
            PanickingDriver {
                stage,
                drops: drops.clone(),
            },
            engine,
            123,
            connect,
            receiver,
            stopped,
            state,
        ));
        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        if matches!(stage, PanicStage::Disconnect) {
            assert_eq!(result.unwrap_err().code, 18);
        } else {
            assert!(result.is_ok());
        }
        assert_eq!(drops.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(Arc::strong_count(&drops), 1);
        assert_eq!(*states.borrow(), ConnectionState::Closed);
        assert_eq!(resources.snapshot(), [0, 0, 0, 0]);
        let mut errors = Vec::new();
        while let Ok(event) = events.try_recv() {
            let code = u32::from_le_bytes(event[12..16].try_into().unwrap());
            if code != 0 {
                errors.push(code);
            }
        }
        match stage {
            PanicStage::Connect => {
                assert_eq!(errors, [18]);
            }
            PanicStage::Gatt => {
                assert_eq!(errors, [18]);
            }
            PanicStage::Disconnect => assert!(errors.is_empty()),
        }
        drop(stop);
    }
}

struct Call {
    code: u32,
    payload: Vec<u8>,
    reply: oneshot::Sender<Result<Vec<u8>, Error>>,
}
struct FakeDriver {
    calls: mpsc::UnboundedSender<Call>,
    notifications: Option<mpsc::UnboundedReceiver<Result<ValueNotification, Error>>>,
    poll_panic: Option<Arc<std::sync::atomic::AtomicUsize>>,
    drop_panic: Option<Arc<std::sync::atomic::AtomicUsize>>,
    initialization_drop_panic: Option<(bool, Arc<std::sync::atomic::AtomicUsize>)>,
    cleanup_drop_panic: Option<Arc<std::sync::atomic::AtomicUsize>>,
    registry: Option<Arc<crate::peripheral_lease::Registry>>,
    lease: Option<crate::peripheral_lease::Lease>,
}
impl FakeDriver {
    async fn call(&mut self, code: u32, payload: &[u8]) -> Result<Vec<u8>, Error> {
        let (tx, rx) = oneshot::channel();
        self.calls
            .send(Call {
                code,
                payload: payload.to_vec(),
                reply: tx,
            })
            .unwrap();
        rx.await
            .unwrap_or_else(|_| Err(Error::new(8, "Cancelled fake OS call")))
    }
}
impl Driver for FakeDriver {
    fn connect(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        let guard = self
            .initialization_drop_panic
            .as_ref()
            .filter(|(setup, _)| !setup)
            .map(|(_, drops)| GattDropPanic(drops.clone()));
        DropPanickingOperation {
            future: Box::pin(async move {
                if let Some(registry) = &self.registry {
                    let lease = registry.acquire("shared-device".into())?;
                    let recover = lease.recover;
                    self.lease = Some(lease);
                    if recover {
                        self.call(31, &[]).await?;
                    }
                }
                self.call(30, &[]).await.map(|_| ())
            }),
            _guard: guard,
        }
    }
    fn disconnect(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        let guard = self.cleanup_drop_panic.clone().map(GattDropPanic);
        DropPanickingOperation {
            future: Box::pin(async move {
                if self.registry.is_some() && self.lease.is_none() {
                    return Ok(());
                }
                self.call(31, &[]).await?;
                if let Some(lease) = &mut self.lease {
                    lease.mark_clean()?;
                }
                Ok(())
            }),
            _guard: guard,
        }
    }
    fn notifications(&mut self) -> impl Future<Output = Result<Notifications, Error>> + Send {
        let guard = self
            .initialization_drop_panic
            .as_ref()
            .filter(|(setup, _)| *setup)
            .map(|(_, drops)| GattDropPanic(drops.clone()));
        let stalled = guard.is_some();
        DropPanickingOperation {
            future: Box::pin(async move {
                if stalled {
                    self.call(55, &[]).await?;
                }
                let rx = self.notifications.take().unwrap();
                let panic = self.poll_panic.take();
                let stream: Notifications = Box::pin(
                    futures_util::stream::unfold(rx, |mut rx| async move {
                        rx.recv().await.map(|v| (v, rx))
                    })
                    .map(move |value| {
                        if let Some(drops) = &panic {
                            std::panic::panic_any(PanicPayload(drops.clone()));
                        }
                        value
                    }),
                );
                Ok(stream)
            }),
            _guard: guard,
        }
    }
    fn operate(
        &mut self,
        code: u32,
        payload: &[u8],
    ) -> impl Future<Output = Result<Vec<u8>, Error>> + Send {
        let guard = if code == 41 {
            self.drop_panic.clone().map(GattDropPanic)
        } else {
            None
        };
        DropPanickingOperation {
            future: Box::pin(self.call(code, payload)),
            _guard: guard,
        }
    }
}
struct DropPanickingOperation<F> {
    future: Pin<Box<F>>,
    _guard: Option<GattDropPanic>,
}
impl<F: Future> Future for DropPanickingOperation<F> {
    type Output = F::Output;
    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(cx)
    }
}
struct GattDropPanic(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for GattDropPanic {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::panic::panic_any(PanicPayload(self.0.clone()));
    }
}
struct Harness {
    commands: mpsc::Sender<Command>,
    stop: watch::Sender<u32>,
    calls: mpsc::UnboundedReceiver<Call>,
    events: mpsc::UnboundedReceiver<Vec<u8>>,
    notifications: mpsc::UnboundedSender<Result<ValueNotification, Error>>,
    resources: crate::resources::Resources,
    state: watch::Receiver<ConnectionState>,
    task: tokio::task::JoinHandle<Result<(), Error>>,
}
#[derive(Default)]
struct DriverFaults {
    poll_panic: Option<Arc<std::sync::atomic::AtomicUsize>>,
    drop_panic: Option<Arc<std::sync::atomic::AtomicUsize>>,
    initialization_drop_panic: Option<(bool, Arc<std::sync::atomic::AtomicUsize>)>,
    cleanup_drop_panic: Option<Arc<std::sync::atomic::AtomicUsize>>,
}
impl Harness {
    fn starting() -> (Self, Arc<crate::engine::Request>) {
        Self::starting_with(None, DriverFaults::default())
    }
    fn starting_with_registry(
        registry: Option<Arc<crate::peripheral_lease::Registry>>,
    ) -> (Self, Arc<crate::engine::Request>) {
        Self::starting_with(registry, DriverFaults::default())
    }
    fn starting_with(
        registry: Option<Arc<crate::peripheral_lease::Registry>>,
        faults: DriverFaults,
    ) -> (Self, Arc<crate::engine::Request>) {
        let (sink, events) = mpsc::unbounded_channel();
        let (engine, _) = testing_engine(Arc::new(ChannelSink(sink)));
        let (commands, receiver) = mpsc::channel(1024);
        let (stop, stopped) = watch::channel(0);
        let (state, states) = watch::channel(ConnectionState::Connecting);
        let (calls, observed) = mpsc::unbounded_channel();
        let (notifications, incoming) = mpsc::unbounded_channel();
        let connect = testing_command(30, Instant::now() + Duration::from_secs(10));
        assert!(connect.request.start());
        let request = connect.request.clone();
        let resources = engine.resources.clone();
        let task = tokio::spawn(worker(
            FakeDriver {
                calls,
                notifications: Some(incoming),
                poll_panic: faults.poll_panic,
                drop_panic: faults.drop_panic,
                initialization_drop_panic: faults.initialization_drop_panic,
                cleanup_drop_panic: faults.cleanup_drop_panic,
                registry,
                lease: None,
            },
            engine,
            123,
            connect,
            receiver,
            stopped,
            state,
        ));
        let harness = Self {
            commands,
            stop,
            calls: observed,
            events,
            notifications,
            resources,
            state: states,
            task,
        };
        (harness, request)
    }
    async fn connected() -> Self {
        let (mut harness, _) = Self::starting();
        let call = harness.calls.recv().await.unwrap();
        assert_eq!(call.code, 30);
        call.reply.send(Ok(Vec::new())).unwrap();
        let connected = harness.events.recv().await.unwrap();
        assert_eq!(&connected[16..], &123u64.to_le_bytes());
        assert_eq!(*harness.state.borrow(), ConnectionState::Connected);
        assert_eq!(harness.resources.snapshot(), [0, 1, 1, 0]);
        harness
    }
    async fn enqueue(&self, code: u32, payload: Vec<u8>) -> Arc<crate::engine::Request> {
        let mut command = testing_command(code, Instant::now() + Duration::from_secs(10));
        command.payload = [123u64.to_le_bytes().as_slice(), payload.as_slice()].concat();
        let request = command.request.clone();
        self.commands.send(command).await.unwrap();
        request
    }
    async fn close(mut self) {
        self.stop.send_replace(8);
        let cleanup = self.calls.recv().await.unwrap();
        assert_eq!(cleanup.code, 31);
        cleanup.reply.send(Ok(Vec::new())).unwrap();
        assert!(self.task.await.unwrap().is_ok());
        assert_eq!(self.resources.snapshot(), [0; 4]);
        assert_eq!(*self.state.borrow(), ConnectionState::Closed);
        assert!(self.notifications.is_closed());
        assert!(self.commands.is_closed());
        let event = self.events.recv().await.unwrap();
        assert_eq!(u32::from_le_bytes(event[..4].try_into().unwrap()), 5);
        assert_eq!(u64::from_le_bytes(event[4..12].try_into().unwrap()), 123);
    }
}
async fn shared_connected(registry: Arc<crate::peripheral_lease::Registry>) -> Harness {
    let (mut harness, _) = Harness::starting_with_registry(Some(registry));
    let call = harness.calls.recv().await.unwrap();
    assert_eq!(call.code, 30);
    call.reply.send(Ok(Vec::new())).unwrap();
    assert_eq!(
        u32::from_le_bytes(
            harness.events.recv().await.unwrap()[12..16]
                .try_into()
                .unwrap()
        ),
        0
    );
    harness
}
async fn shared_rejected(registry: Arc<crate::peripheral_lease::Registry>) {
    let (mut competing, _) = Harness::starting_with_registry(Some(registry));
    let result = competing.events.recv().await.unwrap();
    assert_eq!(u32::from_le_bytes(result[12..16].try_into().unwrap()), 6);
    assert!(competing.task.await.unwrap().is_ok());
    assert!(competing.calls.try_recv().is_err());
}
#[tokio::test]
async fn competing_engine_does_not_disconnect_owned_peripheral() {
    let registry = Arc::new(crate::peripheral_lease::Registry::default());
    let first = shared_connected(registry.clone()).await;
    shared_rejected(registry.clone()).await;
    assert_eq!(*first.state.borrow(), ConnectionState::Connected);
    first.close().await;
    shared_connected(registry).await.close().await;
}
#[tokio::test]
async fn failed_disconnect_requires_recovery_before_next_os_connect() {
    let registry = Arc::new(crate::peripheral_lease::Registry::default());
    let mut first = shared_connected(registry.clone()).await;
    first.stop.send_replace(8);
    first
        .calls
        .recv()
        .await
        .unwrap()
        .reply
        .send(Err(Error::new(15, "OS cleanup failed")))
        .unwrap();
    assert_eq!(first.task.await.unwrap().unwrap_err().code, 15);
    assert_eq!(first.resources.snapshot(), [0; 4]);
    let (mut next, _) = Harness::starting_with_registry(Some(registry));
    let recovery = next.calls.recv().await.unwrap();
    assert_eq!(recovery.code, 31);
    recovery.reply.send(Ok(Vec::new())).unwrap();
    let connect = next.calls.recv().await.unwrap();
    assert_eq!(connect.code, 30);
    connect.reply.send(Ok(Vec::new())).unwrap();
    assert_eq!(
        u32::from_le_bytes(
            next.events.recv().await.unwrap()[12..16]
                .try_into()
                .unwrap()
        ),
        0
    );
    next.close().await;
}
#[tokio::test]
async fn cancellation_during_recovery_retains_lease_until_cleanup_joins() {
    let registry = Arc::new(crate::peripheral_lease::Registry::default());
    drop(registry.acquire("shared-device".into()).unwrap());
    let (mut next, request) = Harness::starting_with_registry(Some(registry.clone()));
    let recovery = next.calls.recv().await.unwrap();
    assert_eq!(recovery.code, 31);
    request.cancel.send_replace(true);
    let cleanup = next.calls.recv().await.unwrap();
    assert_eq!(cleanup.code, 31);
    shared_rejected(registry.clone()).await;
    assert!(recovery.reply.send(Ok(Vec::new())).is_err());
    cleanup.reply.send(Ok(Vec::new())).unwrap();
    assert_eq!(
        u32::from_le_bytes(
            next.events.recv().await.unwrap()[12..16]
                .try_into()
                .unwrap()
        ),
        10
    );
    assert!(next.task.await.unwrap().is_ok());
    assert_eq!(next.resources.snapshot(), [0; 4]);
    shared_connected(registry).await.close().await;
}
fn code(value: &[u8]) -> u32 {
    u32::from_le_bytes(value[12..16].try_into().unwrap())
}
#[tokio::test]
async fn rediscovery_cannot_replace_subscribed_attributes_and_recovers_after_teardown() {
    let mut h = Harness::connected().await;
    let service = uuid::Uuid::from_u128(1);
    let uuid = uuid::Uuid::from_u128(2);
    let key = [service.as_bytes().as_slice(), uuid.as_bytes().as_slice()].concat();
    h.enqueue(44, key.clone()).await;
    h.calls
        .recv()
        .await
        .unwrap()
        .reply
        .send(Ok(Vec::new()))
        .unwrap();
    assert_eq!(code(&h.events.recv().await.unwrap()), 0);
    h.enqueue(40, Vec::new()).await;
    assert_eq!(code(&h.events.recv().await.unwrap()), 16);
    assert!(h.calls.try_recv().is_err());
    assert_eq!(*h.state.borrow(), ConnectionState::Connected);
    h.notifications
        .send(Ok(ValueNotification {
            service_uuid: service,
            uuid,
            value: vec![7],
        }))
        .unwrap();
    let notification = h.events.recv().await.unwrap();
    assert_eq!(&notification[..4], &6u32.to_le_bytes());
    assert_eq!(&notification[48..], &[7]);
    h.enqueue(45, key).await;
    h.calls
        .recv()
        .await
        .unwrap()
        .reply
        .send(Ok(Vec::new()))
        .unwrap();
    assert_eq!(code(&h.events.recv().await.unwrap()), 0);
    h.enqueue(40, Vec::new()).await;
    let discovery = h.calls.recv().await.unwrap();
    assert_eq!(discovery.code, 40);
    discovery.reply.send(Ok(Vec::new())).unwrap();
    assert_eq!(code(&h.events.recv().await.unwrap()), 0);
    h.close().await;
}
#[tokio::test]
async fn fifo_failure_does_not_poison_queue_and_subscriptions_scope_notifications() {
    let mut h = Harness::connected().await;
    h.enqueue(41, vec![1, 0, 255]).await;
    h.enqueue(42, vec![2]).await;
    let read = h.calls.recv().await.unwrap();
    assert_eq!(read.code, 41);
    assert_eq!(read.payload, [1, 0, 255]);
    assert!(h.calls.try_recv().is_err());
    read.reply
        .send(Err(Error::new(15, "Read rejected")))
        .unwrap();
    assert_eq!(code(&h.events.recv().await.unwrap()), 15);
    let write = h.calls.recv().await.unwrap();
    assert_eq!(write.code, 42);
    write.reply.send(Ok(Vec::new())).unwrap();
    assert_eq!(code(&h.events.recv().await.unwrap()), 0);
    let service = uuid::Uuid::from_u128(1);
    let characteristic = uuid::Uuid::from_u128(2);
    let key = [
        service.as_bytes().as_slice(),
        characteristic.as_bytes().as_slice(),
    ]
    .concat();
    h.enqueue(44, key).await;
    let subscribe = h.calls.recv().await.unwrap();
    subscribe.reply.send(Ok(Vec::new())).unwrap();
    h.events.recv().await.unwrap();
    h.notifications
        .send(Ok(ValueNotification {
            service_uuid: uuid::Uuid::from_u128(3),
            uuid: characteristic,
            value: vec![77],
        }))
        .unwrap();
    h.notifications
        .send(Ok(ValueNotification {
            service_uuid: service,
            uuid: characteristic,
            value: vec![0, 255, 128],
        }))
        .unwrap();
    let notification = h.events.recv().await.unwrap();
    assert_eq!(u32::from_le_bytes(notification[..4].try_into().unwrap()), 6);
    assert_eq!(&notification[48..], &[0, 255, 128]);
    h.close().await;
}
#[tokio::test]
async fn cancellation_of_running_operation_closes_generation_and_skips_queued_os_work() {
    let mut h = Harness::connected().await;
    let request = h.enqueue(41, Vec::new()).await;
    let running = h.calls.recv().await.unwrap();
    assert_eq!(running.code, 41);
    h.enqueue(42, Vec::new()).await;
    request.cancel.send_replace(true);
    assert_eq!(code(&h.events.recv().await.unwrap()), 10);
    assert_eq!(code(&h.events.recv().await.unwrap()), 8);
    let cleanup = h.calls.recv().await.unwrap();
    assert_eq!(cleanup.code, 31);
    assert!(running.reply.send(Ok(vec![99])).is_err());
    cleanup.reply.send(Ok(Vec::new())).unwrap();
    assert!(h.task.await.unwrap().is_ok());
    assert_eq!(h.resources.snapshot(), [0; 4]);
    assert_eq!(*h.state.borrow(), ConnectionState::Closed);
    assert!(h.calls.try_recv().is_err());
}
#[tokio::test(start_paused = true)]
async fn gatt_adapter_or_permission_loss_retires_generation_before_queued_os_calls() {
    for failure in [1, 2, 3, 8] {
        let mut h = Harness::connected().await;
        h.enqueue(41, Vec::new()).await;
        let running = h.calls.recv().await.unwrap();
        assert_eq!(running.code, 41);
        h.enqueue(42, Vec::new()).await;
        running
            .reply
            .send(Err(Error::new(failure, "OS access lost")))
            .unwrap();
        assert_eq!(code(&h.events.recv().await.unwrap()), failure);
        let queued = tokio::time::timeout(Duration::from_secs(1), h.events.recv())
            .await
            .expect("Queued request must fail without waiting for its OS deadline")
            .unwrap();
        assert_eq!(code(&queued), 8);
        let cleanup = h.calls.recv().await.unwrap();
        assert_eq!(cleanup.code, 31, "Queued write must not touch OS");
        assert_eq!(*h.state.borrow(), ConnectionState::Closing);
        assert_eq!(h.resources.snapshot(), [0, 1, 1, 0]);
        assert!(!h.task.is_finished(), "Cleanup still owns the worker");
        cleanup.reply.send(Ok(Vec::new())).unwrap();
        assert!(h.task.await.unwrap().is_ok());
        assert_eq!(h.resources.snapshot(), [0; 4]);
        assert_eq!(*h.state.borrow(), ConnectionState::Closed);
        assert!(h.commands.is_closed());
        assert!(h.notifications.is_closed());
        assert_eq!(h.events.recv().await.unwrap()[..4], 5u32.to_le_bytes());
        assert!(h.calls.try_recv().is_err());
    }
}
#[tokio::test]
async fn cancelled_connect_joins_cleanup_before_completing_and_discards_late_os_result() {
    let (mut h, request) = Harness::starting();
    let connecting = h.calls.recv().await.unwrap();
    assert_eq!(connecting.code, 30);
    request.cancel.send_replace(true);
    let cleanup = h.calls.recv().await.unwrap();
    assert_eq!(cleanup.code, 31);
    assert!(h.events.try_recv().is_err());
    assert!(connecting.reply.send(Ok(Vec::new())).is_err());
    cleanup.reply.send(Ok(Vec::new())).unwrap();
    assert_eq!(code(&h.events.recv().await.unwrap()), 10);
    assert!(h.task.await.unwrap().is_ok());
    assert_eq!(h.resources.snapshot(), [0; 4]);
    assert_eq!(*h.state.borrow(), ConnectionState::Closed);
    assert!(h.notifications.is_closed());
}
#[tokio::test(start_paused = true)]
async fn timed_out_gatt_operation_invalidates_generation() {
    let mut h = Harness::connected().await;
    h.enqueue(41, Vec::new()).await;
    let running = h.calls.recv().await.unwrap();
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(code(&h.events.recv().await.unwrap()), 9);
    let cleanup = h.calls.recv().await.unwrap();
    assert_eq!(cleanup.code, 31);
    assert!(running.reply.send(Ok(vec![99])).is_err());
    cleanup.reply.send(Ok(Vec::new())).unwrap();
    assert!(h.task.await.unwrap().is_ok());
    assert_eq!(h.resources.snapshot(), [0; 4]);
    assert_eq!(*h.state.borrow(), ConnectionState::Closed);
}
#[tokio::test(start_paused = true)]
async fn disconnect_future_drop_panic_preserves_error_and_closes_generation() {
    for outcome in [0, 15, 9] {
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (mut h, _) = Harness::starting_with(
            None,
            DriverFaults {
                cleanup_drop_panic: Some(drops.clone()),
                ..DriverFaults::default()
            },
        );
        h.calls
            .recv()
            .await
            .unwrap()
            .reply
            .send(Ok(Vec::new()))
            .unwrap();
        assert_eq!(code(&h.events.recv().await.unwrap()), 0);
        h.enqueue(
            44,
            [
                uuid::Uuid::from_u128(1).as_bytes().as_slice(),
                uuid::Uuid::from_u128(2).as_bytes().as_slice(),
            ]
            .concat(),
        )
        .await;
        h.calls
            .recv()
            .await
            .unwrap()
            .reply
            .send(Ok(Vec::new()))
            .unwrap();
        assert_eq!(code(&h.events.recv().await.unwrap()), 0);
        h.stop.send_replace(17);
        let cleanup = h.calls.recv().await.unwrap();
        assert_eq!(cleanup.code, 31);
        assert_eq!(h.resources.snapshot(), [0, 1, 1, 1]);
        assert!(!h.task.is_finished());
        assert!(h.events.try_recv().is_err());
        match outcome {
            0 => cleanup.reply.send(Ok(Vec::new())).unwrap(),
            15 => cleanup
                .reply
                .send(Err(Error::new(15, "Native disconnect failed")))
                .unwrap(),
            9 => {
                tokio::time::advance(Duration::from_secs(5)).await;
                // Clock advancement makes the timeout ready but does not
                // itself poll the worker. Observe actual future retirement.
                let mut reply = cleanup.reply;
                tokio::time::timeout(Duration::from_secs(1), reply.closed())
                    .await
                    .unwrap();
                assert!(reply.send(Ok(Vec::new())).is_err());
            }
            _ => unreachable!(),
        }
        let joined = h.task.await;
        let panicked = joined.is_err();
        let result = match joined {
            Ok(result) => result,
            Err(error) => Err(crate::engine::task_error(
                error,
                "Worker unwound during disconnect disposal",
            )),
        };
        assert!(!panicked, "Worker must return an observable cleanup result");
        let error = result.unwrap_err();
        assert_eq!(error.code, if outcome == 0 { 18 } else { outcome });
        assert_eq!(
            error.message,
            match outcome {
                0 => "Connection cleanup future disposal panicked",
                15 => "Native disconnect failed",
                9 => "Connection cleanup timed out",
                _ => unreachable!(),
            }
        );
        assert_eq!(*h.state.borrow(), ConnectionState::Closed);
        assert_eq!(h.resources.snapshot(), [0; 4]);
        assert_eq!(h.events.recv().await.unwrap()[..4], 5u32.to_le_bytes());
        assert_eq!(drops.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(Arc::strong_count(&drops), 1);
    }
}
#[tokio::test(start_paused = true)]
async fn initialization_future_drop_panic_awaits_disconnect_and_preserves_failure() {
    for setup in [false, true] {
        for expected in [10, 9, 17] {
            for cleanup_failed in [false, true] {
                let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let (mut h, request) = Harness::starting_with(
                    None,
                    DriverFaults {
                        initialization_drop_panic: Some((setup, drops.clone())),
                        ..DriverFaults::default()
                    },
                );
                let mut pending = h.calls.recv().await.unwrap();
                assert_eq!(pending.code, 30);
                if setup {
                    pending.reply.send(Ok(Vec::new())).unwrap();
                    pending = h.calls.recv().await.unwrap();
                    assert_eq!(pending.code, 55);
                }
                match expected {
                    10 => {
                        request.cancel.send_replace(true);
                    }
                    9 => tokio::time::advance(Duration::from_secs(11)).await,
                    17 => {
                        h.stop.send_replace(17);
                    }
                    _ => unreachable!(),
                }
                let cleanup = tokio::time::timeout(Duration::from_secs(1), h.calls.recv())
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(cleanup.code, 31);
                assert!(
                    h.events.try_recv().is_err(),
                    "Connect completion must await disconnect"
                );
                assert_eq!(*h.state.borrow(), ConnectionState::Closing);
                assert!(!h.task.is_finished());
                assert!(pending.reply.send(Ok(Vec::new())).is_err());
                cleanup
                    .reply
                    .send(if cleanup_failed {
                        Err(Error::new(15, "Native disconnect failed"))
                    } else {
                        Ok(Vec::new())
                    })
                    .unwrap();
                assert_eq!(code(&h.events.recv().await.unwrap()), expected);
                let failure = h.task.await.unwrap().unwrap_err();
                assert_eq!(failure.code, if cleanup_failed { 15 } else { 18 });
                assert_eq!(
                    failure.message,
                    if cleanup_failed {
                        "Native disconnect failed"
                    } else {
                        "Connection initialization future cleanup panicked"
                    }
                );
                assert_eq!(*h.state.borrow(), ConnectionState::Closed);
                assert_eq!(h.resources.snapshot(), [0; 4]);
                assert_eq!(drops.load(std::sync::atomic::Ordering::Relaxed), 2);
                assert_eq!(Arc::strong_count(&drops), 1);
            }
        }
    }
}
#[tokio::test(start_paused = true)]
async fn gatt_future_drop_panic_preserves_failures_and_overrides_success() {
    for expected in [10, 9, 17, 0, 15] {
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (mut h, _) = Harness::starting_with(
            None,
            DriverFaults {
                drop_panic: Some(drops.clone()),
                ..DriverFaults::default()
            },
        );
        h.calls
            .recv()
            .await
            .unwrap()
            .reply
            .send(Ok(Vec::new()))
            .unwrap();
        assert_eq!(code(&h.events.recv().await.unwrap()), 0);
        let request = h.enqueue(41, Vec::new()).await;
        let mut pending = Some(h.calls.recv().await.unwrap());
        h.enqueue(42, Vec::new()).await;
        match expected {
            10 => {
                request.cancel.send_replace(true);
            }
            9 => tokio::time::advance(Duration::from_secs(11)).await,
            17 => {
                h.stop.send_replace(17);
            }
            0 => {
                pending.take().unwrap().reply.send(Ok(vec![99])).unwrap();
            }
            15 => {
                pending
                    .take()
                    .unwrap()
                    .reply
                    .send(Err(Error::new(15, "Native operation failed")))
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let diagnostic = tokio::time::timeout(Duration::from_secs(1), h.events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(u32::from_le_bytes(diagnostic[..4].try_into().unwrap()), 6);
        assert_eq!(code(&diagnostic), 18);
        assert_eq!(&diagnostic[16..], b"GATT future cleanup panicked");
        let completion = h.events.recv().await.unwrap();
        assert_eq!(code(&completion), if expected == 0 { 18 } else { expected });
        if expected == 15 {
            assert_eq!(&completion[16..], b"Native operation failed");
        }
        assert_eq!(
            code(&h.events.recv().await.unwrap()),
            if expected == 17 { 17 } else { 8 }
        );
        if let Some(pending) = pending {
            assert!(pending.reply.send(Ok(vec![99])).is_err());
        }
        let cleanup = h.calls.recv().await.unwrap();
        assert_eq!(cleanup.code, 31);
        assert!(!h.task.is_finished());
        cleanup.reply.send(Ok(Vec::new())).unwrap();
        assert!(h.task.await.unwrap().is_ok());
        assert_eq!(*h.state.borrow(), ConnectionState::Closed);
        assert_eq!(h.resources.snapshot(), [0; 4]);
        assert_eq!(drops.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(Arc::strong_count(&drops), 1);
    }
}
#[tokio::test(start_paused = true)]
async fn notification_poll_panic_disconnects_and_joins_idle_or_running_generation() {
    for running in [false, true] {
        let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (mut h, _) = Harness::starting_with(
            None,
            DriverFaults {
                poll_panic: Some(drops.clone()),
                ..DriverFaults::default()
            },
        );
        h.calls
            .recv()
            .await
            .unwrap()
            .reply
            .send(Ok(Vec::new()))
            .unwrap();
        assert_eq!(code(&h.events.recv().await.unwrap()), 0);
        h.enqueue(
            44,
            [
                uuid::Uuid::from_u128(1).as_bytes().as_slice(),
                uuid::Uuid::from_u128(2).as_bytes().as_slice(),
            ]
            .concat(),
        )
        .await;
        h.calls
            .recv()
            .await
            .unwrap()
            .reply
            .send(Ok(Vec::new()))
            .unwrap();
        assert_eq!(code(&h.events.recv().await.unwrap()), 0);
        let call = if running {
            h.enqueue(41, Vec::new()).await;
            let call = h.calls.recv().await.unwrap();
            h.enqueue(42, Vec::new()).await;
            Some(call)
        } else {
            None
        };
        h.notifications
            .send(Err(Error::new(15, "Trigger controlled poll panic")))
            .unwrap();
        let diagnostic = tokio::time::timeout(Duration::from_secs(1), h.events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(u32::from_le_bytes(diagnostic[..4].try_into().unwrap()), 6);
        assert_eq!(code(&diagnostic), 18);
        assert_eq!(&diagnostic[16..], b"Notification stream panicked");
        if let Some(call) = call {
            assert_eq!(code(&h.events.recv().await.unwrap()), 18);
            assert_eq!(code(&h.events.recv().await.unwrap()), 8);
            assert!(call.reply.send(Ok(vec![99])).is_err());
        }
        let cleanup = h.calls.recv().await.unwrap();
        assert_eq!(cleanup.code, 31);
        assert_eq!(h.resources.snapshot(), [0, 1, 1, 1]);
        assert!(!h.task.is_finished());
        cleanup.reply.send(Ok(Vec::new())).unwrap();
        assert!(h.task.await.unwrap().is_ok());
        assert_eq!(*h.state.borrow(), ConnectionState::Closed);
        assert_eq!(h.resources.snapshot(), [0; 4]);
        assert_eq!(drops.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(Arc::strong_count(&drops), 1);
    }
}
#[tokio::test(start_paused = true)]
async fn notification_error_preserves_cause_and_joins_idle_or_running_generation() {
    for error in [
        Error::new(15, "Controlled JNI callback failure"),
        Error::from(btleplug::Error::RuntimeError(
            "D-Bus notification transport lost".into(),
        )),
    ] {
        for running in [false, true] {
            let mut h = Harness::connected().await;
            let call = if running {
                h.enqueue(41, Vec::new()).await;
                let call = h.calls.recv().await.unwrap();
                h.enqueue(42, Vec::new()).await;
                Some(call)
            } else {
                None
            };
            h.notifications.send(Err(error.clone())).unwrap();
            let diagnostic = tokio::time::timeout(Duration::from_secs(1), h.events.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(u32::from_le_bytes(diagnostic[..4].try_into().unwrap()), 6);
            assert_eq!(code(&diagnostic), error.code);
            assert_eq!(&diagnostic[16..], error.message.as_bytes());
            if let Some(call) = call {
                let failed = h.events.recv().await.unwrap();
                assert_eq!(code(&failed), error.code);
                assert_eq!(&failed[16..], error.message.as_bytes());
                assert_eq!(code(&h.events.recv().await.unwrap()), 8);
                assert!(call.reply.send(Ok(vec![99])).is_err());
            }
            let cleanup = h.calls.recv().await.unwrap();
            assert_eq!(cleanup.code, 31);
            assert_eq!(h.resources.snapshot(), [0, 1, 1, 0]);
            assert!(!h.task.is_finished());
            cleanup.reply.send(Ok(Vec::new())).unwrap();
            assert!(h.task.await.unwrap().is_ok());
            assert_eq!(h.resources.snapshot(), [0; 4]);
        }
    }
}
#[tokio::test(start_paused = true)]
async fn terminal_notification_stream_interrupts_running_gatt_and_skips_queued_work() {
    for operation in [40, 41, 42] {
        let mut h = Harness::connected().await;
        h.enqueue(operation, Vec::new()).await;
        let running = h.calls.recv().await.unwrap();
        assert_eq!(running.code, operation);
        h.enqueue(42, Vec::new()).await;
        drop(h.notifications);
        let failed = tokio::time::timeout(Duration::from_secs(1), h.events.recv())
            .await
            .expect("Stream loss must interrupt GATT before its deadline")
            .unwrap();
        assert_eq!(code(&failed), 8);
        assert_eq!(code(&h.events.recv().await.unwrap()), 8);
        let cleanup = h.calls.recv().await.unwrap();
        assert_eq!(cleanup.code, 31, "queued work must never reach the OS");
        assert!(running.reply.send(Ok(vec![99])).is_err());
        assert_eq!(*h.state.borrow(), ConnectionState::Closing);
        assert_eq!(h.resources.snapshot(), [0, 1, 1, 0]);
        assert!(!h.task.is_finished());
        assert!(h.events.try_recv().is_err());
        cleanup.reply.send(Ok(Vec::new())).unwrap();
        assert!(h.task.await.unwrap().is_ok());
        assert_eq!(h.resources.snapshot(), [0; 4]);
        assert_eq!(*h.state.borrow(), ConnectionState::Closed);
        assert_eq!(h.events.recv().await.unwrap()[..4], 5u32.to_le_bytes());
        assert!(h.calls.try_recv().is_err());
    }
}
#[tokio::test(start_paused = true)]
async fn subscribed_notifications_are_delivered_while_gatt_is_pending() {
    let mut h = Harness::connected().await;
    let service_uuid = uuid::Uuid::from_u128(1);
    let characteristic_uuid = uuid::Uuid::from_u128(2);
    h.enqueue(
        44,
        [
            service_uuid.as_bytes().as_slice(),
            characteristic_uuid.as_bytes().as_slice(),
        ]
        .concat(),
    )
    .await;
    h.calls
        .recv()
        .await
        .unwrap()
        .reply
        .send(Ok(Vec::new()))
        .unwrap();
    assert_eq!(code(&h.events.recv().await.unwrap()), 0);
    h.enqueue(41, Vec::new()).await;
    let running = h.calls.recv().await.unwrap();
    h.notifications
        .send(Ok(ValueNotification {
            service_uuid,
            uuid: characteristic_uuid,
            value: vec![0, 255],
        }))
        .unwrap();
    let value = tokio::time::timeout(Duration::from_secs(1), h.events.recv())
        .await
        .expect("Notification must not wait for GATT completion")
        .unwrap();
    assert_eq!(value[..4], 6u32.to_le_bytes());
    assert_eq!(&value[48..], &[0, 255]);
    running.reply.send(Ok(vec![42])).unwrap();
    assert_eq!(code(&h.events.recv().await.unwrap()), 0);
    h.close().await;
}
#[tokio::test(start_paused = true)]
async fn setup_notifications_wait_for_success_and_failed_setup_discards_them() {
    for succeeds in [true, false] {
        let mut h = Harness::connected().await;
        let first = (uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
        let pending = (uuid::Uuid::from_u128(3), uuid::Uuid::from_u128(4));
        let key = |pair: (uuid::Uuid, uuid::Uuid)| {
            [pair.0.as_bytes().as_slice(), pair.1.as_bytes().as_slice()].concat()
        };
        h.enqueue(44, key(first)).await;
        h.calls
            .recv()
            .await
            .unwrap()
            .reply
            .send(Ok(Vec::new()))
            .unwrap();
        assert_eq!(code(&h.events.recv().await.unwrap()), 0);
        h.enqueue(44, key(pending)).await;
        let setup = h.calls.recv().await.unwrap();
        for (pair, value) in [(pending, 7), (first, 8)] {
            h.notifications
                .send(Ok(ValueNotification {
                    service_uuid: pair.0,
                    uuid: pair.1,
                    value: vec![value],
                }))
                .unwrap();
        }
        let barrier = tokio::time::timeout(Duration::from_secs(1), h.events.recv())
            .await
            .expect("Existing subscription must progress while new setup waits")
            .unwrap();
        assert_eq!(barrier[..4], 6u32.to_le_bytes());
        assert_eq!(&barrier[16..32], first.0.as_bytes());
        assert_eq!(&barrier[48..], &[8]);
        assert!(
            h.events.try_recv().is_err(),
            "New subscription must not deliver before setup succeeds"
        );
        setup
            .reply
            .send(if succeeds {
                Ok(Vec::new())
            } else {
                Err(Error::new(15, "Setup failed"))
            })
            .unwrap();
        assert_eq!(
            code(&h.events.recv().await.unwrap()),
            if succeeds { 0 } else { 15 }
        );
        if succeeds {
            let delivered = tokio::time::timeout(Duration::from_secs(1), h.events.recv())
                .await
                .expect("Deferred notification must settle after setup reply")
                .unwrap();
            assert_eq!(&delivered[16..32], pending.0.as_bytes());
            assert_eq!(&delivered[48..], &[7]);
        } else {
            h.notifications
                .send(Ok(ValueNotification {
                    service_uuid: first.0,
                    uuid: first.1,
                    value: vec![9],
                }))
                .unwrap();
            let delivered = tokio::time::timeout(Duration::from_secs(1), h.events.recv())
                .await
                .expect("Deferred notification must settle after setup reply")
                .unwrap();
            assert_eq!(&delivered[16..32], first.0.as_bytes());
            assert_eq!(&delivered[48..], &[9]);
        }
        h.close().await;
    }
}
#[tokio::test(start_paused = true)]
async fn setup_notification_backlog_is_bounded_and_overflow_joins_cleanup() {
    let mut h = Harness::connected().await;
    let service = uuid::Uuid::from_u128(1);
    let uuid = uuid::Uuid::from_u128(2);
    h.enqueue(
        44,
        [service.as_bytes().as_slice(), uuid.as_bytes().as_slice()].concat(),
    )
    .await;
    let setup = h.calls.recv().await.unwrap();
    h.enqueue(41, Vec::new()).await;
    for _ in 0..=1024 {
        h.notifications
            .send(Ok(ValueNotification {
                service_uuid: service,
                uuid,
                value: vec![0, 255],
            }))
            .unwrap();
    }
    let failed = tokio::time::timeout(Duration::from_secs(1), h.events.recv())
        .await
        .expect("Setup backlog overflow must not wait for the GATT deadline")
        .unwrap();
    assert_eq!(code(&failed), 8);
    assert_eq!(code(&h.events.recv().await.unwrap()), 8);
    let cleanup = h.calls.recv().await.unwrap();
    assert_eq!(cleanup.code, 31);
    assert!(setup.reply.send(Ok(Vec::new())).is_err());
    assert_eq!(h.resources.snapshot(), [0, 1, 1, 0]);
    assert!(h.events.try_recv().is_err());
    cleanup.reply.send(Ok(Vec::new())).unwrap();
    assert!(h.task.await.unwrap().is_ok());
    assert_eq!(h.resources.snapshot(), [0; 4]);
    assert_eq!(h.events.recv().await.unwrap()[..4], 5u32.to_le_bytes());
    assert!(h.calls.try_recv().is_err());
}
#[tokio::test]
async fn terminal_notification_stream_joins_disconnect_without_a_stop_command() {
    let mut h = Harness::connected().await;
    drop(h.notifications);
    let cleanup = h.calls.recv().await.unwrap();
    assert_eq!(cleanup.code, 31);
    assert_eq!(*h.state.borrow(), ConnectionState::Closing);
    assert!(h.commands.is_closed());
    assert!(h.events.try_recv().is_err());
    cleanup.reply.send(Ok(Vec::new())).unwrap();
    assert!(h.task.await.unwrap().is_ok());
    assert_eq!(h.resources.snapshot(), [0; 4]);
    assert_eq!(*h.state.borrow(), ConnectionState::Closed);
    let event = h.events.recv().await.unwrap();
    assert_eq!(u32::from_le_bytes(event[..4].try_into().unwrap()), 5);
    assert_eq!(u64::from_le_bytes(event[4..12].try_into().unwrap()), 123);
    assert!(h.calls.try_recv().is_err());
}
#[tokio::test]
async fn subscription_counts_follow_acknowledged_setup_and_teardown() {
    let mut h = Harness::connected().await;
    let key = [
        uuid::Uuid::from_u128(1).as_bytes().as_slice(),
        uuid::Uuid::from_u128(2).as_bytes().as_slice(),
    ]
    .concat();
    for (operation, succeeds, retained) in
        [(44, false, 0), (44, true, 1), (45, false, 1), (45, true, 0)]
    {
        h.enqueue(operation, key.clone()).await;
        let call = h.calls.recv().await.unwrap();
        assert_eq!(call.code, operation);
        call.reply
            .send(if succeeds {
                Ok(Vec::new())
            } else {
                Err(Error::new(15, "Controlled subscription failure"))
            })
            .unwrap();
        assert_eq!(
            code(&h.events.recv().await.unwrap()),
            if succeeds { 0 } else { 15 }
        );
        assert_eq!(h.resources.snapshot(), [0, 1, 1, retained]);
    }
    h.close().await;
}
#[tokio::test]
async fn hundred_connection_and_subscription_cycles_join_workers_and_drop_streams() {
    for _ in 0..100 {
        let mut h = Harness::connected().await;
        let key = [
            uuid::Uuid::from_u128(1).as_bytes().as_slice(),
            uuid::Uuid::from_u128(2).as_bytes().as_slice(),
        ]
        .concat();
        for operation in [44, 45] {
            h.enqueue(operation, key.clone()).await;
            let call = h.calls.recv().await.unwrap();
            assert_eq!(call.code, operation);
            call.reply.send(Ok(Vec::new())).unwrap();
            assert_eq!(code(&h.events.recv().await.unwrap()), 0);
            assert_eq!(
                h.resources.snapshot(),
                [0, 1, 1, u64::from(operation == 44)]
            );
        }
        h.close().await;
    }
}
fn identity_service(
    service: u128,
    characteristic: u128,
    properties: CharPropFlags,
) -> btleplug::api::Service {
    let service_uuid = uuid::Uuid::from_u128(service);
    btleplug::api::Service {
        uuid: service_uuid,
        primary: true,
        characteristics: [Characteristic {
            service_uuid,
            uuid: uuid::Uuid::from_u128(characteristic),
            properties,
            descriptors: BTreeSet::new(),
        }]
        .into(),
    }
}
#[test]
fn scoped_lookup_selects_repeated_uuid_in_requested_service() {
    for characteristic in 1..=100 {
        let services = [
            identity_service(101, characteristic, CharPropFlags::READ),
            identity_service(102, characteristic, CharPropFlags::WRITE),
        ]
        .into();
        for (service, properties) in [(101, CharPropFlags::READ), (102, CharPropFlags::WRITE)] {
            let selected = lookup_characteristic(
                &services,
                uuid::Uuid::from_u128(service),
                uuid::Uuid::from_u128(characteristic),
            )
            .unwrap();
            assert_eq!(selected.service_uuid, uuid::Uuid::from_u128(service));
            assert_eq!(selected.properties, properties);
        }
    }
}
#[test]
fn unique_lookup_and_missing_scoped_identities_remain_precise() {
    let services = [
        identity_service(1, 2, CharPropFlags::READ),
        identity_service(3, 4, CharPropFlags::WRITE),
    ]
    .into();
    assert_eq!(
        lookup_characteristic(
            &services,
            uuid::Uuid::from_u128(1),
            uuid::Uuid::from_u128(2)
        )
        .unwrap()
        .service_uuid,
        uuid::Uuid::from_u128(1)
    );
    assert_eq!(
        lookup_characteristic(
            &services,
            uuid::Uuid::from_u128(99),
            uuid::Uuid::from_u128(2)
        )
        .unwrap_err()
        .code,
        12
    );
    assert_eq!(
        lookup_characteristic(
            &services,
            uuid::Uuid::from_u128(1),
            uuid::Uuid::from_u128(4)
        )
        .unwrap_err()
        .code,
        13
    );
}
#[test]
fn duplicate_characteristics_within_one_service_are_never_selected_arbitrarily() {
    let mut service = identity_service(1, 2, CharPropFlags::READ);
    service.characteristics.insert(
        identity_service(1, 2, CharPropFlags::WRITE)
            .characteristics
            .into_iter()
            .next()
            .unwrap(),
    );
    let services = [service].into();
    assert_eq!(
        lookup_characteristic(
            &services,
            uuid::Uuid::from_u128(1),
            uuid::Uuid::from_u128(2)
        )
        .unwrap_err()
        .code,
        11
    );
}
#[test]
fn service_wire_fixture_and_property_checks_match_dart() {
    let service_uuid = uuid::Uuid::from_u128(1);
    let uuid = uuid::Uuid::from_u128(2);
    let descriptor_uuid = uuid::Uuid::from_u128(3);
    let characteristic = Characteristic {
        service_uuid,
        uuid,
        properties: CharPropFlags::READ | CharPropFlags::WRITE | CharPropFlags::NOTIFY,
        descriptors: [Descriptor {
            service_uuid,
            characteristic_uuid: uuid,
            uuid: descriptor_uuid,
        }]
        .into(),
    };
    let services = [btleplug::api::Service {
        uuid: service_uuid,
        primary: true,
        characteristics: [characteristic.clone()].into(),
    }]
    .into();
    assert_eq!(
        encode_services(&services).unwrap(),
        include_bytes!("../../../test/fixtures/services.bin")
    );
    assert!(require(&characteristic, CharPropFlags::READ).is_ok());
    assert!(require(&characteristic, CharPropFlags::WRITE_WITHOUT_RESPONSE).is_err());
}
