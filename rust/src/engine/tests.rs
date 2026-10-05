use super::*;
use crate::event::EventSink;
struct ChannelSink(mpsc::UnboundedSender<Vec<u8>>);
impl EventSink for ChannelSink {
    fn send(&self, event: &[u8]) -> bool {
        self.0.send(event.to_vec()).is_ok()
    }
}
struct PanickingPayload(Arc<AtomicU64>);
impl Drop for PanickingPayload {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
        panic!("injected join payload destructor failure");
    }
}
struct FutureDrop {
    panics: bool,
    drops: Arc<AtomicU64>,
    payload_drops: Arc<AtomicU64>,
}
impl Drop for FutureDrop {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
        if self.panics {
            std::panic::panic_any(PanickingPayload(self.payload_drops.clone()));
        }
    }
}

#[tokio::test]
async fn task_error_releases_panicking_payload_and_preserves_string_cause() {
    let drops = Arc::new(AtomicU64::new(0));
    let captured = drops.clone();
    let error = tokio::spawn(async move {
        std::panic::panic_any(PanickingPayload(captured));
    })
    .await
    .unwrap_err();
    let failure = task_error(error, "Injected task");
    assert_eq!(failure.code, 18);
    assert!(failure.message.starts_with("Injected task:"));
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(Arc::strong_count(&drops), 1);
    let error = tokio::spawn(async {
        panic!("original task cause");
    })
    .await
    .unwrap_err();
    assert!(
        task_error(error, "Worker")
            .message
            .contains("original task cause")
    );
}

#[tokio::test]
async fn cancelled_tasks_join_future_drop_and_release_all_owners_even_after_panic() {
    for panics in [false, true] {
        let (sink, _events) = mpsc::unbounded_channel();
        let (engine, _) = testing_engine(Arc::new(ChannelSink(sink)));
        let resources = engine.resources.clone();
        let drops = Arc::new(AtomicU64::new(0));
        let payload_drops = Arc::new(AtomicU64::new(0));
        let mut tasks = JoinSet::new();
        for task_panics in [panics, false] {
            let (started, entered) = tokio::sync::oneshot::channel();
            let guard = FutureDrop {
                panics: task_panics,
                drops: drops.clone(),
                payload_drops: payload_drops.clone(),
            };
            let owner = resources.clone();
            tasks.spawn(async move {
                let _guard = guard;
                let _resource = owner.track(crate::resources::Kind::Subscription);
                started.send(()).unwrap();
                std::future::pending::<()>().await;
            });
            entered.await.unwrap();
        }
        assert_eq!(resources.snapshot(), [0, 0, 0, 2]);
        let failure = tokio::time::timeout(Duration::from_secs(2), cancel_tasks(&mut tasks))
            .await
            .unwrap();
        assert!(tasks.is_empty());
        assert_eq!(resources.snapshot(), [0, 0, 0, 0]);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
        assert_eq!(Arc::strong_count(&drops), 1);
        assert_eq!(payload_drops.load(Ordering::Relaxed), u64::from(panics));
        assert_eq!(Arc::strong_count(&payload_drops), 1);
        if panics {
            let failure = failure.unwrap();
            assert_eq!(failure.code, 18);
            assert!(failure.message.contains("during shutdown"));
            engine.fail_shutdown(failure.clone());
            assert_eq!(engine.shutdown_result(false), Err(failure));
        } else {
            assert!(failure.is_none());
        }
    }
}

#[test]
fn recorded_task_cause_survives_engine_panic_shutdown() {
    let (sink, _events) = mpsc::unbounded_channel();
    let (engine, _) = testing_engine(Arc::new(ChannelSink(sink)));
    let failure = Error::new(18, "Original joined task cause");
    engine.fail_shutdown(failure.clone());
    assert_eq!(engine.shutdown_result(true), Err(failure));
}

#[test]
fn engine_admission_bounds_starts_and_rolls_back_queue_failure() {
    let registry = Mutex::new(HashMap::new());
    let (starts, mut receiver) = mpsc::channel(MAX_ENGINES);
    let mut owners = Vec::new();
    for id in 1..=MAX_ENGINES as u64 {
        let (sink, _) = mpsc::unbounded_channel();
        let (engine, stop) = testing_engine(Arc::new(ChannelSink(sink)));
        owners.push(Arc::downgrade(&engine));
        // Closing engines still occupy admission slots until joined cleanup.
        engine.stop.send_replace(true);
        let (_, commands) = mpsc::channel(1);
        assert_eq!(
            start_engine(&registry, &starts, (id, engine, commands, stop)),
            Ok(id)
        );
    }
    assert_eq!(registry.lock().unwrap().len(), MAX_ENGINES);
    assert_eq!(starts.capacity(), 0);
    let (sink, _) = mpsc::unbounded_channel();
    let (denied, stop) = testing_engine(Arc::new(ChannelSink(sink)));
    let (_, commands) = mpsc::channel(1);
    let error =
        start_engine(&registry, &starts, (999, denied.clone(), commands, stop)).unwrap_err();
    assert_eq!(error.code, 16);
    assert!(error.message.contains("Process engine capacity"));
    assert_eq!(Arc::strong_count(&denied), 1);
    for _ in 0..MAX_ENGINES {
        drop(receiver.try_recv().unwrap());
    }
    assert!(receiver.try_recv().is_err());
    registry.lock().unwrap().clear();
    assert!(owners.iter().all(|owner| owner.upgrade().is_none()));

    // A full or disconnected start channel must roll back registration and
    // release the rejected start even if the engine registry has capacity.
    for closed in [false, true] {
        let (starts, receiver) = mpsc::channel(1);
        let (_, commands) = mpsc::channel(1);
        let (_, stop) = watch::channel(false);
        start_engine(&registry, &starts, (1, denied.clone(), commands, stop)).unwrap();
        if closed {
            drop(receiver);
        }
        let (_, commands) = mpsc::channel(1);
        let (_, stop) = watch::channel(false);
        let error =
            start_engine(&registry, &starts, (2, denied.clone(), commands, stop)).unwrap_err();
        assert_eq!(error.code, if closed { 18 } else { 16 });
        assert_eq!(registry.lock().unwrap().len(), 1);
        assert!(!registry.lock().unwrap().contains_key(&2));
        registry.lock().unwrap().clear();
        drop(starts);
    }
    assert_eq!(Arc::strong_count(&denied), 1);
}

#[test]
fn concurrent_engine_admission_never_exceeds_process_or_start_capacity() {
    let registry = Mutex::new(HashMap::new());
    let (starts, mut receiver) = mpsc::channel(MAX_ENGINES);
    let accepted = AtomicU64::new(0);
    let owners = Mutex::new(Vec::new());
    let barrier = std::sync::Barrier::new(8);
    std::thread::scope(|scope| {
        for worker in 0..8 {
            let registry = &registry;
            let starts = &starts;
            let accepted = &accepted;
            let owners = &owners;
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                for attempt in 0..32 {
                    let id = worker * 32 + attempt + 1;
                    let (sink, _) = mpsc::unbounded_channel();
                    let (engine, stop) = testing_engine(Arc::new(ChannelSink(sink)));
                    owners.lock().unwrap().push(Arc::downgrade(&engine));
                    let (_, commands) = mpsc::channel(1);
                    match start_engine(registry, starts, (id, engine, commands, stop)) {
                        Ok(_) => {
                            accepted.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(error) => assert_eq!(error.code, 16),
                    }
                }
            });
        }
    });
    assert_eq!(accepted.load(Ordering::Relaxed), MAX_ENGINES as u64);
    assert_eq!(registry.lock().unwrap().len(), MAX_ENGINES);
    assert_eq!(starts.capacity(), 0);
    for _ in 0..MAX_ENGINES {
        drop(receiver.try_recv().unwrap());
    }
    assert!(receiver.try_recv().is_err());
    registry.lock().unwrap().clear();
    assert!(
        owners
            .lock()
            .unwrap()
            .iter()
            .all(|owner| owner.upgrade().is_none())
    );
}

#[tokio::test]
async fn supervisor_recovers_failed_task_even_with_poisoned_registries() {
    for poisoned in [false, true] {
        let registry = Arc::new(Mutex::new(HashMap::new()));
        let (starts, receiver) = mpsc::channel(MAX_ENGINES);
        let (sink, mut events) = mpsc::unbounded_channel();
        let (failed, stop) = testing_engine(Arc::new(ChannelSink(sink)));
        failed.close_id.store(99, Ordering::Release);
        failed
            .requests
            .lock()
            .unwrap()
            .insert(42, Arc::new(Request::new()));
        registry.lock().unwrap().insert(1, failed.clone());
        if poisoned {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _guard = registry.lock().unwrap();
                panic!("injected registry poison");
            }));
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _guard = failed.requests.lock().unwrap();
                panic!("injected request poison");
            }));
        }
        let drops = Arc::new(AtomicU64::new(0));
        let captured = drops.clone();
        let supervisor = tokio::spawn(supervise(
            registry.clone(),
            receiver,
            move |engine, _, _| {
                let captured = captured.clone();
                async move {
                    let _resource = engine.resources.track(crate::resources::Kind::Subscription);
                    if engine.close_id.load(Ordering::Acquire) == 99 {
                        std::panic::panic_any(PanickingPayload(captured));
                    }
                    false
                }
            },
        ));
        let (_, commands) = mpsc::channel(1);
        starts
            .send((1, failed.clone(), commands, stop))
            .await
            .unwrap();
        let request = receive(&mut events).await;
        assert_eq!(u32::from_le_bytes(request[..4].try_into().unwrap()), 1);
        assert_eq!(u64::from_le_bytes(request[4..12].try_into().unwrap()), 42);
        assert_eq!(code(&request), 18);
        let closed = receive(&mut events).await;
        assert_eq!(u32::from_le_bytes(closed[..4].try_into().unwrap()), 2);
        assert_eq!(u64::from_le_bytes(closed[4..12].try_into().unwrap()), 99);
        assert_eq!(code(&closed), 18);
        assert!(String::from_utf8_lossy(&closed).contains("Engine task terminated"));
        assert!(*failed.stop.borrow());
        assert!(
            failed
                .requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
        );
        assert!(
            registry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
        );
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(Arc::strong_count(&drops), 2); // Test owner plus live runner closure.

        // The same supervisor must still accept and acknowledge another engine.
        let (sink, mut events) = mpsc::unbounded_channel();
        let (healthy, stop) = testing_engine(Arc::new(ChannelSink(sink)));
        healthy.close_id.store(100, Ordering::Release);
        registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(2, healthy.clone());
        let (_, commands) = mpsc::channel(1);
        starts
            .send((2, healthy.clone(), commands, stop))
            .await
            .unwrap();
        assert_eq!(code(&receive(&mut events).await), 0);
        drop(starts);
        tokio::time::timeout(Duration::from_secs(2), supervisor)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(Arc::strong_count(&drops), 1);
        assert_eq!(Arc::strong_count(&failed), 1);
        assert_eq!(Arc::strong_count(&healthy), 1);
        assert_eq!(failed.resources.snapshot(), [0, 0, 0, 0]);
        assert_eq!(healthy.resources.snapshot(), [0, 0, 0, 0]);
        assert!(events.try_recv().is_err());
    }
}

fn new_engine() -> (u64, mpsc::UnboundedReceiver<Vec<u8>>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (open(Arc::new(ChannelSink(tx))).unwrap(), rx)
}
async fn receive(rx: &mut mpsc::UnboundedReceiver<Vec<u8>>) -> Vec<u8> {
    tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap()
}
fn code(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes[12..16].try_into().unwrap())
}
#[tokio::test]
async fn cancellation_timeout_and_shutdown_are_terminal() {
    let (engine, mut rx) = new_engine();
    let id = submit(engine, 3, vec![], 1000).unwrap();
    cancel(engine, id).unwrap();
    assert_eq!(code(&receive(&mut rx).await), 10);
    cancel(engine, id).unwrap();
    submit(engine, 3, vec![], 1).unwrap();
    assert_eq!(code(&receive(&mut rx).await), 9);
    let closed = close(engine).unwrap();
    let event = receive(&mut rx).await;
    assert_eq!(u64::from_le_bytes(event[4..12].try_into().unwrap()), closed);
    assert_eq!(code(&event), 0);
    assert!(submit(engine, 1, vec![], 1000).is_err());
    assert_eq!(close(engine).unwrap(), 0);
}
#[tokio::test]
async fn shutdown_cancels_pending_requests_before_ack() {
    let (engine, mut rx) = new_engine();
    let id = submit(engine, 3, vec![], 60_000).unwrap();
    close(engine).unwrap();
    let event = receive(&mut rx).await;
    assert_eq!(u64::from_le_bytes(event[4..12].try_into().unwrap()), id);
    assert_eq!(code(&event), 17);
    let ack = receive(&mut rx).await;
    assert_eq!(u32::from_le_bytes(ack[0..4].try_into().unwrap()), 2);
    assert!(super::engine(engine).is_err());
}
#[tokio::test]
async fn repeat_engine_lifecycle_without_retained_handles() {
    let mut previous = 0;
    for _ in 0..100 {
        let (engine, mut rx) = new_engine();
        assert!(engine > previous);
        previous = engine;
        submit(engine, 1, vec![0, 255, 128], 1000).unwrap();
        assert_eq!(&receive(&mut rx).await[16..], &[0, 255, 128]);
        close(engine).unwrap();
        receive(&mut rx).await;
        assert!(super::engine(engine).is_err());
    }
}
#[tokio::test]
async fn shutdown_reports_cleanup_failure_in_acknowledgement() {
    let (handle, mut rx) = new_engine();
    engine(handle)
        .unwrap()
        .fail_shutdown(Error::new(15, "OS scanner stop failed"));
    close(handle).unwrap();
    let acknowledgement = receive(&mut rx).await;
    assert_eq!(code(&acknowledgement), 15);
    assert_eq!(&acknowledgement[16..], b"OS scanner stop failed");
    assert!(engine(handle).is_err());
}
#[tokio::test]
async fn queued_request_cancel_and_deadline_skip_platform_work() {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (engine, _) = testing_engine(Arc::new(ChannelSink(tx)));
    let command = testing_command(20, Instant::now() + Duration::from_secs(60));
    let request = command.request.clone();
    request.cancel.send_replace(true);
    wait_queued(engine.clone(), command).await;
    assert_eq!(code(&receive(&mut rx).await), 10);
    assert!(!request.start());
    let command = testing_command(20, Instant::now());
    let request = command.request.clone();
    wait_queued(engine.clone(), command).await;
    assert_eq!(code(&receive(&mut rx).await), 9);
    assert!(!request.start());
}
#[tokio::test(start_paused = true)]
async fn idle_engine_probes_port_and_joins_on_failed_delivery() {
    let (tx, mut events) = mpsc::unbounded_channel();
    let (engine, stop) = testing_engine(Arc::new(ChannelSink(tx)));
    let (_commands, receiver) = mpsc::channel(1);
    let task = tokio::spawn(run(engine.clone(), receiver, stop));
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(999)).await;
    assert!(events.try_recv().is_err());
    tokio::time::advance(Duration::from_millis(1)).await;
    let heartbeat = events.recv().await.unwrap();
    assert_eq!(heartbeat, event(9, 0, Ok(Vec::new())));
    assert!(!*engine.stop.borrow());
    drop(events);
    tokio::time::advance(Duration::from_secs(1)).await;
    task.await.unwrap();
    assert!(*engine.stop.borrow());
    assert!(engine.requests.lock().unwrap().is_empty());
}

#[test]
fn request_cannot_finish_twice_or_restart() {
    let request = Request::new();
    assert!(request.start());
    assert!(request.finish(State::Cancelled));
    assert!(!request.finish(State::Completed));
    assert!(!request.start());
}
