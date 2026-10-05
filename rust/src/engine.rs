//! Process runtime and explicit engine/request ownership. No unsafe code here.
use crate::{
    codec::{Error, event},
    event::Sink,
};
use futures_util::FutureExt;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    runtime::Runtime,
    sync::{mpsc, watch},
    task::JoinSet,
    time::Instant,
};

const MAX_PENDING: usize = 1024;
// Closing engines retain their slot until their task joins and registry retirement.
const MAX_ENGINES: usize = 128;
static NEXT_ID: AtomicU64 = AtomicU64::new(1);
static HOST: OnceLock<Result<Host, Error>> = OnceLock::new();

pub(crate) fn next_id() -> Result<u64, Error> {
    NEXT_ID
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| {
            (id < i64::MAX as u64).then_some(id + 1)
        })
        .map_err(|_| Error::new(18, "Native handle space exhausted"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum State {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
}

pub(crate) struct Request {
    state: AtomicU8,
    pub(crate) cancel: watch::Sender<bool>,
}

impl Request {
    fn new() -> Self {
        let (cancel, _) = watch::channel(false);
        Self {
            state: AtomicU8::new(State::Queued as u8),
            cancel,
        }
    }
    pub(crate) fn start(&self) -> bool {
        let started = self
            .state
            .compare_exchange(
                State::Queued as u8,
                State::Running as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();
        if started {
            self.cancel.send_modify(|_| {});
        }
        started
    }
    fn finish(&self, state: State) -> bool {
        self.state
            .compare_exchange(
                State::Running as u8,
                state as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }
}

pub(crate) struct Command {
    id: u64,
    pub(crate) operation: u32,
    pub(crate) payload: Vec<u8>,
    pub(crate) deadline: Instant,
    pub(crate) request: Arc<Request>,
}

pub(crate) struct Engine {
    driver: DriverMode,
    pub(crate) sink: Sink,
    pub(crate) resources: crate::resources::Resources,
    commands: mpsc::Sender<Command>,
    pub(crate) stop: watch::Sender<bool>,
    requests: Mutex<HashMap<u64, Arc<Request>>>,
    close_id: AtomicU64,
    shutdown_failure: Mutex<Option<Error>>,
}

#[derive(Clone, Copy)]
enum DriverMode {
    Native,
    #[cfg(feature = "test-support")]
    Fixture,
}

impl Engine {
    pub(crate) fn fail_shutdown(&self, error: Error) {
        if let Ok(mut failure) = self.shutdown_failure.lock()
            && failure.is_none()
        {
            *failure = Some(error);
        }
    }
    #[cfg(test)]
    pub(crate) fn testing_shutdown_failure(&self) -> Option<Error> {
        self.shutdown_failure.lock().unwrap().clone()
    }
    fn shutdown_result(&self, panicked: bool) -> Result<Vec<u8>, Error> {
        self.shutdown_failure
            .lock()
            .map_err(|_| Error::new(18, "Shutdown state poisoned"))?
            .take()
            .map_or_else(
                || {
                    if panicked {
                        Err(Error::new(18, "Native engine panicked"))
                    } else {
                        Ok(Vec::new())
                    }
                },
                Err,
            )
    }
}

type Start = (
    u64,
    Arc<Engine>,
    mpsc::Receiver<Command>,
    watch::Receiver<bool>,
);

struct Host {
    _runtime: Runtime,
    engines: Arc<Mutex<HashMap<u64, Arc<Engine>>>>,
    starts: mpsc::Sender<Start>,
    // Process-wide supervisor owns and joins every engine task.
    _supervisor: tokio::task::JoinHandle<()>,
}

// JoinError owns the caught panic payload, whose destructor may also panic.
pub(crate) fn task_error(error: tokio::task::JoinError, context: &str) -> Error {
    let failure = Error::new(18, format!("{context}: {error}"));
    if error.is_panic() {
        crate::callback_boundary::recover(Err(error.into_panic()), || failure)
    } else {
        failure
    }
}

async fn cancel_tasks<T: 'static>(tasks: &mut JoinSet<T>) -> Option<Error> {
    tasks.abort_all();
    let mut failure = None;
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            // Expected cancellation owns no panic payload and is not a failure.
            if error.is_cancelled() {
                continue;
            }
            let error = task_error(error, "Request task terminated during shutdown");
            if failure.is_none() {
                failure = Some(error);
            }
        }
    }
    failure
}

async fn run_guarded(
    engine: Arc<Engine>,
    commands: mpsc::Receiver<Command>,
    stop: watch::Receiver<bool>,
) -> bool {
    // Platform/backend panics become a controlled engine failure.
    let outcome = std::panic::AssertUnwindSafe(run(engine, commands, stop))
        .catch_unwind()
        .await;
    crate::callback_boundary::recover(outcome.map(|()| false), || true)
}

async fn supervise<F>(
    registry: Arc<Mutex<HashMap<u64, Arc<Engine>>>>,
    mut receiver: mpsc::Receiver<Start>,
    run_engine: impl Fn(Arc<Engine>, mpsc::Receiver<Command>, watch::Receiver<bool>) -> F,
) where
    F: std::future::Future<Output = bool> + Send + 'static,
{
    let mut tasks = JoinSet::new();
    // Retain the engine independently of the task result and registry lock, so
    // even an unexpected JoinError or poisoned registry cannot lose cleanup.
    let mut task_engines = HashMap::new();
    loop {
        tokio::select! {
            start = receiver.recv() => {
                let Some((id, engine, commands, stop)) = start else { break };
                let task = tasks.spawn(run_engine(engine.clone(), commands, stop));
                task_engines.insert(task.id(), (id, engine));
            }
            done = tasks.join_next_with_id(), if !tasks.is_empty() => {
                let outcome = match done {
                    Some(Ok((task_id, panicked))) => task_engines.remove(&task_id)
                        .map(|(id, engine)| (id, engine, panicked)),
                    Some(Err(error)) => {
                        let engine = task_engines.remove(&error.id());
                        let failure = task_error(error, "Engine task terminated");
                        engine.map(|(id, engine)| {
                            engine.fail_shutdown(failure);
                            (id, engine, true)
                        })
                    },
                    None => None,
                };
                if let Some((id, engine, panicked)) = outcome {
                    engine.stop.send_replace(true);
                    // Poison records a failed operation, but the owned map is
                    // still valid and must be drained to release its resources.
                    registry.lock().unwrap_or_else(std::sync::PoisonError::into_inner).remove(&id);
                    let pending: Vec<_> = engine.requests.lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner).drain().collect();
                    for (id, _) in pending {
                        engine.sink.send(&event(1, id, Err(Error::new(if panicked {18} else {17}, "Engine shut down"))));
                    }
                    let close_id = engine.close_id.load(Ordering::Acquire);
                    // Acknowledgement follows task termination and registry removal.
                    engine.sink.send(&event(2, close_id, engine.shutdown_result(panicked)));
                }
            }
        }
    }
    let _ = cancel_tasks(&mut tasks).await;
}

impl Host {
    fn new() -> Result<Self, Error> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| Error::new(18, format!("Cannot create runtime: {e}")))?;
        let engines = Arc::new(Mutex::new(HashMap::<u64, Arc<Engine>>::new()));
        let (starts, receiver) = mpsc::channel::<Start>(MAX_ENGINES);
        let registry = engines.clone();
        let supervisor = runtime.spawn(supervise(registry, receiver, run_guarded));
        Ok(Self {
            _runtime: runtime,
            engines,
            starts,
            _supervisor: supervisor,
        })
    }
}

fn host() -> Result<&'static Host, Error> {
    HOST.get_or_init(Host::new).as_ref().map_err(Clone::clone)
}

pub(crate) fn open(sink: Sink) -> Result<u64, Error> {
    open_with_driver(sink, DriverMode::Native)
}
#[cfg(feature = "test-support")]
pub(crate) fn open_fixture(sink: Sink) -> Result<u64, Error> {
    open_with_driver(sink, DriverMode::Fixture)
}
fn open_with_driver(sink: Sink, driver: DriverMode) -> Result<u64, Error> {
    let host = host()?;
    let id = next_id()?;
    let (commands, receiver) = mpsc::channel(MAX_PENDING);
    let (stop, stopped) = watch::channel(false);
    let engine = Arc::new(Engine {
        driver,
        resources: crate::resources::Resources::default(),
        sink,
        commands,
        stop,
        requests: Mutex::new(HashMap::new()),
        close_id: AtomicU64::new(0),
        shutdown_failure: Mutex::new(None),
    });
    start_engine(&host.engines, &host.starts, (id, engine, receiver, stopped))
}

fn start_engine(
    registry: &Mutex<HashMap<u64, Arc<Engine>>>,
    starts: &mpsc::Sender<Start>,
    start: Start,
) -> Result<u64, Error> {
    let id = start.0;
    let mut engines = registry
        .lock()
        .map_err(|_| Error::new(18, "Engine registry poisoned"))?;
    if engines.len() >= MAX_ENGINES {
        return Err(Error::new(16, "Process engine capacity exceeded"));
    }
    engines.insert(id, start.1.clone());
    if let Err(error) = starts.try_send(start) {
        engines.remove(&id);
        return Err(match error {
            mpsc::error::TrySendError::Full(_) => {
                Error::new(16, "Engine start queue capacity exceeded")
            }
            mpsc::error::TrySendError::Closed(_) => {
                Error::new(18, "Runtime supervisor unavailable")
            }
        });
    }
    Ok(id)
}

fn engine(handle: u64) -> Result<Arc<Engine>, Error> {
    host()?
        .engines
        .lock()
        .map_err(|_| Error::new(18, "Engine registry poisoned"))?
        .get(&handle)
        .cloned()
        .ok_or_else(|| Error::new(17, "Unknown or closed engine"))
}

pub(crate) fn submit(
    handle: u64,
    operation: u32,
    payload: Vec<u8>,
    timeout_ms: u32,
) -> Result<u64, Error> {
    let engine = engine(handle)?;
    let mut requests = engine
        .requests
        .lock()
        .map_err(|_| Error::new(18, "Request registry poisoned"))?;
    if *engine.stop.borrow() {
        return Err(Error::new(17, "Engine closing"));
    }
    if requests.len() >= MAX_PENDING {
        return Err(Error::new(16, "Request capacity exceeded"));
    }
    let id = next_id()?;
    let request = Arc::new(Request::new());
    requests.insert(id, request.clone());
    if engine
        .commands
        .try_send(Command {
            id,
            operation,
            payload,
            deadline: Instant::now() + Duration::from_millis(timeout_ms.into()),
            request,
        })
        .is_err()
    {
        requests.remove(&id);
        return Err(Error::new(16, "Command queue unavailable"));
    }
    Ok(id)
}

pub(crate) fn cancel(handle: u64, id: u64) -> Result<(), Error> {
    // Close/cancel is idempotent, including unknown/stale handles.
    if let Ok(engine) = engine(handle) {
        let requests = engine
            .requests
            .lock()
            .map_err(|_| Error::new(18, "Request registry poisoned"))?;
        if let Some(request) = requests.get(&id) {
            request.cancel.send_replace(true);
        }
    }
    Ok(())
}

pub(crate) fn close(handle: u64) -> Result<u64, Error> {
    let Ok(engine) = engine(handle) else {
        return Ok(0);
    };
    let _requests = engine
        .requests
        .lock()
        .map_err(|_| Error::new(18, "Request registry poisoned"))?;
    let previous = engine.close_id.load(Ordering::Acquire);
    if previous != 0 {
        return Ok(previous);
    }
    let id = next_id()?;
    engine.close_id.store(id, Ordering::Release);
    engine.stop.send_replace(true);
    Ok(id)
}

#[cfg(any(debug_assertions, feature = "test-support"))]
pub(crate) fn counts() -> (usize, usize) {
    let Some(Ok(host)) = HOST.get() else {
        return (0, 0);
    };
    let Ok(engines) = host.engines.lock() else {
        return (usize::MAX, usize::MAX);
    };
    (
        engines.len(),
        engines
            .values()
            .map(|e| e.requests.lock().map(|r| r.len()).unwrap_or(usize::MAX))
            .sum(),
    )
}

async fn run(
    engine: Arc<Engine>,
    mut commands: mpsc::Receiver<Command>,
    mut stop: watch::Receiver<bool>,
) {
    let mut tasks = JoinSet::new();
    let (adapter_commands, receiver) = mpsc::channel(MAX_PENDING);
    let mut adapter_task = JoinSet::new();
    let adapter_engine = engine.clone();
    let adapter_stop = stop.clone();
    adapter_task.spawn(async move {
        match adapter_engine.driver {
            DriverMode::Native => crate::scan::run(adapter_engine, receiver, adapter_stop).await,
            #[cfg(feature = "test-support")]
            DriverMode::Fixture => {
                crate::scan::worker(
                    crate::fixture::Adapter::new(adapter_engine.sink.clone()),
                    adapter_engine,
                    receiver,
                    adapter_stop,
                )
                .await
            }
        }
    });
    // A quiet engine otherwise never notices isolate teardown. This owned timer
    // probes only VM port liveness; it performs no BLE work and needs no reply.
    let mut liveness = tokio::time::interval_at(
        Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    );
    liveness.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            _ = liveness.tick() => {
                if !engine.sink.send(&event(9, 0, Ok(Vec::new()))) { break; }
            },
            command = commands.recv() => {
                let Some(command) = command else { break };
                if matches!(command.operation, 10 | 20 | 21 | 30 | 31 | 40..=51)
                    || (cfg!(feature = "test-support") && command.operation == 60) {
                    let waiting = Command { id: command.id, operation: command.operation, payload: Vec::new(), deadline: command.deadline, request: command.request.clone() };
                    if let Err(error) = adapter_commands.try_send(command) {
                        let command = error.into_inner();
                        if command.request.start() { finish(&engine, &command, Err(Error::new(16, "Adapter command queue unavailable"))); }
                    } else {
                        tasks.spawn(wait_queued(engine.clone(), waiting));
                    }
                } else {
                    let engine = engine.clone();
                    tasks.spawn(async move { execute(engine, command).await });
                }
            }
            done = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = done {
                    engine.fail_shutdown(task_error(error, "Request task terminated"));
                    engine.stop.send_replace(true); break;
                }
            }
            done = adapter_task.join_next(), if !adapter_task.is_empty() => {
                if let Some(Err(error)) = done { engine.fail_shutdown(task_error(error, "Adapter worker terminated")); }
                engine.stop.send_replace(true); break;
            }
        }
    }
    // Cancels all in-flight work and joins before the supervisor acknowledges.
    engine.stop.send_replace(true);
    if let Some(error) = cancel_tasks(&mut tasks).await {
        engine.fail_shutdown(error);
    }
    drop(adapter_commands);
    while let Some(done) = adapter_task.join_next().await {
        if let Err(error) = done {
            engine.fail_shutdown(task_error(error, "Adapter worker terminated"));
        }
    }
    commands.close();
}

// Queue waiting has its own cancellation/deadline observer. Once the worker
// starts the request it owns cancellation and cleanup instead.
async fn wait_queued(engine: Arc<Engine>, command: Command) {
    let mut cancelled = command.request.cancel.subscribe();
    if command.request.state.load(Ordering::Acquire) != State::Queued as u8 {
        return;
    }
    let (state, error) = if *cancelled.borrow() {
        (State::Cancelled, Error::new(10, "Queued request cancelled"))
    } else {
        tokio::select! {
            biased;
            _ = cancelled.changed() => {
                if !*cancelled.borrow() { return; }
                (State::Cancelled, Error::new(10, "Queued request cancelled"))
            }
            _ = tokio::time::sleep_until(command.deadline) => (State::TimedOut, Error::new(9, "Queued request timed out")),
        }
    };
    if command
        .request
        .state
        .compare_exchange(
            State::Queued as u8,
            state as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .is_ok()
    {
        publish(&engine, command.id, Err(error));
    }
}

async fn execute(engine: Arc<Engine>, mut command: Command) {
    if !command.request.start() {
        return;
    }
    let mut cancelled = command.request.cancel.subscribe();
    let result = if *cancelled.borrow() {
        Err(Error::new(10, "Request cancelled"))
    } else {
        tokio::select! {
            biased;
            _ = cancelled.changed() => Err(Error::new(10, "Request cancelled")),
            _ = tokio::time::sleep_until(command.deadline) => Err(Error::new(9, "Request timed out")),
            result = std::panic::AssertUnwindSafe(operation(command.operation, std::mem::take(&mut command.payload))).catch_unwind() => {
                crate::callback_boundary::recover(result, || Err(Error::new(18, "Native operation panicked")))
            }
        }
    };
    finish(&engine, &command, result);
}

pub(crate) fn finish(engine: &Engine, command: &Command, result: Result<Vec<u8>, Error>) {
    let state = match &result {
        Err(e) if e.code == 9 => State::TimedOut,
        Err(e) if e.code == 10 => State::Cancelled,
        Err(_) => State::Failed,
        Ok(_) => State::Completed,
    };
    if command.request.finish(state) {
        publish(engine, command.id, result);
    }
}

fn publish(engine: &Engine, id: u64, result: Result<Vec<u8>, Error>) {
    if let Ok(mut requests) = engine.requests.lock() {
        requests.remove(&id);
    }
    // A dead isolate's port fails safely; close the orphaned engine.
    if !engine.sink.send(&event(1, id, result)) {
        engine.stop.send_replace(true);
    }
}

async fn operation(operation: u32, payload: Vec<u8>) -> Result<Vec<u8>, Error> {
    match operation {
        1 => Ok(payload), // Binary ABI contract probe, no hardware needed.
        2 => Err(Error::new(11, "Controlled unsupported operation")),
        3 => std::future::pending().await, // Cancellable ABI contract probe.
        _ => Err(Error::new(11, "Unknown operation")),
    }
}

#[cfg(test)]
pub(crate) fn testing_engine(sink: Sink) -> (Arc<Engine>, watch::Receiver<bool>) {
    let (commands, _) = mpsc::channel(1);
    let (stop, stopped) = watch::channel(false);
    (
        Arc::new(Engine {
            driver: DriverMode::Native,
            resources: crate::resources::Resources::default(),
            sink,
            commands,
            stop,
            requests: Mutex::new(HashMap::new()),
            close_id: AtomicU64::new(0),
            shutdown_failure: Mutex::new(None),
        }),
        stopped,
    )
}
#[cfg(test)]
pub(crate) fn testing_command(operation: u32, deadline: Instant) -> Command {
    Command {
        id: next_id().unwrap(),
        operation,
        payload: Vec::new(),
        deadline,
        request: Arc::new(Request::new()),
    }
}

#[cfg(test)]
mod tests {
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
                        let _resource =
                            engine.resources.track(crate::resources::Kind::Subscription);
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
}
