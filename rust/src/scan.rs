//! One engine owns its adapter stream and scan intent. Android shares physical
//! scanner ownership because upstream supplies one process-wide adapter.
use crate::{
    codec::{Error, Reader, event},
    connection::{self, ConnectionState, Device, Slot},
    engine::{Command, Engine, finish, next_id},
    event::Sink,
};
mod native;
use futures_util::{FutureExt, Stream, StreamExt};
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};

pub(crate) type Events = Pin<Box<dyn Stream<Item = Result<Vec<u8>, Error>> + Send>>;
pub(crate) trait Driver: Send {
    type Device: Device;
    #[cfg(feature = "test-support")]
    fn control(&mut self, _payload: &[u8]) -> Result<Vec<u8>, Error> {
        Err(Error::new(11, "No fixture controls on this adapter"))
    }
    fn initialize(&mut self) -> impl Future<Output = Result<(u8, Events), Error>> + Send;
    fn start(&mut self) -> impl Future<Output = Result<(), Error>> + Send;
    fn stop(&mut self) -> impl Future<Output = Result<(), Error>> + Send;
    fn shutdown(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
        async { Ok(()) }
    }
    fn peripheral(
        &mut self,
        _id: &str,
    ) -> impl Future<Output = Result<Self::Device, Error>> + Send {
        async { Err(Error::new(11, "Peripheral lookup unavailable")) }
    }
}
fn adapter_error(state: u8) -> Error {
    Error::new(
        match state {
            2 => 2,
            3 => 3,
            _ => 1,
        },
        "Bluetooth adapter is not ready",
    )
}

pub(crate) async fn run(
    engine: Arc<Engine>,
    commands: mpsc::Receiver<Command>,
    stop: watch::Receiver<bool>,
) {
    worker(native::NativeDriver::default(), engine, commands, stop).await;
}
async fn cleanup(driver: &mut impl Driver) -> Result<(), Error> {
    let mut operation = Box::pin(std::panic::AssertUnwindSafe(driver.stop()).catch_unwind());
    // Borrow the owned future so timeout does not dispose it outside the guard.
    let result = match tokio::time::timeout(Duration::from_secs(5), operation.as_mut()).await {
        Ok(result) => crate::callback_boundary::recover(result, || {
            Err(Error::new(18, "Scanner cleanup panicked"))
        }),
        Err(_) => Err(Error::new(9, "Scanner cleanup timed out")),
    };
    match crate::callback_boundary::invoke(
        || {
            drop(operation);
            Ok(())
        },
        || Error::new(18, "Scanner cleanup future disposal panicked"),
    ) {
        Ok(()) => result,
        Err(error) => result.and(Err(error)),
    }
}
fn emit(sink: &Sink, stop: &watch::Sender<bool>, value: &[u8]) {
    if !sink.send(value) {
        stop.send_replace(true);
    }
}
pub(crate) async fn worker<D: Driver>(
    mut driver: D,
    engine: Arc<Engine>,
    mut commands: mpsc::Receiver<Command>,
    mut stop: watch::Receiver<bool>,
) {
    let _worker_resource = engine
        .resources
        .track(crate::resources::Kind::AdapterWorker);
    let mut events: Option<Events> = None;
    let mut state = 1;
    let mut scanning = false;
    let mut uncertain = false;
    let mut connections = HashMap::<u64, Slot<D::Device>>::new();
    let mut connection_tasks = JoinSet::new();
    'adapter: loop {
        if *stop.borrow() {
            break;
        }
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            command = commands.recv() => {
                let Some(command) = command else { break };
                if matches!(command.operation, 31 | 40..=51) {
                    if command.operation == 31 && command.payload.len() != 8 {
                        if command.request.start() { finish(&engine, &command, Err(Error::new(16, "Malformed disconnect command"))); } continue;
                    }
                    let generation = Reader::new(&command.payload).u64();
                    match generation {
                        Ok(generation) if command.operation == 31 && command.payload.len() == 8 => {
                            if let Some(slot) = connections.get_mut(&generation) {
                                // Disposal intent survives cancellation of the acknowledgement.
                                slot.stop.send_replace(8);
                                if command.request.start() { slot.closing.push(command); }
                            } else if command.request.start() { finish(&engine, &command, Ok(Vec::new())); }
                        }
                        Ok(generation) => {
                            if let Some(slot) = connections.get(&generation) {
                                if *slot.state.borrow() != ConnectionState::Connected {
                                    if command.request.start() { finish(&engine, &command, Err(Error::new(8, "Connection generation is not active"))); }
                                } else if let Err(error) = slot.commands.try_send(command) {
                                    let command = error.into_inner();
                                    if command.request.start() { finish(&engine, &command, Err(Error::new(16, "Connection command queue unavailable"))); }
                                }
                            } else if command.request.start() { finish(&engine, &command, Err(Error::new(8, "Unknown connection generation"))); }
                        }
                        Err(error) => { if command.request.start() { finish(&engine, &command, Err(error)); } }
                    }
                    continue;
                }
                if !command.request.start() { continue; }
                if command.operation == 30 {
                    let id = match std::str::from_utf8(&command.payload) {
                        Ok(id) if !id.is_empty() && id.len() <= 4096 => id.to_owned(),
                        _ => { finish(&engine, &command, Err(Error::new(16, "Invalid device identifier"))); continue; }
                    };
                    if state != 4 { finish(&engine, &command, Err(adapter_error(state))); continue; }
                    // A retry is allowed immediately after logical cancellation.
                    // Wait for the retired worker to join before reusing the OS device.
                    while connections.values().any(|slot| slot.device_id == id &&
                        (*slot.connect_cancel.borrow() || matches!(*slot.state.borrow(), ConnectionState::Closing | ConnectionState::Closed))) {
                        let mut cancelled = command.request.cancel.subscribe();
                        if *cancelled.borrow() { finish(&engine, &command, Err(Error::new(10, "Connect cancelled during cleanup"))); continue 'adapter; }
                        tokio::select! { biased;
                            _ = stop.changed() => { finish(&engine, &command, Err(Error::new(17, "Engine closing"))); break 'adapter; },
                            _ = cancelled.changed() => { finish(&engine, &command, Err(Error::new(10, "Connect cancelled during cleanup"))); continue 'adapter; },
                            _ = tokio::time::sleep_until(command.deadline) => { finish(&engine, &command, Err(Error::new(9, "Connect timed out during cleanup"))); continue 'adapter; },
                            done = connection_tasks.join_next() => {
                                match done {
                                    Some(Ok((generation, result))) => complete_connection(&engine, &mut connections, generation, result),
                                    Some(Err(error)) => { engine.fail_shutdown(crate::engine::task_error(error, "Connection task terminated")); engine.stop.send_replace(true); },
                                    None => { finish(&engine, &command, Err(Error::new(18, "Missing connection worker"))); continue 'adapter; },
                                }
                            }
                        }
                        if *stop.borrow() { finish(&engine, &command, Err(Error::new(17, "Engine closing"))); break 'adapter; }
                    }
                    if let Some(slot) = connections.values().find(|slot| slot.device_id == id) {
                        let code = if *slot.state.borrow() == ConnectionState::Connected {5} else {6};
                        finish(&engine, &command, Err(Error::new(code, "Device already has an owned connection generation"))); continue;
                    }
                    if connections.len() >= 128 { finish(&engine, &command, Err(Error::new(16, "Connection capacity exceeded"))); continue; }
                    let mut cancelled = command.request.cancel.subscribe();
                    let peripheral = if *cancelled.borrow() { Err(Error::new(10, "Connect cancelled")) } else {
                        let mut lookup = Box::pin(std::panic::AssertUnwindSafe(driver.peripheral(&id)).catch_unwind());
                        let result = tokio::select! { biased;
                            _ = stop.changed() => Err(Error::new(17, "Engine closing")),
                            _ = cancelled.changed() => Err(Error::new(10, "Connect cancelled")),
                            _ = tokio::time::sleep_until(command.deadline) => Err(Error::new(9, "Connect timed out")),
                            result = lookup.as_mut() => crate::callback_boundary::recover(result, || Err(Error::new(18, "Peripheral lookup panicked"))),
                        };
                        match crate::callback_boundary::invoke(
                            || { drop(lookup); Ok(()) },
                            || Error::new(18, "Peripheral lookup future disposal panicked"),
                        ) {
                            Ok(()) => result,
                            Err(error) => {
                                engine.fail_shutdown(error.clone());
                                engine.stop.send_replace(true);
                                result.and(Err(error))
                            }
                        }
                    };
                    match peripheral.and_then(|peripheral| next_id().map(|generation| (generation, peripheral))) {
                        Ok((generation, peripheral)) => {
                            let (slot, receiver, stopped, state) = connection::prepare(id, peripheral.clone(), command.request.cancel.subscribe());
                            connections.insert(generation, slot);
                            let engine = engine.clone();
                            connection_tasks.spawn(async move {
                                let result = crate::callback_boundary::recover(std::panic::AssertUnwindSafe(connection::run(engine, generation, peripheral, command, receiver, stopped, state)).catch_unwind().await, || Err(Error::new(18, "Connection worker panicked")));
                                (generation, result)
                            });
                        }
                        Err(error) => finish(&engine, &command, Err(error)),
                    }
                    continue;
                }
                let mut cancelled = command.request.cancel.subscribe();
                let starting = command.operation == 20;
                let result = if *cancelled.borrow() { Err(Error::new(10, "Request cancelled")) } else {
                    let mut operation = Box::pin(std::panic::AssertUnwindSafe(async {
                            match command.operation {
                                #[cfg(feature = "test-support")]
                                60 => driver.control(&command.payload),
                                10 if events.is_none() => {
                                    let (value, stream) = driver.initialize().await?;
                                    state = value; events = Some(stream); Ok(vec![state])
                                }
                                10 => Ok(vec![state]),
                                20 if state != 4 => Err(adapter_error(state)),
                                20 if !scanning => { uncertain = true; driver.start().await?; scanning = true; uncertain = false; Ok(Vec::new()) }
                                20 => Ok(Vec::new()),
                                21 if scanning || uncertain => { driver.stop().await?; scanning = false; uncertain = false; Ok(Vec::new()) }
                                21 => Ok(Vec::new()),
                                _ => Err(Error::new(11, "Unknown adapter operation")),
                            }
                        }).catch_unwind());
                    let result = tokio::select! {
                        biased;
                        _ = stop.changed() => Err(Error::new(17, "Engine closing")),
                        _ = cancelled.changed() => Err(Error::new(10, "Request cancelled")),
                        _ = tokio::time::sleep_until(command.deadline) => Err(Error::new(9, "Request timed out")),
                        result = operation.as_mut() => crate::callback_boundary::recover(result, || Err(Error::new(18, "Adapter operation panicked")))
                    };
                    match crate::callback_boundary::invoke(
                        || { drop(operation); Ok(()) },
                        || Error::new(18, "Adapter operation future disposal panicked"),
                    ) {
                        Ok(()) => result,
                        Err(error) => {
                            engine.fail_shutdown(error.clone());
                            engine.stop.send_replace(true);
                            result.and(Err(error))
                        }
                    }
                };
                if starting && result.is_err() && uncertain {
                    // A dropped OS future may already have enabled scanning. Stop before taking the next command.
                    if let Err(error) = cleanup(&mut driver).await {
                        engine.fail_shutdown(error.clone());
                        emit(&engine.sink, &engine.stop, &event(4, 0, Err(error)));
                        engine.stop.send_replace(true);
                    }
                    scanning = false; uncertain = false;
                }
                finish(&engine, &command, result);
            }
            done = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                if let Some(Ok((generation, result))) = done { complete_connection(&engine, &mut connections, generation, result); }
                else if let Some(Err(error)) = done { engine.fail_shutdown(crate::engine::task_error(error, "Connection task terminated")); engine.stop.send_replace(true); }
            }
            value = async { match events.as_mut() { Some(events) => crate::callback_boundary::recover(std::panic::AssertUnwindSafe(events.next()).catch_unwind().await, || Some(Err(Error::new(18, "Adapter event stream panicked")))), None => std::future::pending().await } } => {
                match value {
                    Some(Ok(value)) if !value.is_empty() => {
                        if value[0] == 7 {
                            // Central events have no generation. Verify the current physical state
                            // rather than relabeling a delayed disconnect as a new generation's event.
                            if let Ok(id) = std::str::from_utf8(&value[16..])
                                && let Some(slot) = connections.values().find(|slot| slot.device_id == id)
                                && *slot.state.borrow() == ConnectionState::Connected {
                                let disconnected = tokio::select! { biased;
                                    _ = stop.changed() => break,
                                    value = std::panic::AssertUnwindSafe(tokio::time::timeout(Duration::from_secs(5), slot.peripheral.probe_connected())).catch_unwind() => crate::callback_boundary::recover(value.map(|value| !matches!(value, Ok(Ok(true)))), || true),
                                };
                                if disconnected { slot.stop.send_replace(8); }
                            }
                            continue;
                        }
                        if value[0] == 3 {
                            state = value[16];
                            if state != 4 { for slot in connections.values() { slot.stop.send_replace(8); } }
                        }
                        if value[0] == 3 || scanning { emit(&engine.sink, &engine.stop, &value); }
                        if state != 4 && (scanning || uncertain) {
                            if let Err(error) = cleanup(&mut driver).await {
                                emit(&engine.sink, &engine.stop, &event(4, 0, Err(error.clone())));
                                engine.fail_shutdown(error);
                                uncertain = true; engine.stop.send_replace(true);
                            } else { uncertain = false; }
                            scanning = false;
                        }
                    }
                    Some(Err(error)) => {
                        let cleanup_error = if error.code != 18 && (scanning || uncertain) {
                            // A failed scan event retires this engine's lease before
                            // retry admission. Existing connection workers remain owned.
                            let result = cleanup(&mut driver).await;
                            scanning = false;
                            uncertain = result.is_err();
                            result.err()
                        } else { None };
                        if error.code == 18 { engine.stop.send_replace(true); }
                        emit(&engine.sink, &engine.stop, &event(4, 0, Err(error)));
                        if let Some(error) = cleanup_error {
                            engine.fail_shutdown(error.clone());
                            emit(&engine.sink, &engine.stop, &event(4, 0, Err(error)));
                            engine.stop.send_replace(true);
                        }
                    },
                    None => { emit(&engine.sink, &engine.stop, &event(3, 0, Ok(vec![1]))); engine.stop.send_replace(true); }
                    _ => {}
                }
            }
        }
    }
    commands.close();
    for slot in connections.values() {
        slot.stop.send_replace(17);
    }
    while let Some(done) = connection_tasks.join_next().await {
        match done {
            Ok((generation, result)) => {
                complete_connection(&engine, &mut connections, generation, result)
            }
            Err(error) => engine.fail_shutdown(crate::engine::task_error(
                error,
                "Connection task terminated",
            )),
        }
    }
    if (scanning || uncertain)
        && let Err(error) = cleanup(&mut driver).await
    {
        engine.fail_shutdown(error.clone());
        emit(&engine.sink, &engine.stop, &event(4, 0, Err(error)));
    }
    // Drop event registrations before shutting down their transport. BlueZ
    // stream Drop retires local callbacks and queues owned remote match removal.
    if let Err(error) = crate::callback_boundary::invoke(
        || {
            drop(events);
            Ok(())
        },
        || Error::new(18, "Adapter event stream cleanup panicked"),
    ) {
        engine.fail_shutdown(error);
    }
    // Join platform-owned tasks as well as our workers before acknowledging close.
    let mut shutdown = Box::pin(std::panic::AssertUnwindSafe(driver.shutdown()).catch_unwind());
    if let Err(error) = crate::callback_boundary::recover(shutdown.as_mut().await, || {
        Err(Error::new(18, "Platform shutdown panicked"))
    }) {
        engine.fail_shutdown(error);
    }
    if let Err(error) = crate::callback_boundary::invoke(
        || {
            drop(shutdown);
            Ok(())
        },
        || Error::new(18, "Platform shutdown future disposal panicked"),
    ) {
        engine.fail_shutdown(error);
    }
    // Driver drops here, before the engine shutdown acknowledgement.
}

fn complete_connection<P>(
    engine: &Engine,
    connections: &mut HashMap<u64, Slot<P>>,
    generation: u64,
    result: Result<(), Error>,
) {
    if let Err(error) = &result {
        engine.fail_shutdown(error.clone());
        engine.stop.send_replace(true);
    }
    if let Some(slot) = connections.remove(&generation) {
        for command in slot.closing {
            let result = if *command.request.cancel.borrow() {
                Err(Error::new(10, "Disconnect acknowledgement cancelled"))
            } else if tokio::time::Instant::now() >= command.deadline {
                Err(Error::new(9, "Disconnect acknowledgement timed out"))
            } else {
                result.clone().map(|_| Vec::new())
            };
            finish(engine, &command, result);
        }
    }
}

#[cfg(test)]
#[path = "scan/tests.rs"]
mod tests;
