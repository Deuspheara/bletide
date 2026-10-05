//! One engine owns its adapter stream and scan intent. Android shares physical
//! scanner ownership because upstream supplies one process-wide adapter.
use crate::{
    codec::{Error, Reader, event},
    connection::{self, ConnectionState, Device, Slot},
    engine::{Command, Engine, finish, next_id},
    event::Sink,
};
use btleplug::{
    api::{Central, CentralEvent, CentralState, Manager as _, Peripheral as _, ScanFilter},
    platform::{Adapter, Manager},
};
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
#[derive(Default)]
struct NativeDriver {
    manager: Option<Manager>,
    adapter: Option<Adapter>,
    #[cfg(target_os = "android")]
    scan_owner: Arc<()>,
    #[cfg(target_os = "android")]
    scan_generation: Arc<std::sync::atomic::AtomicU64>,
}
#[cfg(target_os = "android")]
impl Drop for NativeDriver {
    fn drop(&mut self) {
        if let Some(adapter) = &self.adapter {
            adapter.release_discovery_owner(&self.scan_owner);
        }
    }
}
impl Driver for NativeDriver {
    type Device = btleplug::platform::Peripheral;
    async fn shutdown(&mut self) -> Result<(), Error> {
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        if let Some(adapter) = &self.adapter {
            adapter.shutdown().await.map_err(Error::from)?;
        }
        #[cfg(target_os = "linux")]
        if let Some(manager) = &self.manager {
            manager.shutdown().await.map_err(Error::from)?;
        }
        #[cfg(target_os = "android")]
        if let Some(adapter) = &self.adapter {
            adapter.release_discovery_owner(&self.scan_owner);
        }
        self.adapter = None;
        self.manager = None;
        Ok(())
    }
    async fn initialize(&mut self) -> Result<(u8, Events), Error> {
        if self.adapter.is_some() {
            return Err(Error::new(
                16,
                "Adapter initialization already owned by this engine",
            ));
        }
        #[cfg(target_os = "android")]
        crate::android::adapter_state()?;
        let manager = Manager::new().await.map_err(initialization_error)?;
        // Retain the transport before adapter enumeration: cancellation or
        // failure during that await must still reach explicit shutdown.
        self.manager = Some(manager);
        let adapter = self
            .manager
            .as_ref()
            .ok_or_else(|| Error::new(18, "Missing adapter manager"))?
            .adapters()
            .await
            .map_err(initialization_error)?
            .into_iter()
            .next()
            .ok_or_else(|| Error::new(1, "No Bluetooth adapter available"))?;
        // Keep ownership before any further await so failed/cancelled bootstrap
        // can still execute the adapter's explicit shutdown path.
        #[cfg(target_os = "android")]
        adapter.retain_discovery_owner(&self.scan_owner);
        self.adapter = Some(adapter.clone());
        let events = adapter.events().await.map_err(initialization_error)?;
        #[cfg(not(target_os = "android"))]
        let state = adapter
            .adapter_state()
            .await
            .map_err(initialization_error)?;
        #[cfg(target_os = "android")]
        let scan_generation = self.scan_generation.clone();
        let source = adapter.clone();
        let events = events.then(move |value| {
            let adapter = source.clone();
            #[cfg(target_os = "android")]
            let scan_generation = scan_generation.clone();
            async move {
                match value {
                    CentralEvent::AdapterError { message } => Err(Error::new(18, message)),
                    #[cfg(target_os = "android")]
                    CentralEvent::ScanError {
                        generation,
                        error_code,
                    } => {
                        if scan_generation.load(std::sync::atomic::Ordering::Acquire) != generation
                        {
                            return Ok(Vec::new());
                        }
                        if adapter.scan_state().map_err(Error::from)?.0 == generation {
                            crate::shared_scan::process_scanner().fail_attempt(generation);
                        }
                        Err(android_scan_error(error_code))
                    }
                    CentralEvent::StateUpdate(state) => {
                        Ok(event(3, 0, Ok(vec![state_byte(state)])))
                    }
                    CentralEvent::DeviceDiscovered(id)
                    | CentralEvent::DeviceUpdated(id)
                    | CentralEvent::ManufacturerDataAdvertisement { id, .. }
                    | CentralEvent::ServiceDataAdvertisement { id, .. }
                    | CentralEvent::ServicesAdvertisement { id, .. }
                    | CentralEvent::RssiUpdate { id, .. } => {
                        let peripheral = adapter
                            .peripheral(&id)
                            .await
                            .map_err(initialization_error)?;
                        match peripheral
                            .properties()
                            .await
                            .map_err(initialization_error)?
                        {
                            Some(properties) => {
                                let address = if cfg!(any(target_os = "macos", target_os = "ios")) {
                                    None
                                } else {
                                    Some(properties.address.to_string())
                                };
                                Ok(event(
                                    4,
                                    0,
                                    Ok(advertisement(
                                        &id.to_string(),
                                        &properties,
                                        address.as_deref(),
                                    )?),
                                ))
                            }
                            None => Ok(Vec::new()),
                        }
                    }
                    CentralEvent::DeviceDisconnected(id) => {
                        Ok(event(7, 0, Ok(id.to_string().into_bytes())))
                    }
                    _ => Ok(Vec::new()),
                }
            }
        });
        #[cfg(target_os = "android")]
        {
            let (state, state_events) = crate::android::state_events();
            Ok((
                state,
                Box::pin(futures_util::stream::select(
                    Box::pin(events),
                    Box::pin(state_events),
                )),
            ))
        }
        #[cfg(not(target_os = "android"))]
        Ok((state_byte(state), Box::pin(events)))
    }
    async fn start(&mut self) -> Result<(), Error> {
        let adapter = self
            .adapter
            .as_ref()
            .ok_or_else(|| Error::new(1, "Adapter not initialized"))?;
        #[cfg(target_os = "android")]
        {
            crate::shared_scan::process_scanner()
                .start(
                    &self.scan_owner,
                    async {
                        let state = crate::android::adapter_state()?;
                        if state != 4 {
                            return Err(adapter_error(state));
                        }
                        adapter
                            .start_scan(ScanFilter::default())
                            .await
                            .map_err(Error::from)
                    },
                    async { adapter.stop_scan().await.map_err(Error::from) },
                )
                .await?;
            let (generation, failure) = adapter.scan_state().map_err(Error::from)?;
            self.scan_generation
                .store(generation, std::sync::atomic::Ordering::Release);
            if failure != 0 {
                // Startup reports this cached cause through its command ACK;
                // discard the queued callback rather than reporting it twice.
                self.scan_generation
                    .store(0, std::sync::atomic::Ordering::Release);
                crate::shared_scan::process_scanner().fail_attempt(generation);
                return Err(android_scan_error(failure));
            }
            Ok(())
        }
        #[cfg(not(target_os = "android"))]
        adapter
            .start_scan(ScanFilter::default())
            .await
            .map_err(Error::from)
    }
    async fn peripheral(&mut self, id: &str) -> Result<btleplug::platform::Peripheral, Error> {
        let adapter = self
            .adapter
            .as_ref()
            .ok_or_else(|| Error::new(1, "Adapter not initialized"))?;
        if let Some(peripheral) = adapter
            .peripherals()
            .await
            .map_err(Error::from)?
            .into_iter()
            .find(|p| p.id().to_string() == id)
        {
            return Ok(peripheral);
        }
        // A disconnect can evict the adapter cache. Retrieve through the OS;
        // never start an implicit scan or reuse a retired generation's GATT state.
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        {
            let uuid = uuid::Uuid::parse_str(id)
                .map_err(|_| Error::new(4, "Invalid CoreBluetooth device identifier"))?;
            return adapter.peripheral(&uuid.into()).await.map_err(Error::from);
        }
        #[cfg(any(target_os = "android", target_os = "windows"))]
        {
            let address = id
                .parse::<btleplug::api::BDAddr>()
                .map_err(|_| Error::new(4, "Invalid device address"))?;
            return adapter
                .add_peripheral(&address.into())
                .await
                .map_err(Error::from);
        }
        #[cfg(target_os = "linux")]
        let peripherals = adapter
            .retrieve_peripherals(Default::default())
            .await
            .map_err(Error::from)?;
        #[cfg(target_os = "linux")]
        peripherals
            .into_iter()
            .find(|p| p.id().to_string() == id)
            .ok_or_else(|| Error::new(4, "Device is not known to the operating system"))
    }
    async fn stop(&mut self) -> Result<(), Error> {
        if let Some(adapter) = &self.adapter {
            #[cfg(target_os = "android")]
            {
                self.scan_generation
                    .store(0, std::sync::atomic::Ordering::Release);
                return crate::shared_scan::process_scanner()
                    .stop(&self.scan_owner, async {
                        adapter.stop_scan().await.map_err(Error::from)
                    })
                    .await;
            }
            #[cfg(not(target_os = "android"))]
            adapter.stop_scan().await.map_err(Error::from)?;
        }
        Ok(())
    }
}
// Generic transport failures during adapter setup are availability failures,
// not GATT failures. Keep precise upstream classifications and native messages.
fn initialization_error(error: btleplug::Error) -> Error {
    let mut error = Error::from(error);
    if error.code == 15 {
        error.code = 1;
    }
    error
}

#[cfg(any(target_os = "android", test))]
fn android_scan_error(native_code: i32) -> Error {
    // ScanCallback failures are not GATT failures or fatal engine panics.
    // Keep unfamiliar OS codes recoverable without inventing their meaning.
    let code = match native_code {
        1 => 16, // SCAN_FAILED_ALREADY_STARTED
        4 => 11, // SCAN_FAILED_FEATURE_UNSUPPORTED
        _ => 19,
    };
    Error::new(code, format!("Android scan failed: code {native_code}"))
        .with_native_code(native_code.to_string())
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

fn state_byte(state: CentralState) -> u8 {
    match state {
        CentralState::PoweredOn => 4,
        CentralState::PoweredOff => 2,
        CentralState::Unknown => 1,
    }
}

// Length prefixes are bounded by the same 1 MiB transport limit as commands.
fn bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), Error> {
    let len =
        u32::try_from(value.len()).map_err(|_| Error::new(18, "Advertisement field too large"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(value);
    Ok(())
}
fn optional_string(out: &mut Vec<u8>, value: Option<&str>) -> Result<(), Error> {
    out.push(u8::from(value.is_some()));
    if let Some(value) = value {
        bytes(out, value.as_bytes())?;
    }
    Ok(())
}
fn advertisement(
    id: &str,
    p: &btleplug::api::PeripheralProperties,
    address: Option<&str>,
) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    bytes(&mut out, id.as_bytes())?;
    optional_string(
        &mut out,
        p.local_name.as_deref().or(p.advertisement_name.as_deref()),
    )?;
    out.push(u8::from(p.rssi.is_some()));
    if let Some(rssi) = p.rssi {
        out.extend_from_slice(&rssi.to_le_bytes());
    }
    optional_string(&mut out, address)?;
    out.extend_from_slice(&(p.services.len() as u32).to_le_bytes());
    for uuid in &p.services {
        out.extend_from_slice(uuid.as_bytes());
    }
    out.extend_from_slice(&(p.manufacturer_data.len() as u32).to_le_bytes());
    for (id, value) in &p.manufacturer_data {
        out.extend_from_slice(&id.to_le_bytes());
        bytes(&mut out, value)?;
    }
    out.extend_from_slice(&(p.service_data.len() as u32).to_le_bytes());
    for (uuid, value) in &p.service_data {
        out.extend_from_slice(uuid.as_bytes());
        bytes(&mut out, value)?;
    }
    if out.len() > 1_048_576 {
        return Err(Error::new(18, "Advertisement exceeds transport limit"));
    }
    Ok(out)
}

pub(crate) async fn run(
    engine: Arc<Engine>,
    commands: mpsc::Receiver<Command>,
    stop: watch::Receiver<bool>,
) {
    worker(NativeDriver::default(), engine, commands, stop).await;
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
mod tests {
    use super::*;
    use crate::{
        engine::{testing_command, testing_engine},
        event::EventSink,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn android_scan_errors_preserve_os_codes_without_gatt_or_fatal_classification() {
        for (native, portable) in [
            (1, 16),
            (2, 19),
            (3, 19),
            (4, 11),
            (5, 19),
            (6, 19),
            (99, 19),
            (-1, 19),
        ] {
            let error = android_scan_error(native);
            assert_eq!(error.code, portable);
            assert_eq!(
                error.native_code.as_deref(),
                Some(native.to_string().as_str())
            );
            assert_eq!(error.message, format!("Android scan failed: code {native}"));
        }
    }

    #[derive(Clone)]
    struct StreamDropDevice {
        observations: mpsc::UnboundedSender<&'static str>,
        drops: Arc<AtomicUsize>,
    }
    struct DropPanickingStream(StreamDropDevice);
    impl Stream for DropPanickingStream {
        type Item = Result<btleplug::api::ValueNotification, Error>;
        fn poll_next(
            self: Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            std::task::Poll::Pending
        }
    }
    struct StreamPanicPayload(Arc<AtomicUsize>);
    impl Drop for StreamPanicPayload {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("controlled stream panic payload destructor");
        }
    }
    impl Drop for DropPanickingStream {
        fn drop(&mut self) {
            self.0.observations.send("stream-drop").unwrap();
            self.0.drops.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(StreamPanicPayload(self.0.drops.clone()));
        }
    }
    impl Device for StreamDropDevice {
        type Driver = Self;
        fn driver(self) -> Self {
            self
        }
        async fn probe_connected(&self) -> Result<bool, Error> {
            Ok(true)
        }
    }
    impl connection::Driver for StreamDropDevice {
        async fn connect(&mut self) -> Result<(), Error> {
            Ok(())
        }
        async fn disconnect(&mut self) -> Result<(), Error> {
            self.observations.send("disconnect").unwrap();
            Ok(())
        }
        async fn notifications(&mut self) -> Result<connection::Notifications, Error> {
            Ok(Box::pin(DropPanickingStream(self.clone())))
        }
        async fn operate(&mut self, _: u32, _: &[u8]) -> Result<Vec<u8>, Error> {
            Ok(Vec::new())
        }
    }
    struct StreamDropAdapter(StreamDropDevice);
    impl Driver for StreamDropAdapter {
        type Device = StreamDropDevice;
        async fn initialize(&mut self) -> Result<(u8, Events), Error> {
            Ok((4, Box::pin(futures_util::stream::pending())))
        }
        async fn start(&mut self) -> Result<(), Error> {
            Ok(())
        }
        async fn stop(&mut self) -> Result<(), Error> {
            Ok(())
        }
        async fn peripheral(&mut self, _: &str) -> Result<Self::Device, Error> {
            Ok(self.0.clone())
        }
        async fn shutdown(&mut self) -> Result<(), Error> {
            self.0.observations.send("adapter-shutdown").unwrap();
            Ok(())
        }
    }

    #[tokio::test]
    async fn notification_stream_drop_panic_is_joined_before_disconnect_ack_and_adapter_shutdown() {
        for _ in 0..100 {
            let (sink, mut events) = mpsc::unbounded_channel();
            let (engine, stopped) = testing_engine(Arc::new(ChannelSink(sink)));
            let resources = engine.resources.clone();
            let (commands, receiver) = mpsc::channel(8);
            let (observations, mut observed) = mpsc::unbounded_channel();
            let drops = Arc::new(AtomicUsize::new(0));
            let task = tokio::spawn(worker(
                StreamDropAdapter(StreamDropDevice {
                    observations,
                    drops: drops.clone(),
                }),
                engine.clone(),
                receiver,
                stopped,
            ));
            let deadline = || Instant::now() + Duration::from_secs(10);
            commands
                .send(testing_command(10, deadline()))
                .await
                .unwrap();
            assert_eq!(events.recv().await.unwrap()[16], 4);
            let mut connect = testing_command(30, deadline());
            connect.payload = b"controlled-stream-device".to_vec();
            commands.send(connect).await.unwrap();
            let connected = events.recv().await.unwrap();
            let generation = u64::from_le_bytes(connected[16..24].try_into().unwrap());
            let mut subscribe = testing_command(44, deadline());
            subscribe.payload = [
                generation.to_le_bytes().as_slice(),
                uuid::Uuid::from_u128(1).as_bytes().as_slice(),
                uuid::Uuid::from_u128(2).as_bytes().as_slice(),
            ]
            .concat();
            commands.send(subscribe).await.unwrap();
            assert_eq!(&events.recv().await.unwrap()[12..16], &0u32.to_le_bytes());
            assert_eq!(resources.snapshot(), [1, 1, 1, 1]);
            let mut disconnect = testing_command(31, deadline());
            disconnect.payload = generation.to_le_bytes().to_vec();
            commands.send(disconnect).await.unwrap();
            let retired = tokio::time::timeout(Duration::from_secs(2), events.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(&retired[..4], &5u32.to_le_bytes());
            assert_eq!(&retired[4..12], &generation.to_le_bytes());
            let ack = tokio::time::timeout(Duration::from_secs(2), events.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(&ack[..4], &1u32.to_le_bytes());
            assert_eq!(&ack[12..16], &18u32.to_le_bytes());
            assert_eq!(&ack[16..], b"Connection worker panicked");
            assert_eq!(&resources.snapshot()[1..], &[0, 0, 0]);
            tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
            assert!(*engine.stop.borrow());
            assert_eq!(resources.snapshot(), [0; 4]);
            assert_eq!(observed.recv().await.unwrap(), "disconnect");
            assert_eq!(observed.recv().await.unwrap(), "stream-drop");
            assert_eq!(observed.recv().await.unwrap(), "adapter-shutdown");
            assert!(observed.try_recv().is_err());
            assert_eq!(drops.load(Ordering::SeqCst), 2);
            assert_eq!(Arc::strong_count(&drops), 1);
        }
    }
    use tokio::{sync::oneshot, time::Instant};
    struct ChannelSink(mpsc::UnboundedSender<Vec<u8>>);
    impl EventSink for ChannelSink {
        fn send(&self, value: &[u8]) -> bool {
            self.0.send(value.to_vec()).is_ok()
        }
    }
    #[tokio::test]
    async fn fatal_adapter_event_reports_cause_and_joins_an_idle_worker() {
        let (sink, mut results) = mpsc::unbounded_channel();
        let (engine, stopped) = testing_engine(Arc::new(ChannelSink(sink)));
        let (commands, receiver) = mpsc::channel(8);
        let (observations, mut observed) = mpsc::unbounded_channel();
        let (events, rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(worker(
            FakeDriver {
                observations,
                startup: None,
                stops: Arc::new(AtomicUsize::new(0)),
                events: Some(rx),
            },
            engine.clone(),
            receiver,
            stopped,
        ));
        commands
            .send(testing_command(
                10,
                Instant::now() + Duration::from_secs(10),
            ))
            .await
            .unwrap();
        assert_eq!(results.recv().await.unwrap()[16], 4);
        let cause = "CoreBluetooth callback panicked: injected_callback";
        events.send(Err(Error::new(18, cause))).unwrap();
        let result = results.recv().await.unwrap();
        assert_eq!(u32::from_le_bytes(result[..4].try_into().unwrap()), 4);
        assert_eq!(u32::from_le_bytes(result[12..16].try_into().unwrap()), 18);
        assert_eq!(&result[16..], cause.as_bytes());
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert!(*engine.stop.borrow());
        assert!(
            observed.try_recv().is_err(),
            "idle failure initiated a physical scan"
        );
        assert!(
            events.send(Ok(Vec::new())).is_err(),
            "adapter stream remains retained"
        );
    }

    #[derive(Clone)]
    struct RetryDevice {
        attempts: Arc<AtomicUsize>,
        observations: mpsc::UnboundedSender<&'static str>,
        cleanup: Arc<tokio::sync::Notify>,
    }
    impl Device for RetryDevice {
        type Driver = Self;
        fn driver(self) -> Self {
            self
        }
        async fn probe_connected(&self) -> Result<bool, Error> {
            Ok(true)
        }
    }
    impl connection::Driver for RetryDevice {
        async fn connect(&mut self) -> Result<(), Error> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                self.observations.send("connecting").unwrap();
                std::future::pending().await
            } else {
                self.observations.send("retry-connected").unwrap();
                Ok(())
            }
        }
        async fn disconnect(&mut self) -> Result<(), Error> {
            if self.attempts.load(Ordering::SeqCst) == 1 {
                self.observations.send("cleanup").unwrap();
                self.cleanup.notified().await;
            }
            Ok(())
        }
        async fn notifications(&mut self) -> Result<connection::Notifications, Error> {
            Ok(Box::pin(futures_util::stream::pending()))
        }
        async fn operate(&mut self, _: u32, _: &[u8]) -> Result<Vec<u8>, Error> {
            Ok(Vec::new())
        }
    }
    struct RetryAdapter(RetryDevice);
    impl Driver for RetryAdapter {
        type Device = RetryDevice;
        async fn initialize(&mut self) -> Result<(u8, Events), Error> {
            Ok((4, Box::pin(futures_util::stream::pending())))
        }
        async fn start(&mut self) -> Result<(), Error> {
            Ok(())
        }
        async fn stop(&mut self) -> Result<(), Error> {
            Ok(())
        }
        async fn peripheral(&mut self, _: &str) -> Result<Self::Device, Error> {
            Ok(self.0.clone())
        }
    }
    #[tokio::test]
    async fn immediate_retry_waits_for_cancelled_connection_cleanup_and_worker_join() {
        let (sink, mut results) = mpsc::unbounded_channel();
        let (engine, stopped) = testing_engine(Arc::new(ChannelSink(sink)));
        let (commands, receiver) = mpsc::channel(8);
        let (observations, mut observed) = mpsc::unbounded_channel();
        let cleanup = Arc::new(tokio::sync::Notify::new());
        let attempts = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn(worker(
            RetryAdapter(RetryDevice {
                attempts: attempts.clone(),
                observations,
                cleanup: cleanup.clone(),
            }),
            engine.clone(),
            receiver,
            stopped,
        ));
        commands
            .send(testing_command(
                10,
                Instant::now() + Duration::from_secs(10),
            ))
            .await
            .unwrap();
        results.recv().await.unwrap();
        let mut first = testing_command(30, Instant::now() + Duration::from_secs(10));
        first.payload = b"retry-device".to_vec();
        let cancel = first.request.cancel.clone();
        commands.send(first).await.unwrap();
        assert_eq!(observed.recv().await.unwrap(), "connecting");
        cancel.send_replace(true);
        assert_eq!(observed.recv().await.unwrap(), "cleanup");
        let mut retry = testing_command(30, Instant::now() + Duration::from_secs(10));
        retry.payload = b"retry-device".to_vec();
        commands.send(retry).await.unwrap();
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(results.try_recv().is_err());
        cleanup.notify_one();
        let cancelled = results.recv().await.unwrap();
        assert_eq!(
            u32::from_le_bytes(cancelled[12..16].try_into().unwrap()),
            10
        );
        assert_eq!(observed.recv().await.unwrap(), "retry-connected");
        let connected = results.recv().await.unwrap();
        assert_eq!(u32::from_le_bytes(connected[12..16].try_into().unwrap()), 0);
        assert_eq!(connected.len(), 24);
        engine.stop.send_replace(true);
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
    struct FakeDriver {
        observations: mpsc::UnboundedSender<&'static str>,
        startup: Option<oneshot::Receiver<()>>,
        stops: Arc<AtomicUsize>,
        events: Option<mpsc::UnboundedReceiver<Result<Vec<u8>, Error>>>,
    }
    struct StopDropPanic(Arc<AtomicUsize>);
    impl Drop for StopDropPanic {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            std::panic::panic_any(StreamPanicPayload(self.0.clone()));
        }
    }
    struct DropPanickingStop<F> {
        future: Pin<Box<F>>,
        _guard: Option<StopDropPanic>,
    }
    impl<F: Future> Future for DropPanickingStop<F> {
        type Output = F::Output;
        fn poll(
            self: Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            self.get_mut().future.as_mut().poll(cx)
        }
    }
    struct ShutdownDriver {
        stop_drop: Option<Arc<AtomicUsize>>,
        stop_result: Result<(), Error>,
        stop_pending: bool,
        events_panic: bool,
        entered: Option<oneshot::Sender<()>>,
        release: Option<oneshot::Receiver<()>>,
        events_dropped: Arc<AtomicUsize>,
        initialized: Option<oneshot::Sender<()>>,
    }
    impl Driver for ShutdownDriver {
        type Device = btleplug::platform::Peripheral;
        async fn initialize(&mut self) -> Result<(u8, Events), Error> {
            struct Registration(Arc<AtomicUsize>, bool);
            impl Drop for Registration {
                fn drop(&mut self) {
                    self.0.fetch_add(1, Ordering::SeqCst);
                    if self.1 {
                        std::panic::panic_any(StreamPanicPayload(self.0.clone()));
                    }
                }
            }
            let registration = Registration(self.events_dropped.clone(), self.events_panic);
            let events = futures_util::stream::unfold(registration, |registration| async move {
                std::future::pending::<()>().await;
                Some((Ok(Vec::new()), registration))
            });
            self.initialized.take().unwrap().send(()).unwrap();
            Ok((4, Box::pin(events)))
        }
        async fn start(&mut self) -> Result<(), Error> {
            Ok(())
        }
        fn stop(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
            let pending = self.stop_pending;
            let result = self.stop_result.clone();
            DropPanickingStop {
                future: Box::pin(async move {
                    if pending {
                        std::future::pending::<()>().await;
                    }
                    result
                }),
                _guard: self.stop_drop.take().map(StopDropPanic),
            }
        }
        async fn shutdown(&mut self) -> Result<(), Error> {
            assert_eq!(
                self.events_dropped.load(Ordering::SeqCst),
                if self.events_panic { 2 } else { 1 },
                "event registrations must retire before transport shutdown"
            );
            self.entered.take().unwrap().send(()).unwrap();
            self.release.take().unwrap().await.unwrap();
            Ok(())
        }
    }
    #[tokio::test]
    async fn adapter_worker_retains_ownership_until_platform_shutdown_joins() {
        let (output, _results) = mpsc::unbounded_channel();
        let (engine, stopped) = testing_engine(Arc::new(ChannelSink(output)));
        let (commands, receiver) = mpsc::channel(8);
        let (entered, entering) = oneshot::channel();
        let (release, pending) = oneshot::channel();
        let (initialized, ready) = oneshot::channel();
        let events_dropped = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn(worker(
            ShutdownDriver {
                stop_drop: None,
                stop_result: Ok(()),
                stop_pending: false,
                events_panic: false,
                entered: Some(entered),
                release: Some(pending),
                events_dropped: events_dropped.clone(),
                initialized: Some(initialized),
            },
            engine.clone(),
            receiver,
            stopped,
        ));
        commands
            .send(testing_command(
                10,
                Instant::now() + Duration::from_secs(10),
            ))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), ready)
            .await
            .unwrap()
            .unwrap();
        engine.stop.send_replace(true);
        tokio::time::timeout(Duration::from_secs(2), entering)
            .await
            .unwrap()
            .unwrap();
        assert!(!task.is_finished());
        assert_eq!(engine.resources.snapshot(), [1, 0, 0, 0]);
        release.send(()).unwrap();
        task.await.unwrap();
        assert_eq!(engine.resources.snapshot(), [0; 4]);
        assert_eq!(events_dropped.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn adapter_event_stream_drop_panic_still_joins_platform_shutdown() {
        for earlier_failure in [false, true] {
            let (output, mut results) = mpsc::unbounded_channel();
            let (engine, stopped) = testing_engine(Arc::new(ChannelSink(output)));
            let (commands, receiver) = mpsc::channel(8);
            let (entered, entering) = oneshot::channel();
            let (release, pending) = oneshot::channel();
            let (initialized, ready) = oneshot::channel();
            let drops = Arc::new(AtomicUsize::new(0));
            let task = tokio::spawn(worker(
                ShutdownDriver {
                    stop_drop: None,
                    stop_result: Ok(()),
                    stop_pending: false,
                    events_panic: true,
                    entered: Some(entered),
                    release: Some(pending),
                    events_dropped: drops.clone(),
                    initialized: Some(initialized),
                },
                engine.clone(),
                receiver,
                stopped,
            ));
            commands
                .send(testing_command(
                    10,
                    Instant::now() + Duration::from_secs(10),
                ))
                .await
                .unwrap();
            ready.await.unwrap();
            let result = results.recv().await.unwrap();
            assert_eq!(u32::from_le_bytes(result[12..16].try_into().unwrap()), 0);
            let expected = if earlier_failure {
                let error = Error::new(15, "Earlier scanner failure");
                engine.fail_shutdown(error.clone());
                error
            } else {
                Error::new(18, "Adapter event stream cleanup panicked")
            };
            engine.stop.send_replace(true);
            let entered = tokio::time::timeout(Duration::from_secs(2), entering)
                .await
                .unwrap();
            if entered.is_err() {
                if let Err(error) = task.await {
                    let _ = crate::engine::task_error(error, "Adapter worker terminated");
                }
                panic!("Stream destructor skipped platform shutdown");
            }
            assert!(!task.is_finished());
            assert_eq!(engine.resources.snapshot(), [1, 0, 0, 0]);
            assert_eq!(drops.load(Ordering::SeqCst), 2);
            release.send(()).unwrap();
            task.await.unwrap();
            assert_eq!(engine.testing_shutdown_failure(), Some(expected));
            assert_eq!(engine.resources.snapshot(), [0; 4]);
            assert_eq!(Arc::strong_count(&drops), 1);
        }
    }
    #[tokio::test(start_paused = true)]
    async fn scanner_stop_future_disposal_preserves_native_failure_and_timeout() {
        for outcome in 0..3 {
            let drops = Arc::new(AtomicUsize::new(0));
            let expected = match outcome {
                0 => Error::new(18, "Scanner cleanup future disposal panicked"),
                1 => Error::new(15, "Native scanner stop failed"),
                _ => Error::new(9, "Scanner cleanup timed out"),
            };
            let mut driver = ShutdownDriver {
                stop_drop: Some(drops.clone()),
                stop_result: if outcome == 1 {
                    Err(expected.clone())
                } else {
                    Ok(())
                },
                stop_pending: outcome == 2,
                events_panic: false,
                entered: None,
                release: None,
                initialized: None,
                events_dropped: Arc::new(AtomicUsize::new(0)),
            };
            // Recover here so the failing-before test safely disposes its payload.
            let result = crate::callback_boundary::recover(
                std::panic::AssertUnwindSafe(cleanup(&mut driver))
                    .catch_unwind()
                    .await,
                || Err(Error::new(18, "Scanner cleanup escaped its boundary")),
            );
            assert_eq!(result, Err(expected));
            assert_eq!(drops.load(Ordering::SeqCst), 2);
            assert_eq!(Arc::strong_count(&drops), 1);
        }
    }
    #[tokio::test]
    async fn scanner_stop_destructor_panic_still_joins_platform_shutdown() {
        for earlier_failure in [false, true] {
            let (output, mut results) = mpsc::unbounded_channel();
            let (engine, stopped) = testing_engine(Arc::new(ChannelSink(output)));
            let (commands, receiver) = mpsc::channel(8);
            let (entered, entering) = oneshot::channel();
            let (release, pending) = oneshot::channel();
            let (initialized, ready) = oneshot::channel();
            let drops = Arc::new(AtomicUsize::new(0));
            let events_dropped = Arc::new(AtomicUsize::new(0));
            let task = tokio::spawn(worker(
                ShutdownDriver {
                    stop_drop: Some(drops.clone()),
                    stop_result: Ok(()),
                    stop_pending: false,
                    events_panic: false,
                    entered: Some(entered),
                    release: Some(pending),
                    events_dropped: events_dropped.clone(),
                    initialized: Some(initialized),
                },
                engine.clone(),
                receiver,
                stopped,
            ));
            commands
                .send(testing_command(
                    10,
                    Instant::now() + Duration::from_secs(10),
                ))
                .await
                .unwrap();
            ready.await.unwrap();
            for operation in [None, Some(20)] {
                if let Some(operation) = operation {
                    commands
                        .send(testing_command(
                            operation,
                            Instant::now() + Duration::from_secs(10),
                        ))
                        .await
                        .unwrap();
                }
                let result = results.recv().await.unwrap();
                assert_eq!(u32::from_le_bytes(result[12..16].try_into().unwrap()), 0);
            }
            let expected = if earlier_failure {
                let error = Error::new(15, "Earlier scanner failure");
                engine.fail_shutdown(error.clone());
                error
            } else {
                Error::new(18, "Scanner cleanup future disposal panicked")
            };
            engine.stop.send_replace(true);
            let entered = tokio::time::timeout(Duration::from_secs(2), entering)
                .await
                .unwrap();
            if entered.is_err() {
                if let Err(error) = task.await {
                    let _ = crate::engine::task_error(error, "Adapter worker terminated");
                }
                panic!("Scanner future destructor skipped platform shutdown");
            }
            assert!(!task.is_finished());
            assert_eq!(engine.resources.snapshot(), [1, 0, 0, 0]);
            assert_eq!(drops.load(Ordering::SeqCst), 2);
            assert_eq!(events_dropped.load(Ordering::SeqCst), 1);
            release.send(()).unwrap();
            task.await.unwrap();
            assert_eq!(engine.testing_shutdown_failure(), Some(expected));
            assert_eq!(engine.resources.snapshot(), [0; 4]);
            assert_eq!(Arc::strong_count(&drops), 1);
        }
    }
    struct OperationDropDriver {
        shutdown_result: Result<(), Error>,
        operation: u32,
        drops: Option<Arc<AtomicUsize>>,
        entered: mpsc::UnboundedSender<u32>,
        shutdown_entered: Option<oneshot::Sender<()>>,
        shutdown_release: Option<oneshot::Receiver<()>>,
        stops: Arc<AtomicUsize>,
    }
    impl Driver for OperationDropDriver {
        type Device = btleplug::platform::Peripheral;
        fn initialize(&mut self) -> impl Future<Output = Result<(u8, Events), Error>> + Send {
            let guard = if self.operation == 10 {
                self.drops.take().map(StopDropPanic)
            } else {
                None
            };
            let entered = self.entered.clone();
            let pending = guard.is_some();
            DropPanickingStop {
                future: Box::pin(async move {
                    entered.send(10).unwrap();
                    if pending {
                        std::future::pending::<()>().await;
                    }
                    Ok((4, Box::pin(futures_util::stream::pending()) as Events))
                }),
                _guard: guard,
            }
        }
        fn start(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
            let guard = if self.operation == 20 {
                self.drops.take().map(StopDropPanic)
            } else {
                None
            };
            let entered = self.entered.clone();
            let pending = guard.is_some();
            DropPanickingStop {
                future: Box::pin(async move {
                    entered.send(20).unwrap();
                    if pending {
                        std::future::pending::<()>().await;
                    }
                    Ok(())
                }),
                _guard: guard,
            }
        }
        fn stop(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
            self.stops.fetch_add(1, Ordering::SeqCst);
            let guard = if self.operation == 21 {
                self.drops.take().map(StopDropPanic)
            } else {
                None
            };
            let entered = self.entered.clone();
            let pending = guard.is_some();
            DropPanickingStop {
                future: Box::pin(async move {
                    entered.send(21).unwrap();
                    if pending {
                        std::future::pending::<()>().await;
                    }
                    Ok(())
                }),
                _guard: guard,
            }
        }
        fn peripheral(
            &mut self,
            id: &str,
        ) -> impl Future<Output = Result<Self::Device, Error>> + Send {
            assert_eq!(id, "lookup-device");
            let guard = self.drops.take().map(StopDropPanic);
            let entered = self.entered.clone();
            DropPanickingStop {
                future: Box::pin(async move {
                    entered.send(30).unwrap();
                    std::future::pending().await
                }),
                _guard: guard,
            }
        }
        fn shutdown(&mut self) -> impl Future<Output = Result<(), Error>> + Send {
            let guard = if self.operation == 0 {
                self.drops.take().map(StopDropPanic)
            } else {
                None
            };
            let entered = self.shutdown_entered.take().unwrap();
            let release = self.shutdown_release.take().unwrap();
            let result = self.shutdown_result.clone();
            DropPanickingStop {
                future: Box::pin(async move {
                    entered.send(()).unwrap();
                    release.await.unwrap();
                    result
                }),
                _guard: guard,
            }
        }
    }
    #[tokio::test(start_paused = true)]
    async fn adapter_operation_cancellation_disposes_future_before_shutdown_and_ack() {
        for operation in [10, 20, 21, 30] {
            for outcome in [10, 9, 17] {
                let (output, mut results) = mpsc::unbounded_channel();
                let (engine, stopped) = testing_engine(Arc::new(ChannelSink(output)));
                let (commands, receiver) = mpsc::channel(8);
                let (entered, mut observations) = mpsc::unbounded_channel();
                let (shutdown_entered, shutdown_wait) = oneshot::channel();
                let (shutdown_release, shutdown_pending) = oneshot::channel();
                let drops = Arc::new(AtomicUsize::new(0));
                let stops = Arc::new(AtomicUsize::new(0));
                let task = tokio::spawn(worker(
                    OperationDropDriver {
                        shutdown_result: Ok(()),
                        operation,
                        drops: Some(drops.clone()),
                        entered,
                        shutdown_entered: Some(shutdown_entered),
                        shutdown_release: Some(shutdown_pending),
                        stops: stops.clone(),
                    },
                    engine.clone(),
                    receiver,
                    stopped,
                ));
                for prerequisite in [10, 20] {
                    if prerequisite >= operation || (operation == 30 && prerequisite == 20) {
                        break;
                    }
                    commands
                        .send(testing_command(
                            prerequisite,
                            Instant::now() + Duration::from_secs(10),
                        ))
                        .await
                        .unwrap();
                    assert_eq!(observations.recv().await, Some(prerequisite));
                    let result = results.recv().await.unwrap();
                    assert_eq!(u32::from_le_bytes(result[12..16].try_into().unwrap()), 0);
                }
                let mut command =
                    testing_command(operation, Instant::now() + Duration::from_secs(10));
                if operation == 30 {
                    command.payload = b"lookup-device".to_vec();
                }
                let request = command.request.clone();
                commands.send(command).await.unwrap();
                assert_eq!(observations.recv().await, Some(operation));
                match outcome {
                    10 => {
                        request.cancel.send_replace(true);
                    }
                    9 => tokio::time::advance(Duration::from_secs(10)).await,
                    _ => {
                        engine.stop.send_replace(true);
                    }
                }
                let entered = tokio::time::timeout(Duration::from_secs(2), shutdown_wait)
                    .await
                    .unwrap();
                if entered.is_err() {
                    if let Err(error) = task.await {
                        let _ = crate::engine::task_error(error, "Adapter worker terminated");
                    }
                    panic!("Adapter operation disposal skipped shutdown");
                }
                assert_eq!(drops.load(Ordering::SeqCst), 2);
                assert_eq!(engine.resources.snapshot(), [1, 0, 0, 0]);
                let ack = results.recv().await.unwrap();
                assert_eq!(ack[0], 1);
                assert_eq!(u32::from_le_bytes(ack[12..16].try_into().unwrap()), outcome);
                assert_eq!(
                    engine.testing_shutdown_failure(),
                    Some(Error::new(
                        18,
                        if operation == 30 {
                            "Peripheral lookup future disposal panicked"
                        } else {
                            "Adapter operation future disposal panicked"
                        }
                    ))
                );
                assert!(!task.is_finished());
                assert_eq!(
                    stops.load(Ordering::SeqCst),
                    match operation {
                        10 | 30 => 0,
                        20 => 1,
                        _ => 2,
                    }
                );
                shutdown_release.send(()).unwrap();
                task.await.unwrap();
                assert_eq!(engine.resources.snapshot(), [0; 4]);
                assert_eq!(Arc::strong_count(&drops), 1);
                assert!(
                    results.try_recv().is_err(),
                    "Request completed more than once"
                );
            }
        }
    }
    #[tokio::test]
    async fn platform_shutdown_future_disposal_retains_native_and_earlier_failure() {
        for native_result in [
            Err(Error::new(15, "Native transport shutdown failed")),
            Ok(()),
        ] {
            for earlier_failure in [false, true] {
                let (output, mut results) = mpsc::unbounded_channel();
                let (engine, stopped) = testing_engine(Arc::new(ChannelSink(output)));
                let (commands, receiver) = mpsc::channel(8);
                let (entered, mut observations) = mpsc::unbounded_channel();
                let (shutdown_entered, shutdown_wait) = oneshot::channel();
                let (shutdown_release, shutdown_pending) = oneshot::channel();
                let drops = Arc::new(AtomicUsize::new(0));
                let task = tokio::spawn(worker(
                    OperationDropDriver {
                        shutdown_result: native_result.clone(),
                        operation: 0,
                        drops: Some(drops.clone()),
                        entered,
                        shutdown_entered: Some(shutdown_entered),
                        shutdown_release: Some(shutdown_pending),
                        stops: Arc::new(AtomicUsize::new(0)),
                    },
                    engine.clone(),
                    receiver,
                    stopped,
                ));
                commands
                    .send(testing_command(
                        10,
                        Instant::now() + Duration::from_secs(10),
                    ))
                    .await
                    .unwrap();
                assert_eq!(observations.recv().await, Some(10));
                let ack = results.recv().await.unwrap();
                assert_eq!(u32::from_le_bytes(ack[12..16].try_into().unwrap()), 0);
                let expected = if earlier_failure {
                    let error = Error::new(15, "Earlier scanner failure");
                    engine.fail_shutdown(error.clone());
                    error
                } else {
                    native_result.clone().err().unwrap_or_else(|| {
                        Error::new(18, "Platform shutdown future disposal panicked")
                    })
                };
                engine.stop.send_replace(true);
                shutdown_wait.await.unwrap();
                assert_eq!(engine.resources.snapshot(), [1, 0, 0, 0]);
                assert_eq!(drops.load(Ordering::SeqCst), 0);
                assert!(!task.is_finished());
                shutdown_release.send(()).unwrap();
                let joined = task.await;
                let panicked = joined.is_err();
                if let Err(error) = joined {
                    engine.fail_shutdown(crate::engine::task_error(
                        error,
                        "Adapter worker terminated",
                    ));
                }
                assert_eq!(engine.testing_shutdown_failure(), Some(expected));
                assert!(!panicked, "Platform cleanup panic escaped the worker");
                assert_eq!(engine.resources.snapshot(), [0; 4]);
                assert_eq!(drops.load(Ordering::SeqCst), 2);
                assert_eq!(Arc::strong_count(&drops), 1);
            }
        }
    }
    impl Driver for FakeDriver {
        type Device = btleplug::platform::Peripheral;
        async fn initialize(&mut self) -> Result<(u8, Events), Error> {
            let rx = self.events.take().unwrap();
            let stream = futures_util::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.map(|item| (item, rx))
            });
            Ok((4, Box::pin(stream)))
        }
        async fn start(&mut self) -> Result<(), Error> {
            self.observations.send("start").unwrap();
            if let Some(startup) = self.startup.take() {
                let _ = startup.await;
            }
            Ok(())
        }
        async fn stop(&mut self) -> Result<(), Error> {
            self.stops.fetch_add(1, Ordering::SeqCst);
            self.observations.send("stop").unwrap();
            Ok(())
        }
    }
    struct SharedFakeDriver {
        scanner: Arc<crate::shared_scan::SharedScan>,
        owner: Arc<()>,
        starts: Arc<AtomicUsize>,
        stops: Arc<AtomicUsize>,
        events: Option<mpsc::UnboundedReceiver<Result<Vec<u8>, Error>>>,
        state: Option<Arc<crate::adapter_state::AdapterState>>,
    }
    impl Driver for SharedFakeDriver {
        type Device = btleplug::platform::Peripheral;
        async fn initialize(&mut self) -> Result<(u8, Events), Error> {
            if let Some(state) = &self.state {
                let (initial, events) = state.subscribe();
                return Ok((
                    initial,
                    Box::pin(events.map(|value| Ok(event(3, 0, Ok(vec![value]))))),
                ));
            }
            let rx = self.events.take().unwrap();
            Ok((
                4,
                Box::pin(futures_util::stream::unfold(rx, |mut rx| async move {
                    rx.recv().await.map(|item| (item, rx))
                })),
            ))
        }
        async fn start(&mut self) -> Result<(), Error> {
            self.scanner
                .start(
                    &self.owner,
                    async {
                        self.starts.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    },
                    async {
                        self.stops.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    },
                )
                .await
        }
        async fn stop(&mut self) -> Result<(), Error> {
            self.scanner
                .stop(&self.owner, async {
                    self.stops.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
        }
    }
    #[tokio::test]
    async fn closing_one_engine_preserves_another_engines_shared_scan() {
        let scanner = Arc::new(crate::shared_scan::SharedScan::default());
        let starts = Arc::new(AtomicUsize::new(0));
        let stops = Arc::new(AtomicUsize::new(0));
        let mut engines = Vec::new();
        for _ in 0..2 {
            let (output, mut results) = mpsc::unbounded_channel();
            let (engine, stopped) = testing_engine(Arc::new(ChannelSink(output)));
            let (commands, receiver) = mpsc::channel(8);
            let (events, rx) = mpsc::unbounded_channel();
            let task = tokio::spawn(worker(
                SharedFakeDriver {
                    scanner: scanner.clone(),
                    owner: Arc::new(()),
                    starts: starts.clone(),
                    stops: stops.clone(),
                    events: Some(rx),
                    state: None,
                },
                engine.clone(),
                receiver,
                stopped,
            ));
            for operation in [10, 20] {
                commands
                    .send(testing_command(
                        operation,
                        Instant::now() + Duration::from_secs(10),
                    ))
                    .await
                    .unwrap();
                let result = results.recv().await.unwrap();
                assert_eq!(u32::from_le_bytes(result[12..16].try_into().unwrap()), 0);
            }
            engines.push((engine, commands, events, results, task));
        }
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        let (engine, _commands, _events, _results, task) = engines.remove(0);
        engine.stop.send_replace(true);
        task.await.unwrap();
        assert_eq!(stops.load(Ordering::SeqCst), 0);
        let (engine, commands, events, mut results, task) = engines.remove(0);
        let advertisement = event(4, 0, Ok(vec![1, 2, 3]));
        events.send(Ok(advertisement.clone())).unwrap();
        assert_eq!(results.recv().await.unwrap(), advertisement);
        commands
            .send(testing_command(
                21,
                Instant::now() + Duration::from_secs(10),
            ))
            .await
            .unwrap();
        assert_eq!(
            u32::from_le_bytes(results.recv().await.unwrap()[12..16].try_into().unwrap()),
            0
        );
        assert_eq!(stops.load(Ordering::SeqCst), 1);
        engine.stop.send_replace(true);
        task.await.unwrap();
        assert_eq!(stops.load(Ordering::SeqCst), 1);
    }
    #[tokio::test]
    async fn rapid_adapter_loss_stops_scan_before_recovery_without_auto_restart() {
        let scanner = Arc::new(crate::shared_scan::SharedScan::default());
        let state = Arc::new(crate::adapter_state::AdapterState::default());
        state.update(4, || {});
        let starts = Arc::new(AtomicUsize::new(0));
        let stops = Arc::new(AtomicUsize::new(0));
        let (output, mut results) = mpsc::unbounded_channel();
        let (engine, stopped) = testing_engine(Arc::new(ChannelSink(output)));
        let (commands, receiver) = mpsc::channel(8);
        let task = tokio::spawn(worker(
            SharedFakeDriver {
                scanner: scanner.clone(),
                owner: Arc::new(()),
                starts: starts.clone(),
                stops: stops.clone(),
                events: None,
                state: Some(state.clone()),
            },
            engine.clone(),
            receiver,
            stopped,
        ));
        for operation in [10, 20] {
            commands
                .send(testing_command(
                    operation,
                    Instant::now() + Duration::from_secs(10),
                ))
                .await
                .unwrap();
            results.recv().await.unwrap();
        }
        assert_eq!(engine.resources.snapshot(), [1, 0, 0, 0]);
        // Both changes occur before yielding to the worker's state subscription.
        state.update(2, || scanner.invalidate(2));
        state.update(4, || {});
        assert_eq!(results.recv().await.unwrap()[16], 2);
        assert_eq!(results.recv().await.unwrap()[16], 4);
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(stops.load(Ordering::SeqCst), 1);
        commands
            .send(testing_command(
                20,
                Instant::now() + Duration::from_secs(10),
            ))
            .await
            .unwrap();
        results.recv().await.unwrap();
        assert_eq!(starts.load(Ordering::SeqCst), 2);
        engine.stop.send_replace(true);
        task.await.unwrap();
        assert_eq!(engine.resources.snapshot(), [0; 4]);
        assert_eq!(stops.load(Ordering::SeqCst), 2);
    }
    struct FailingScanCleanup(FakeDriver);
    impl Driver for FailingScanCleanup {
        type Device = btleplug::platform::Peripheral;
        async fn initialize(&mut self) -> Result<(u8, Events), Error> {
            self.0.initialize().await
        }
        async fn start(&mut self) -> Result<(), Error> {
            self.0.start().await
        }
        async fn stop(&mut self) -> Result<(), Error> {
            self.0.stop().await?;
            Err(Error::new(15, "controlled scan cleanup failure"))
        }
    }

    #[tokio::test]
    async fn scan_event_cleanup_failure_preserves_original_cause_and_retires_engine() {
        let (output, mut results) = mpsc::unbounded_channel();
        let (engine, stopped) = testing_engine(Arc::new(ChannelSink(output)));
        let (commands, receiver) = mpsc::channel(8);
        let (observations, _observed) = mpsc::unbounded_channel();
        let (events, rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(worker(
            FailingScanCleanup(FakeDriver {
                observations,
                startup: None,
                stops: Arc::new(AtomicUsize::new(0)),
                events: Some(rx),
            }),
            engine.clone(),
            receiver,
            stopped,
        ));
        for operation in [10, 20] {
            commands
                .send(testing_command(
                    operation,
                    Instant::now() + Duration::from_secs(10),
                ))
                .await
                .unwrap();
            assert_eq!(&results.recv().await.unwrap()[12..16], &0u32.to_le_bytes());
        }
        events
            .send(Err(Error::new(15, "original scanner callback cause")))
            .unwrap();
        let first = tokio::time::timeout(Duration::from_secs(2), results.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&first[16..], b"original scanner callback cause");
        let cleanup = results.recv().await.unwrap();
        assert_eq!(&cleanup[16..], b"controlled scan cleanup failure");
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert!(*engine.stop.borrow());
        assert_eq!(
            engine.testing_shutdown_failure(),
            Some(Error::new(15, "controlled scan cleanup failure"))
        );
        assert_eq!(engine.resources.snapshot(), [0; 4]);
    }

    #[tokio::test]
    async fn recoverable_scan_event_failure_retires_lease_and_allows_explicit_retry() {
        for _ in 0..100 {
            let (output, mut results) = mpsc::unbounded_channel();
            let (engine, stopped) = testing_engine(Arc::new(ChannelSink(output)));
            let (commands, receiver) = mpsc::channel(8);
            let (observations, mut observed) = mpsc::unbounded_channel();
            let (events, rx) = mpsc::unbounded_channel();
            let stops = Arc::new(AtomicUsize::new(0));
            let task = tokio::spawn(worker(
                FakeDriver {
                    observations,
                    startup: None,
                    stops: stops.clone(),
                    events: Some(rx),
                },
                engine.clone(),
                receiver,
                stopped,
            ));
            for operation in [10, 20] {
                commands
                    .send(testing_command(
                        operation,
                        Instant::now() + Duration::from_secs(10),
                    ))
                    .await
                    .unwrap();
                assert_eq!(&results.recv().await.unwrap()[12..16], &0u32.to_le_bytes());
            }
            assert_eq!(observed.recv().await.unwrap(), "start");
            events
                .send(Err(Error::new(15, "controlled scan callback failure: 3")))
                .unwrap();
            let failure = tokio::time::timeout(Duration::from_secs(2), results.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(&failure[12..16], &15u32.to_le_bytes());
            assert_eq!(&failure[16..], b"controlled scan callback failure: 3");
            assert_eq!(stops.load(Ordering::SeqCst), 1);
            assert_eq!(observed.recv().await.unwrap(), "stop");
            assert!(!*engine.stop.borrow());
            assert!(
                observed.try_recv().is_err(),
                "must not automatically restart"
            );
            // A later explicit start must invoke the driver again rather than
            // reuse a failed worker's scanning=true state.
            commands
                .send(testing_command(
                    20,
                    Instant::now() + Duration::from_secs(10),
                ))
                .await
                .unwrap();
            assert_eq!(&results.recv().await.unwrap()[12..16], &0u32.to_le_bytes());
            assert_eq!(observed.recv().await.unwrap(), "start");
            let advertisement = event(4, 0, Ok(vec![1, 2, 3]));
            events.send(Ok(advertisement.clone())).unwrap();
            assert_eq!(results.recv().await.unwrap(), advertisement);
            engine.stop.send_replace(true);
            tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(stops.load(Ordering::SeqCst), 2);
            assert_eq!(engine.resources.snapshot(), [0; 4]);
        }
    }

    #[tokio::test]
    async fn cancelled_start_is_compensated_before_next_command_and_join() {
        let (output, mut results) = mpsc::unbounded_channel();
        let (engine, stopped) = testing_engine(Arc::new(ChannelSink(output)));
        let (commands, receiver) = mpsc::channel(8);
        let (observations, mut observed) = mpsc::unbounded_channel();
        let (startup, pending) = oneshot::channel();
        let (events, rx) = mpsc::unbounded_channel();
        let stops = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn(worker(
            FakeDriver {
                observations,
                startup: Some(pending),
                stops: stops.clone(),
                events: Some(rx),
            },
            engine.clone(),
            receiver,
            stopped,
        ));
        commands
            .send(testing_command(
                10,
                Instant::now() + Duration::from_secs(10),
            ))
            .await
            .unwrap();
        assert_eq!(results.recv().await.unwrap()[16], 4);
        let start = testing_command(20, Instant::now() + Duration::from_secs(10));
        let cancel = start.request.cancel.clone();
        commands.send(start).await.unwrap();
        assert_eq!(observed.recv().await.unwrap(), "start");
        cancel.send_replace(true);
        assert_eq!(observed.recv().await.unwrap(), "stop");
        let result = results.recv().await.unwrap();
        assert_eq!(u32::from_le_bytes(result[12..16].try_into().unwrap()), 10);
        drop(startup);
        commands
            .send(testing_command(
                20,
                Instant::now() + Duration::from_secs(10),
            ))
            .await
            .unwrap();
        assert_eq!(observed.recv().await.unwrap(), "start");
        assert_eq!(
            u32::from_le_bytes(results.recv().await.unwrap()[12..16].try_into().unwrap()),
            0
        );
        // Adapter loss stops the physical scanner and reports the state.
        events.send(Ok(event(3, 0, Ok(vec![2])))).unwrap();
        assert_eq!(results.recv().await.unwrap()[16], 2);
        assert_eq!(observed.recv().await.unwrap(), "stop");
        engine.stop.send_replace(true);
        task.await.unwrap();
        assert_eq!(stops.load(Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn shutdown_joins_cleanup_during_unfinished_start() {
        let (output, mut results) = mpsc::unbounded_channel();
        let (engine, stopped) = testing_engine(Arc::new(ChannelSink(output)));
        let (commands, receiver) = mpsc::channel(8);
        let (observations, mut observed) = mpsc::unbounded_channel();
        let (_startup, pending) = oneshot::channel();
        let (_events, rx) = mpsc::unbounded_channel();
        let stops = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn(worker(
            FakeDriver {
                observations,
                startup: Some(pending),
                stops: stops.clone(),
                events: Some(rx),
            },
            engine.clone(),
            receiver,
            stopped,
        ));
        commands
            .send(testing_command(
                10,
                Instant::now() + Duration::from_secs(10),
            ))
            .await
            .unwrap();
        results.recv().await.unwrap();
        commands
            .send(testing_command(
                20,
                Instant::now() + Duration::from_secs(10),
            ))
            .await
            .unwrap();
        assert_eq!(observed.recv().await.unwrap(), "start");
        engine.stop.send_replace(true);
        task.await.unwrap();
        assert_eq!(stops.load(Ordering::SeqCst), 1);
        assert_eq!(observed.recv().await.unwrap(), "stop");
    }
    #[tokio::test(start_paused = true)]
    async fn timeout_stops_uncertain_scanner_and_next_start_still_works() {
        let (output, mut results) = mpsc::unbounded_channel();
        let (engine, stopped) = testing_engine(Arc::new(ChannelSink(output)));
        let (commands, receiver) = mpsc::channel(8);
        let (observations, mut observed) = mpsc::unbounded_channel();
        let (_startup, pending) = oneshot::channel();
        let (_events, rx) = mpsc::unbounded_channel();
        let stops = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn(worker(
            FakeDriver {
                observations,
                startup: Some(pending),
                stops: stops.clone(),
                events: Some(rx),
            },
            engine.clone(),
            receiver,
            stopped,
        ));
        commands
            .send(testing_command(
                10,
                Instant::now() + Duration::from_secs(10),
            ))
            .await
            .unwrap();
        results.recv().await.unwrap();
        commands
            .send(testing_command(20, Instant::now() + Duration::from_secs(1)))
            .await
            .unwrap();
        assert_eq!(observed.recv().await.unwrap(), "start");
        tokio::time::advance(Duration::from_secs(1)).await;
        assert_eq!(observed.recv().await.unwrap(), "stop");
        let result = results.recv().await.unwrap();
        assert_eq!(u32::from_le_bytes(result[12..16].try_into().unwrap()), 9);
        commands
            .send(testing_command(
                20,
                Instant::now() + Duration::from_secs(10),
            ))
            .await
            .unwrap();
        assert_eq!(observed.recv().await.unwrap(), "start");
        assert_eq!(
            u32::from_le_bytes(results.recv().await.unwrap()[12..16].try_into().unwrap()),
            0
        );
        engine.stop.send_replace(true);
        task.await.unwrap();
        assert_eq!(stops.load(Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn rejected_scan_start_and_repeated_stop_do_not_touch_os_scanner() {
        for state in [1, 2, 3, 4] {
            let (output, mut results) = mpsc::unbounded_channel();
            let (engine, stopped) = testing_engine(Arc::new(ChannelSink(output)));
            let (commands, receiver) = mpsc::channel(8);
            let (observations, mut observed) = mpsc::unbounded_channel();
            let (events, rx) = mpsc::unbounded_channel();
            let stops = Arc::new(AtomicUsize::new(0));
            let task = tokio::spawn(worker(
                FakeDriver {
                    observations,
                    startup: None,
                    stops: stops.clone(),
                    events: Some(rx),
                },
                engine.clone(),
                receiver,
                stopped,
            ));
            commands
                .send(testing_command(
                    10,
                    Instant::now() + Duration::from_secs(10),
                ))
                .await
                .unwrap();
            results.recv().await.unwrap();
            events.send(Ok(event(3, 0, Ok(vec![state])))).unwrap();
            assert_eq!(results.recv().await.unwrap()[16], state);
            let start = testing_command(20, Instant::now() + Duration::from_secs(10));
            if state == 4 {
                start.request.cancel.send_replace(true);
            }
            commands.send(start).await.unwrap();
            let result = results.recv().await.unwrap();
            assert_eq!(
                u32::from_le_bytes(result[12..16].try_into().unwrap()),
                if state == 4 { 10 } else { u32::from(state) }
            );
            for _ in 0..2 {
                commands
                    .send(testing_command(
                        21,
                        Instant::now() + Duration::from_secs(10),
                    ))
                    .await
                    .unwrap();
                assert_eq!(
                    u32::from_le_bytes(results.recv().await.unwrap()[12..16].try_into().unwrap()),
                    0
                );
            }
            engine.stop.send_replace(true);
            task.await.unwrap();
            assert_eq!(stops.load(Ordering::SeqCst), 0);
            assert!(observed.try_recv().is_err());
        }
    }

    #[test]
    fn adapter_initialization_classifies_transport_failure_and_preserves_evidence() {
        let error = initialization_error(btleplug::Error::Other(Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "D-Bus socket absent",
        ))));
        assert_eq!(error.code, 1);
        assert_eq!(error.message, "D-Bus socket absent");
        for (upstream, code) in [
            (btleplug::Error::NoAdapterAvailable, 1),
            (btleplug::Error::PermissionDenied, 3),
            (btleplug::Error::NotSupported("No adapter API".into()), 11),
            (btleplug::Error::TimedOut(Duration::from_secs(1)), 9),
        ] {
            let message = upstream.to_string();
            let error = initialization_error(upstream);
            assert_eq!(error.code, code);
            assert_eq!(error.message, message);
        }
    }

    #[test]
    fn unauthorized_adapter_preserves_permission_error() {
        assert_eq!(adapter_error(3).code, 3);
        assert_eq!(adapter_error(2).code, 2);
        assert_eq!(adapter_error(1).code, 1);
    }
    #[test]
    fn advertisement_preserves_binary_fields_and_opaque_identifier() {
        let mut p = btleplug::api::PeripheralProperties {
            local_name: Some("Test 🌍".into()),
            rssi: Some(-73),
            ..Default::default()
        };
        p.services
            .push(uuid::Uuid::from_u128(0x123456789abcdef0123456789abcdef0));
        p.manufacturer_data.insert(65535, vec![0, 255, 128]);
        p.service_data.insert(p.services[0], vec![1, 0, 254]);
        let bytes = advertisement("opaque-device", &p, None).unwrap();
        assert_eq!(&bytes[4..17], b"opaque-device");
        assert_eq!(
            &bytes,
            include_bytes!("../../test/fixtures/advertisement.bin")
        );
        assert!(bytes.windows(3).any(|window| window == [0, 255, 128]));
        assert!(bytes.windows(3).any(|window| window == [1, 0, 254]));
    }
}
