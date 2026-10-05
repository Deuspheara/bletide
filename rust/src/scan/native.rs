//! Adapter bootstrap, OS scan calls and btleplug event encoding.
#[cfg(target_os = "android")]
use super::adapter_error;
use super::{Driver, Events};
use crate::codec::{Error, event};
use btleplug::{
    api::{Central, CentralEvent, CentralState, Manager as _, Peripheral as _, ScanFilter},
    platform::{Adapter, Manager},
};
use futures_util::StreamExt;
#[cfg(target_os = "android")]
use std::sync::Arc;

#[derive(Default)]
pub(super) struct NativeDriver {
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
pub(super) fn initialization_error(error: btleplug::Error) -> Error {
    let mut error = Error::from(error);
    if error.code == 15 {
        error.code = 1;
    }
    error
}

#[cfg(any(target_os = "android", test))]
pub(super) fn android_scan_error(native_code: i32) -> Error {
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
pub(super) fn advertisement(
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
