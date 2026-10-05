use super::{
    jni::{
        jvm,
        objects::{JScanFilter, JScanResult},
    },
    jni_utils::exceptions::{take_pending_exception, throwable_to_string},
    peripheral::{Peripheral, PeripheralId},
};
use crate::{
    Error, Result,
    api::{BDAddr, Central, CentralEvent, CentralState, PeripheralProperties, ScanFilter},
    common::adapter_manager::AdapterManager,
};
use async_trait::async_trait;
use futures::stream::Stream;
use jni::{
    Env, jni_sig, jni_str,
    objects::{Global, JObject, JString},
    sys::jboolean,
};
use std::{
    fmt::{Debug, Formatter},
    pin::Pin,
    str::FromStr,
    sync::Arc,
};

#[derive(Clone)]
pub struct Adapter {
    manager: Arc<AdapterManager<Peripheral>>,
    internal: Arc<Global<JObject<'static>>>,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("Adapter")
            .field("manager", &self.manager)
            .finish()
    }
}

impl Adapter {
    /// Retain discovered peripherals while an OpenBLE engine owns this adapter.
    pub fn retain_discovery_owner(&self, owner: &Arc<()>) {
        self.manager.retain_discovery_owner(owner);
    }

    /// Retire the last engine's cache; other active engines retain their state.
    pub fn release_discovery_owner(&self, owner: &Arc<()>) {
        self.manager.release_discovery_owner(owner);
    }

    /// Snapshot the Java scanner attempt and its cached native failure code.
    pub fn scan_state(&self) -> Result<(u64, i32)> {
        jvm()?.attach_current_thread(|env| {
            let value = env
                .call_method(
                    self.internal.as_obj(),
                    jni_str!("getScanState"),
                    jni_sig!("()[J"),
                    &[],
                )?
                .l()?;
            let array = env.cast_local::<jni::objects::JLongArray>(value)?;
            if array.len(env)? != 2 {
                return Err(Error::RuntimeError("Invalid Android scan state".into()));
            }
            let mut values = [0i64; 2];
            array.get_region(env, 0, &mut values)?;
            let generation = u64::try_from(values[0])
                .map_err(|_| Error::RuntimeError("Invalid Android scan generation".into()))?;
            let error_code = i32::try_from(values[1])
                .map_err(|_| Error::RuntimeError("Invalid Android scan error".into()))?;
            Ok((generation, error_code))
        })
    }

    pub(crate) fn new() -> Result<Self> {
        jvm()?.attach_current_thread(|env| {
            let obj = env.new_object(
                jni_str!("com/nonpolynomial/btleplug/android/impl/Adapter"),
                jni_sig!("()V"),
                &[],
            )?;
            let internal = Arc::new(env.new_global_ref(&obj)?);
            let adapter = Self {
                manager: Arc::new(AdapterManager::default()),
                internal,
            };
            // Safety: Adapter.handle is a private Java long, initially zero,
            // and stores only this Adapter through the JNI Rust-field protocol.
            unsafe { env.set_rust_field(&obj, jni_str!("handle"), adapter.clone()) }?;

            Ok(adapter)
        })
    }

    pub fn report_scan_result<'a>(
        &self,
        env: &mut Env<'a>,
        scan_result: JObject<'a>,
    ) -> Result<Peripheral> {
        let scan_result = env.cast_local::<JScanResult>(scan_result)?;
        let (addr, properties): (BDAddr, Option<PeripheralProperties>) =
            scan_result.to_peripheral_properties(env)?;

        match self.manager.peripheral(&PeripheralId(addr)) {
            Some(p) => match properties {
                Some(properties) => {
                    self.report_properties(&p, properties, false);
                    Ok(p)
                }
                None => Err(Error::DeviceNotFound),
            },
            None => match properties {
                Some(properties) => {
                    let p = self.add(addr)?;
                    self.report_properties(&p, properties, true);
                    Ok(p)
                }
                None => Err(Error::DeviceNotFound),
            },
        }
    }

    fn add(&self, address: BDAddr) -> Result<Peripheral> {
        if let Some(existing) = self.manager.peripheral(&PeripheralId(address)) {
            return Ok(existing);
        }
        jvm()?.attach_current_thread(|env| {
            let local_adapter = env.new_local_ref(self.internal.as_obj())?;
            let peripheral = Peripheral::new(env, local_adapter, address)?;
            Ok(self.manager.add_peripheral(peripheral))
        })
    }

    fn report_properties(
        &self,
        peripheral: &Peripheral,
        properties: PeripheralProperties,
        new: bool,
    ) {
        peripheral.report_properties(properties.clone());
        self.manager.emit(if new {
            CentralEvent::DeviceDiscovered(PeripheralId(properties.address))
        } else {
            CentralEvent::DeviceUpdated(PeripheralId(properties.address))
        });
        self.manager
            .emit(CentralEvent::ManufacturerDataAdvertisement {
                id: PeripheralId(properties.address),
                manufacturer_data: properties.manufacturer_data,
            });
        self.manager.emit(CentralEvent::ServiceDataAdvertisement {
            id: PeripheralId(properties.address),
            service_data: properties.service_data,
        });
        self.manager.emit(CentralEvent::ServicesAdvertisement {
            id: PeripheralId(properties.address),
            services: properties.services,
        });
    }
}

#[async_trait]
impl Central for Adapter {
    type Peripheral = Peripheral;

    async fn adapter_info(&self) -> Result<String> {
        Ok("Android".to_string())
    }

    async fn events(&self) -> Result<Pin<Box<dyn Stream<Item = CentralEvent> + Send>>> {
        Ok(self.manager.event_stream())
    }

    async fn start_scan(&self, filter: ScanFilter) -> Result<()> {
        jvm()?.attach_current_thread(|env| {
        let filter = JScanFilter::new(env, filter)?;
        let filter_obj: JObject = filter.into();
        match env.call_method(
            self.internal.as_obj(),
            jni_str!("startScan"),
            jni_sig!("(Lcom/nonpolynomial/btleplug/android/impl/ScanFilter;)V"),
            &[(&filter_obj).into()],
        ) {
            Ok(_) => Ok(()),
            Err(jni::errors::Error::JavaException) => {
                let ex = take_pending_exception(env)?;

                let no_adapter_class = <super::jni::objects::JNoBluetoothAdapterException as jni::objects::Reference>::lookup_class(
                    env,
                    &Default::default(),
                )?;

                if env.is_instance_of(&ex, &*no_adapter_class)? {
                    Err(Error::NoAdapterAvailable)
                } else if env.is_instance_of(&ex, jni_str!("java/lang/RuntimeException"))? {
                    let msg = env
                        .call_method(&ex, jni_str!("getMessage"), jni_sig!("()Ljava/lang/String;"), &[])?
                        .l()?;
                    let jstr = env.cast_local::<JString>(msg)?;
                    let msgstr = String::from(jstr.mutf8_chars(env)?);
                    Err(Error::RuntimeError(msgstr))
                } else {
                    let desc = throwable_to_string(env, &ex)?;
                    Err(Error::RuntimeError(format!("Java exception: {}", desc)))
                }
            }
            Err(e) => Err(e.into()),
        }
        })
    }

    async fn stop_scan(&self) -> Result<()> {
        jvm()?.attach_current_thread(|env| {
            env.call_method(
                self.internal.as_obj(),
                jni_str!("stopScan"),
                jni_sig!("()V"),
                &[],
            )?;
            Ok(())
        })
    }

    async fn peripherals(&self) -> Result<Vec<Peripheral>> {
        Ok(self.manager.peripherals())
    }

    async fn peripheral(&self, address: &PeripheralId) -> Result<Peripheral> {
        self.manager
            .peripheral(address)
            .ok_or(Error::DeviceNotFound)
    }

    async fn add_peripheral(&self, address: &PeripheralId) -> Result<Peripheral> {
        self.add(address.0)
    }

    async fn clear_peripherals(&self) -> Result<()> {
        self.manager.clear_peripherals();
        Ok(())
    }

    async fn adapter_address(&self) -> Result<Option<BDAddr>> {
        // Ordinary Android applications cannot access the local factory address.
        Ok(None)
    }

    async fn adapter_state(&self) -> Result<CentralState> {
        Ok(CentralState::Unknown)
    }
}

pub(crate) fn adapter_report_scan_result_internal<'a>(
    env: &mut Env<'a>,
    obj: &JObject,
    scan_result: JObject<'a>,
) -> crate::Result<()> {
    // Safety: only Adapter::new sets this private handle, with the same type.
    let adapter = unsafe { env.get_rust_field::<_, _, Adapter>(obj, jni_str!("handle")) }?;
    let adapter_clone = adapter.clone();
    drop(adapter);
    adapter_clone.report_scan_result(env, scan_result)?;
    Ok(())
}

pub(crate) fn adapter_on_connection_state_changed_internal(
    env: &mut Env,
    obj: &JObject,
    addr: JString,
    connected: jboolean,
) -> crate::Result<()> {
    let addr_str = String::from(addr.mutf8_chars(env)?);
    let addr = BDAddr::from_str(&addr_str)?;
    // Safety: only Adapter::new sets this private handle, with the same type.
    let adapter = unsafe { env.get_rust_field::<_, _, Adapter>(obj, jni_str!("handle")) }?;
    let adapter_clone = adapter.clone();
    drop(adapter);
    // Publishing can synchronously wake consumers; retain an owner, not the
    // Rust-field lock, just as in the scan callback above.
    adapter_clone.manager.emit(if connected {
        CentralEvent::DeviceConnected(PeripheralId(addr))
    } else {
        CentralEvent::DeviceDisconnected(PeripheralId(addr))
    });
    Ok(())
}

pub(crate) fn adapter_report_scan_failed_internal(
    env: &mut Env,
    obj: &JObject,
    generation: jni::sys::jlong,
    error_code: jni::sys::jint,
) -> crate::Result<()> {
    let generation = u64::try_from(generation)
        .map_err(|_| Error::RuntimeError("Invalid Android scan generation".into()))?;
    // Safety: only Adapter::new sets this private handle with the same type.
    let adapter = unsafe { env.get_rust_field::<_, _, Adapter>(obj, jni_str!("handle")) }?;
    let owner = adapter.clone();
    drop(adapter);
    owner.manager.emit(CentralEvent::ScanError {
        generation,
        error_code,
    });
    Ok(())
}
