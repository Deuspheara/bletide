//! btleplug attribute cache and platform operations for one connection generation.
use super::{Device, Driver, Notifications};
use crate::codec::{Error, Reader};
use btleplug::{
    api::{
        CharPropFlags, Characteristic, ConnectionParameterPreset, Descriptor,
        NotificationSetupMode, Peripheral as _, SubscriptionOptions, WriteType,
    },
    platform::Peripheral,
};
use futures_util::StreamExt;
use std::collections::BTreeSet;

impl Device for Peripheral {
    type Driver = NativeDriver;
    fn driver(self) -> NativeDriver {
        NativeDriver {
            peripheral: self,
            services: BTreeSet::new(),
            #[cfg(target_os = "android")]
            lease: None,
        }
    }
    async fn probe_connected(&self) -> Result<bool, Error> {
        btleplug::api::Peripheral::is_connected(self)
            .await
            .map_err(Error::from)
    }
}
pub(crate) struct NativeDriver {
    peripheral: Peripheral,
    services: BTreeSet<btleplug::api::Service>,
    #[cfg(target_os = "android")]
    lease: Option<crate::peripheral_lease::Lease>,
}
impl NativeDriver {
    async fn disconnect_peripheral(&self) -> Result<(), Error> {
        match self.peripheral.disconnect().await {
            Ok(()) | Err(btleplug::Error::NotConnected) => Ok(()),
            Err(error) => Err(Error::from(error)),
        }
    }
    fn characteristic(
        &self,
        service: uuid::Uuid,
        uuid: uuid::Uuid,
    ) -> Result<Characteristic, Error> {
        lookup_characteristic(&self.services, service, uuid)
    }
    fn descriptor(
        &self,
        characteristic: &Characteristic,
        uuid: uuid::Uuid,
    ) -> Result<Descriptor, Error> {
        let mut matches = characteristic.descriptors.iter().filter(|d| d.uuid == uuid);
        let descriptor = matches
            .next()
            .ok_or_else(|| Error::new(14, "Descriptor not discovered"))?;
        if matches.next().is_some() {
            return Err(Error::new(
                11,
                "Ambiguous descriptor UUID within characteristic",
            ));
        }
        Ok(descriptor.clone())
    }
}
pub(super) fn lookup_characteristic(
    services: &BTreeSet<btleplug::api::Service>,
    service_uuid: uuid::Uuid,
    uuid: uuid::Uuid,
) -> Result<Characteristic, Error> {
    let mut matches = services.iter().filter(|s| s.uuid == service_uuid);
    let service = matches
        .next()
        .ok_or_else(|| Error::new(12, "Service not discovered"))?;
    if matches.next().is_some() {
        return Err(Error::new(11, "Ambiguous service UUID"));
    }
    let mut matches = service.characteristics.iter().filter(|c| c.uuid == uuid);
    let characteristic = matches
        .next()
        .ok_or_else(|| Error::new(13, "Characteristic not discovered"))?;
    if matches.next().is_some() {
        return Err(Error::new(
            11,
            "Ambiguous characteristic UUID within service",
        ));
    }
    Ok(characteristic.clone())
}
impl Driver for NativeDriver {
    async fn connect(&mut self) -> Result<(), Error> {
        #[cfg(target_os = "android")]
        {
            let lease = crate::peripheral_lease::process_registry()
                .acquire(self.peripheral.id().to_string())?;
            let recover = lease.recover;
            self.lease = Some(lease);
            if recover {
                self.disconnect_peripheral().await?;
            }
        }
        self.peripheral.connect().await.map_err(Error::from)?;
        if !self.peripheral.is_connected().await.map_err(Error::from)? {
            return Err(Error::new(7, "Connection did not remain established"));
        }
        Ok(())
    }
    async fn disconnect(&mut self) -> Result<(), Error> {
        #[cfg(target_os = "android")]
        if self.lease.is_none() {
            // A rejected cross-engine connect never acquired physical ownership.
            return Ok(());
        }
        self.disconnect_peripheral().await?;
        #[cfg(target_os = "android")]
        if let Some(lease) = &mut self.lease {
            lease.mark_clean()?;
        }
        Ok(())
    }
    async fn notifications(&mut self) -> Result<Notifications, Error> {
        Ok(Box::pin(
            self.peripheral
                .notification_results()
                .await
                .map_err(Error::from)?
                .map(|value| value.map_err(Error::from)),
        ))
    }
    async fn operate(&mut self, operation: u32, payload: &[u8]) -> Result<Vec<u8>, Error> {
        let mut reader = Reader::new(payload);
        match operation {
            40 => {
                reader.finish()?;
                self.services.clear();
                self.peripheral
                    .discover_services()
                    .await
                    .map_err(Error::from)?;
                self.services = self.peripheral.services();
                encode_services(&self.services)
            }
            41..=47 => {
                let service = reader.uuid()?;
                let uuid = reader.uuid()?;
                let characteristic = self.characteristic(service, uuid)?;
                match operation {
                    41 => {
                        reader.finish()?;
                        require(&characteristic, CharPropFlags::READ)?;
                        self.peripheral
                            .read(&characteristic)
                            .await
                            .map_err(Error::from)
                    }
                    42 | 43 => {
                        let with_response = operation == 42;
                        require(
                            &characteristic,
                            if with_response {
                                CharPropFlags::WRITE
                            } else {
                                CharPropFlags::WRITE_WITHOUT_RESPONSE
                            },
                        )?;
                        self.peripheral
                            .write(
                                &characteristic,
                                reader.remaining(),
                                if with_response {
                                    WriteType::WithResponse
                                } else {
                                    WriteType::WithoutResponse
                                },
                            )
                            .await
                            .map_err(Error::from)?;
                        Ok(Vec::new())
                    }
                    44 => {
                        let compatibility = notification_compatibility(reader.remaining())?;
                        require(
                            &characteristic,
                            CharPropFlags::NOTIFY | CharPropFlags::INDICATE,
                        )?;
                        if compatibility {
                            require(&characteristic, CharPropFlags::NOTIFY)?;
                            self.peripheral
                                .subscribe_with_options(
                                    &characteristic,
                                    SubscriptionOptions {
                                        setup_mode: NotificationSetupMode::Compat,
                                    },
                                )
                                .await
                        } else {
                            self.peripheral.subscribe(&characteristic).await
                        }
                        .map_err(Error::from)?;
                        Ok(Vec::new())
                    }
                    45 => {
                        let compatibility = notification_compatibility(reader.remaining())?;
                        if compatibility {
                            require(&characteristic, CharPropFlags::NOTIFY)?;
                            self.peripheral
                                .unsubscribe_with_options(
                                    &characteristic,
                                    SubscriptionOptions {
                                        setup_mode: NotificationSetupMode::Compat,
                                    },
                                )
                                .await
                        } else {
                            self.peripheral.unsubscribe(&characteristic).await
                        }
                        .map_err(Error::from)?;
                        Ok(Vec::new())
                    }
                    46 | 47 => {
                        let descriptor = self.descriptor(&characteristic, reader.uuid()?)?;
                        if operation == 46 {
                            reader.finish()?;
                            self.peripheral
                                .read_descriptor(&descriptor)
                                .await
                                .map_err(Error::from)
                        } else {
                            self.peripheral
                                .write_descriptor(&descriptor, reader.remaining())
                                .await
                                .map_err(Error::from)?;
                            Ok(Vec::new())
                        }
                    }
                    _ => Err(Error::new(11, "Unknown GATT operation")),
                }
            }
            48 => {
                reader.finish()?;
                if cfg!(any(target_os = "linux", target_os = "windows")) {
                    return Err(Error::new(
                        11,
                        "Platform exposes cached RSSI rather than a fresh connected measurement",
                    ));
                }
                Ok(self
                    .peripheral
                    .read_rssi()
                    .await
                    .map_err(Error::from)?
                    .to_le_bytes()
                    .to_vec())
            }
            49 => {
                reader.finish()?;
                Ok(self.peripheral.mtu().to_le_bytes().to_vec())
            }
            50 => {
                let bytes = reader.take(2)?;
                let mtu = u16::from_le_bytes([bytes[0], bytes[1]]);
                reader.finish()?;
                if !(23..=517).contains(&mtu) {
                    return Err(Error::new(16, "MTU must be between 23 and 517"));
                }
                #[cfg(target_os = "android")]
                {
                    return Ok(self
                        .peripheral
                        .request_mtu(mtu)
                        .await
                        .map_err(Error::from)?
                        .to_le_bytes()
                        .to_vec());
                }
                #[cfg(not(target_os = "android"))]
                {
                    Err(Error::new(
                        11,
                        "Explicit MTU negotiation is available only on Android",
                    ))
                }
            }
            51 => {
                let priority = match reader.take(1)?[0] {
                    0 => ConnectionParameterPreset::Balanced,
                    1 => ConnectionParameterPreset::ThroughputOptimized,
                    2 => ConnectionParameterPreset::PowerOptimized,
                    _ => return Err(Error::new(16, "Invalid connection priority")),
                };
                reader.finish()?;
                if !cfg!(target_os = "android") {
                    return Err(Error::new(
                        11,
                        "Connection priority is available only on Android",
                    ));
                }
                self.peripheral
                    .request_connection_parameters(priority)
                    .await
                    .map_err(Error::from)?;
                Ok(Vec::new())
            }
            _ => Err(Error::new(11, "Unknown GATT operation")),
        }
    }
}
pub(super) fn require(c: &Characteristic, flags: CharPropFlags) -> Result<(), Error> {
    if c.properties.intersects(flags) {
        Ok(())
    } else {
        Err(Error::new(11, "Characteristic lacks required property"))
    }
}
pub(crate) fn encode_services(
    services: &BTreeSet<btleplug::api::Service>,
) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    out.extend_from_slice(&(services.len() as u32).to_le_bytes());
    for service in services {
        out.extend_from_slice(service.uuid.as_bytes());
        out.push(u8::from(service.primary));
        out.extend_from_slice(&(service.characteristics.len() as u32).to_le_bytes());
        for characteristic in &service.characteristics {
            out.extend_from_slice(characteristic.uuid.as_bytes());
            out.push(characteristic.properties.bits());
            out.extend_from_slice(&(characteristic.descriptors.len() as u32).to_le_bytes());
            for descriptor in &characteristic.descriptors {
                out.extend_from_slice(descriptor.uuid.as_bytes());
            }
        }
    }
    if out.len() > 1_048_576 {
        return Err(Error::new(18, "Service discovery exceeds transport limit"));
    }
    Ok(out)
}

pub(super) fn notification_compatibility(payload: &[u8]) -> Result<bool, Error> {
    match payload {
        [] | [0] => Ok(false),
        [1] => Ok(true),
        _ => Err(Error::new(16, "Invalid notification compatibility policy")),
    }
}
