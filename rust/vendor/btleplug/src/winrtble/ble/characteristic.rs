// btleplug Source Code File
//
// Copyright 2020 Nonpolynomial Labs LLC. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.
//
// Some portions of this file are taken and/or modified from Rumble
// (https://github.com/mwylde/rumble), using a dual MIT/Apache License under the
// following copyright:
//
// Copyright (c) 2014 The Rust Project Developers

use super::{
    super::utils::to_descriptor_value, callback_gate::CallbackGate, descriptor::BLEDescriptor,
};
use crate::{
    Error, Result,
    api::{CharPropFlags, Characteristic, WriteType},
    winrtble::utils,
};

use log::{debug, trace};
use std::{collections::HashMap, future::IntoFuture};
use tokio::sync::Mutex;
use uuid::Uuid;
use windows::core::Ref;
use windows::{
    Devices::Bluetooth::{
        BluetoothCacheMode,
        GenericAttributeProfile::{
            GattCharacteristic, GattClientCharacteristicConfigurationDescriptorValue,
            GattValueChangedEventArgs, GattWriteOption,
        },
    },
    Foundation::TypedEventHandler,
    Storage::Streams::{DataReader, DataWriter},
};

pub type NotifiyEventHandler = Box<dyn Fn(Vec<u8>) + Send>;

impl From<WriteType> for GattWriteOption {
    fn from(val: WriteType) -> Self {
        match val {
            WriteType::WithoutResponse => GattWriteOption::WriteWithoutResponse,
            WriteType::WithResponse => GattWriteOption::WriteWithResponse,
        }
    }
}

#[derive(Debug)]
struct NotifyHandler {
    token: i64,
    gate: CallbackGate,
}

#[derive(Debug)]
pub struct BLECharacteristic {
    characteristic: GattCharacteristic,
    uuid: Uuid,
    properties: CharPropFlags,
    session_gate: CallbackGate,
    pub descriptors: HashMap<Uuid, BLEDescriptor>,
    notify_token: Mutex<Option<NotifyHandler>>,
}

impl BLECharacteristic {
    pub(crate) fn new(
        characteristic: GattCharacteristic,
        descriptors: HashMap<Uuid, BLEDescriptor>,
        session_gate: CallbackGate,
    ) -> Result<Self> {
        let uuid = utils::to_uuid(&characteristic.Uuid()?);
        let properties = utils::to_char_props(&characteristic.CharacteristicProperties()?);
        Ok(BLECharacteristic {
            characteristic,
            uuid,
            properties,
            session_gate,
            descriptors,
            notify_token: Mutex::new(None),
        })
    }

    pub async fn write_value(&self, data: &[u8], write_type: WriteType) -> Result<()> {
        let writer = DataWriter::new()?;
        writer.WriteBytes(data)?;
        let operation = self
            .characteristic
            .WriteValueWithOptionAsync(&writer.DetachBuffer()?, write_type.into())?;
        let result = operation.into_future().await?;
        utils::to_error(result)
    }

    pub async fn read_value(&self) -> Result<Vec<u8>> {
        let result = self
            .characteristic
            .ReadValueWithCacheModeAsync(BluetoothCacheMode::Uncached)?
            .into_future()
            .await?;
        utils::to_error(result.Status()?)?;
        let value = result.Value()?;
        let reader = DataReader::FromBuffer(&value)?;
        let len = reader.UnconsumedBufferLength()? as usize;
        let mut input = vec![0u8; len];
        reader.ReadBytes(&mut input[0..len])?;
        Ok(input)
    }

    fn remove_notify_handler(&self, notify_token: &mut Option<NotifyHandler>) -> Result<()> {
        if let Some(handler) = notify_token.as_ref() {
            handler.gate.retire();
            // Only relinquish ownership after WinRT confirms removal. This keeps
            // the token available for a later retry when removal fails.
            self.characteristic.RemoveValueChanged(handler.token)?;
            *notify_token = None;
        }
        Ok(())
    }

    pub async fn subscribe(
        &self,
        on_value_changed: NotifiyEventHandler,
        on_error: Box<dyn Fn(&windows::core::Error) + Send>,
    ) -> Result<()> {
        // Held across the CCCD write to serialize subscribe/unsubscribe per characteristic.
        let mut notify_token = self.notify_token.lock().await;

        // Validate before changing the existing subscription state.
        let config = to_descriptor_value(self.characteristic.CharacteristicProperties()?);
        if config == GattClientCharacteristicConfigurationDescriptorValue::None {
            return Err(Error::NotSupported("Can not subscribe to attribute".into()));
        }

        // A replacement is allowed, but never leave two handlers installed. If
        // removal fails, retain the old token and reject the replacement.
        self.remove_notify_handler(&mut notify_token)?;

        let gate = CallbackGate::new();
        let callback_gate = gate.clone();
        let session_gate = self.session_gate.clone();
        let token = {
            let value_handler = TypedEventHandler::new(
                move |_: Ref<GattCharacteristic>, args: Ref<GattValueChangedEventArgs>| {
                    utils::callback_result_reported(
                        || {
                            let args = args.ok()?;
                            let value = args.CharacteristicValue()?;
                            let reader = DataReader::FromBuffer(&value)?;
                            let len = reader.UnconsumedBufferLength()? as usize;
                            let mut input: Vec<u8> = vec![0u8; len];
                            reader.ReadBytes(&mut input[0..len])?;
                            trace!("changed {:?}", input);
                            session_gate.publish(|| {
                                callback_gate.publish(|| on_value_changed(input));
                            });
                            Ok(())
                        },
                        |error| {
                            session_gate.publish(|| {
                                callback_gate.publish(|| on_error(error));
                            });
                        },
                    )
                },
            );
            match self.characteristic.ValueChanged(&value_handler) {
                Ok(token) => token,
                Err(error) => {
                    gate.retire();
                    return Err(error.into());
                }
            }
        };
        *notify_token = Some(NotifyHandler { token, gate });
        // Retire/remove on failure or future cancellation. A failed removal keeps
        // the retired token owned for unsubscribe/replacement/Drop to retry.
        let cleanup = crate::windows_setup_cleanup::SetupCleanup::new(|| {
            if let Err(error) = self.remove_notify_handler(&mut notify_token) {
                debug!("Pending notification setup cleanup failed: {error}");
            }
        });
        let status = self
            .characteristic
            .WriteClientCharacteristicConfigurationDescriptorAsync(config)?
            .into_future()
            .await?;
        trace!("subscribe {:?}", status);
        utils::to_error(status)?;
        cleanup.commit();
        Ok(())
    }

    pub async fn unsubscribe(&self) -> Result<()> {
        let mut notify_token = self.notify_token.lock().await;

        // Disable the CCCD first. If that fails, retain the token and handler so
        // ownership is still available for a later cleanup retry.
        let config = GattClientCharacteristicConfigurationDescriptorValue::None;
        let status = self
            .characteristic
            .WriteClientCharacteristicConfigurationDescriptorAsync(config)?
            .into_future()
            .await?;
        trace!("unsubscribe {:?}", status);
        utils::to_error(status)?;

        // Keep the token if removal fails; the next unsubscribe (or Drop) can retry.
        self.remove_notify_handler(&mut notify_token)
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn to_characteristic(&self, service_uuid: Uuid) -> Characteristic {
        let uuid = self.uuid();
        let properties = self.properties;
        let descriptors = self
            .descriptors
            .values()
            .map(|descriptor| descriptor.to_descriptor(service_uuid, uuid))
            .collect();
        Characteristic {
            uuid,
            service_uuid,
            descriptors,
            properties,
        }
    }
}

impl Drop for BLECharacteristic {
    fn drop(&mut self) {
        if let Some(handler) = self.notify_token.get_mut().as_ref() {
            handler.gate.retire();
            let result = self.characteristic.RemoveValueChanged(handler.token);
            if let Err(err) = result {
                debug!("Drop:remove_connection_status_changed {:?}", err);
            }
        }
    }
}
