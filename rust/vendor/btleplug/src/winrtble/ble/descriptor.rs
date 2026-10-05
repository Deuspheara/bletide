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

use super::super::utils;
use crate::{Result, api::Descriptor};
use std::future::IntoFuture;
use uuid::Uuid;
use windows::{
    Devices::Bluetooth::{BluetoothCacheMode, GenericAttributeProfile::GattDescriptor},
    Storage::Streams::{DataReader, DataWriter},
};

#[derive(Debug)]
pub struct BLEDescriptor {
    descriptor: GattDescriptor,
    uuid: Uuid,
}

impl BLEDescriptor {
    pub fn new(descriptor: GattDescriptor) -> Result<Self> {
        let uuid = utils::to_uuid(&descriptor.Uuid()?);
        Ok(Self { descriptor, uuid })
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn to_descriptor(&self, service_uuid: Uuid, characteristic_uuid: Uuid) -> Descriptor {
        let uuid = self.uuid();
        Descriptor {
            uuid,
            service_uuid,
            characteristic_uuid,
        }
    }

    pub async fn write_value(&self, data: &[u8]) -> Result<()> {
        let writer = DataWriter::new()?;
        writer.WriteBytes(data)?;
        let operation = self.descriptor.WriteValueAsync(&writer.DetachBuffer()?)?;
        let result = operation.into_future().await?;
        utils::to_error(result)
    }

    pub async fn read_value(&self) -> Result<Vec<u8>> {
        let result = self
            .descriptor
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
}
