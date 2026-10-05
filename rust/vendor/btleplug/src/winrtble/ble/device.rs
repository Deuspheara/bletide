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

use std::{sync::Arc, time::Duration};

use super::callback_gate::CallbackGate;

use crate::{Error, Result, api::BDAddr, winrtble::utils};
use log::{debug, trace, warn};
use tokio::time::timeout;
use windows::{
    Devices::Bluetooth::{
        BluetoothCacheMode, BluetoothConnectionStatus, BluetoothLEDevice,
        BluetoothLEPreferredConnectionParameters,
        GenericAttributeProfile::{
            GattCharacteristic, GattDescriptor, GattDeviceService, GattDeviceServicesResult,
            GattSession,
        },
    },
    Foundation::TypedEventHandler,
};

/// Timeout for uncached GATT operations before falling back to cached mode.
/// Some Windows BLE drivers hang indefinitely on uncached requests (see #325).
const GATT_CACHE_TIMEOUT: Duration = Duration::from_secs(5);

pub type ConnectedEventHandler = Box<dyn Fn(bool, &CallbackGate) + Send>;
pub type MaxPduSizeChangedEventHandler = Box<dyn Fn(u16, &CallbackGate) + Send>;
pub type DeviceErrorEventHandler = Arc<dyn Fn(&windows::core::Error, &CallbackGate) + Send + Sync>;

pub struct BLEDevice {
    device: BluetoothLEDevice,
    gatt_session: Option<GattSession>,
    connection_token: Option<i64>,
    pdu_change_token: Option<i64>,
    services: Vec<GattDeviceService>,
    closed: bool,
    callback_gate: CallbackGate,
}

impl BLEDevice {
    pub async fn new(
        address: BDAddr,
        connection_status_changed: ConnectedEventHandler,
        max_pdu_size_changed: MaxPduSizeChangedEventHandler,
        on_error: DeviceErrorEventHandler,
    ) -> Result<Self> {
        let async_op = BluetoothLEDevice::FromBluetoothAddressAsync(address.into())?;
        let device = async_op.await?;

        // Own the device before the next fallible call or await. Partial setup
        // and cancellation must close it even before a session/handler exists.
        let mut owned = BLEDevice {
            device,
            gatt_session: None,
            connection_token: None,
            pdu_change_token: None,
            services: vec![],
            closed: false,
            callback_gate: CallbackGate::new(),
        };
        let async_op = GattSession::FromDeviceIdAsync(&owned.device.BluetoothDeviceId()?)?;
        let gatt_session = async_op.await?;
        owned.gatt_session = Some(gatt_session.clone());

        let connection_gate = owned.callback_gate.clone();
        let connection_error = on_error.clone();
        let connection_status_handler =
            TypedEventHandler::<BluetoothLEDevice, _>::new(move |sender, _| {
                utils::callback_result_reported(
                    || {
                        let sender = sender.ok()?;
                        let status = sender.ConnectionStatus()?;
                        let is_connected = status == BluetoothConnectionStatus::Connected;
                        connection_status_changed(is_connected, &connection_gate);
                        trace!("state {:?}", status);
                        Ok(())
                    },
                    |error| connection_error(error, &connection_gate),
                )
            });
        owned.connection_token = Some(
            owned
                .device
                .ConnectionStatusChanged(&connection_status_handler)?,
        );

        max_pdu_size_changed(gatt_session.MaxPduSize()?, &owned.callback_gate);
        let pdu_gate = owned.callback_gate.clone();
        let max_pdu_size_changed_handler =
            TypedEventHandler::<GattSession, _>::new(move |sender, _| {
                utils::callback_result_reported(
                    || {
                        let sender = sender.ok()?;
                        let mtu = sender.MaxPduSize()?;
                        max_pdu_size_changed(mtu, &pdu_gate);
                        Ok(())
                    },
                    |error| on_error(error, &pdu_gate),
                )
            });
        owned.pdu_change_token =
            Some(gatt_session.MaxPduSizeChanged(&max_pdu_size_changed_handler)?);

        Ok(owned)
    }

    async fn get_gatt_services(
        &self,
        cache_mode: BluetoothCacheMode,
    ) -> Result<GattDeviceServicesResult> {
        let winrt_error = Error::from;
        let async_op = self
            .device
            .GetGattServicesWithCacheModeAsync(cache_mode)
            .map_err(winrt_error)?;
        let service_result = async_op.await.map_err(winrt_error)?;
        Ok(service_result)
    }

    pub(crate) fn callback_gate(&self) -> CallbackGate {
        self.callback_gate.clone()
    }

    pub fn name(&self) -> windows::core::Result<windows::core::HSTRING> {
        self.device.Name()
    }

    pub async fn connect(&self) -> Result<()> {
        if self.is_connected().await? {
            return Ok(());
        }

        let service_result = self.get_gatt_services(BluetoothCacheMode::Uncached).await?;
        let status = service_result.Status()?;
        utils::to_error(status)
    }

    pub(crate) async fn is_connected(&self) -> Result<bool> {
        let winrt_error = Error::from;
        let status = self.device.ConnectionStatus().map_err(winrt_error)?;

        Ok(status == BluetoothConnectionStatus::Connected)
    }

    pub async fn get_characteristics(
        service: &GattDeviceService,
    ) -> Result<Vec<GattCharacteristic>> {
        let async_result = match timeout(
            GATT_CACHE_TIMEOUT,
            service
                .GetCharacteristicsWithCacheModeAsync(BluetoothCacheMode::Uncached)?
                .into_future(),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                warn!("Uncached characteristic discovery timed out, falling back to cached mode");
                service
                    .GetCharacteristicsWithCacheModeAsync(BluetoothCacheMode::Cached)?
                    .await?
            }
        };

        utils::to_error(async_result.Status()?)?;
        let results = async_result.Characteristics()?;
        debug!("characteristics {:?}", results.Size());
        Ok(results.into_iter().collect())
    }

    pub async fn get_characteristic_descriptors(
        characteristic: &GattCharacteristic,
    ) -> Result<Vec<GattDescriptor>> {
        let async_result = match timeout(
            GATT_CACHE_TIMEOUT,
            characteristic
                .GetDescriptorsWithCacheModeAsync(BluetoothCacheMode::Uncached)?
                .into_future(),
        )
        .await
        {
            Ok(result) => result?,
            Err(_) => {
                warn!("Uncached descriptor discovery timed out, falling back to cached mode");
                characteristic
                    .GetDescriptorsWithCacheModeAsync(BluetoothCacheMode::Cached)?
                    .await?
            }
        };
        utils::to_error(async_result.Status()?)?;
        let results = async_result.Descriptors()?;
        debug!("descriptors {:?}", results.Size());
        Ok(results.into_iter().collect())
    }

    pub fn get_connection_parameters(&self) -> Result<crate::api::ConnectionParameters> {
        let winrt_error = Error::from;
        let params = self.device.GetConnectionParameters().map_err(winrt_error)?;
        // ConnectionInterval is in units of 1.25ms, convert to microseconds
        let interval_us = (params.ConnectionInterval().map_err(winrt_error)? as u32) * 1250;
        let latency = params.ConnectionLatency().map_err(winrt_error)?;
        // LinkTimeout is in units of 10ms, convert to microseconds
        let supervision_timeout_us = (params.LinkTimeout().map_err(winrt_error)? as u32) * 10_000;
        Ok(crate::api::ConnectionParameters {
            interval_us,
            latency,
            supervision_timeout_us,
        })
    }

    pub fn request_connection_parameters(
        &self,
        preset: crate::api::ConnectionParameterPreset,
    ) -> Result<()> {
        let winrt_error = Error::from;
        let params = match preset {
            crate::api::ConnectionParameterPreset::Balanced => {
                BluetoothLEPreferredConnectionParameters::Balanced()
            }
            crate::api::ConnectionParameterPreset::ThroughputOptimized => {
                BluetoothLEPreferredConnectionParameters::ThroughputOptimized()
            }
            crate::api::ConnectionParameterPreset::PowerOptimized => {
                BluetoothLEPreferredConnectionParameters::PowerOptimized()
            }
        }
        .map_err(winrt_error)?;
        let result = self
            .device
            .RequestPreferredConnectionParameters(&params)
            .map_err(winrt_error)?;
        let status = result.Status().map_err(winrt_error)?;
        // BluetoothLEPreferredConnectionParametersRequestStatus:
        //   Unspecified = 0, Success = 1, DeviceNotAvailable = 2, AccessDenied = 3
        match status.0 {
            1 => Ok(()),
            2 | 3 => Err(Error::NotSupported(format!(
                "request_connection_parameters not supported (status {:?})",
                status
            ))),
            _ => Err(Error::Other(
                format!(
                    "RequestPreferredConnectionParameters failed with status {:?}",
                    status
                )
                .into(),
            )),
        }
    }

    pub async fn discover_services(&mut self) -> Result<&[GattDeviceService]> {
        let winrt_error = Error::from;
        let service_result = self.get_gatt_services(BluetoothCacheMode::Cached).await?;
        let status = service_result.Status().map_err(winrt_error)?;
        utils::to_error(status)?;
        {
            // We need to convert the IVectorView to a Vec, because IVectorView is not Send and so
            // can't be help past the await point below.
            let services: Vec<_> = service_result
                .Services()
                .map_err(winrt_error)?
                .into_iter()
                .collect();
            self.services = services;
            debug!("services {:?}", self.services.len());
        }
        Ok(self.services.as_slice())
    }
}

impl BLEDevice {
    /// Explicit disconnect reports cleanup failures and retains unfinished steps
    /// for recovery. Drop remains a best-effort fallback for cancelled setup.
    pub(crate) fn close(&mut self) -> Result<()> {
        self.callback_gate.retire();
        if self.closed {
            return Ok(());
        }
        if let (Some(session), Some(token)) = (&self.gatt_session, self.pdu_change_token) {
            session.RemoveMaxPduSizeChanged(token)?;
            self.pdu_change_token = None;
        }
        if let Some(token) = self.connection_token {
            self.device.RemoveConnectionStatusChanged(token)?;
            self.connection_token = None;
        }
        while let Some(service) = self.services.last() {
            service.Close()?;
            self.services.pop();
        }
        if let Some(session) = &self.gatt_session {
            session.Close()?;
            self.gatt_session = None;
        }
        self.device.Close()?;
        self.closed = true;
        Ok(())
    }
}

impl Drop for BLEDevice {
    fn drop(&mut self) {
        self.callback_gate.retire();
        if self.closed {
            return;
        }
        // Unlike explicit close, destruction cannot retain ownership for retry.
        // Attempt every remaining independent step even if one WinRT call fails.
        if let (Some(session), Some(token)) = (&self.gatt_session, self.pdu_change_token) {
            if let Err(err) = session.RemoveMaxPduSizeChanged(token) {
                debug!("Drop: remove PDU handler {:?}", err);
            }
        }
        if let Some(token) = self.connection_token {
            if let Err(err) = self.device.RemoveConnectionStatusChanged(token) {
                debug!("Drop: remove connection handler {:?}", err);
            }
        }
        for service in &self.services {
            if let Err(err) = service.Close() {
                debug!("Drop: close service {:?}", err);
            }
        }
        if let Some(session) = &self.gatt_session {
            if let Err(err) = session.Close() {
                debug!("Drop: close session {:?}", err);
            }
        }
        if let Err(err) = self.device.Close() {
            debug!("Drop: close device {:?}", err);
        }
    }
}
