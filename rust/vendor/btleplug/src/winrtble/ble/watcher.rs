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

use super::callback_gate::CallbackGate;
use crate::{Error, Result, api::ScanFilter, winrtble::utils};
use log::debug;
use std::{collections::HashSet, sync::Mutex};
use windows::{Devices::Bluetooth::Advertisement::*, Foundation::TypedEventHandler, core::Ref};

const MATCH_CACHE_CAPACITY: usize = 1024;

pub type AdvertisementEventHandler =
    Box<dyn Fn(&BluetoothLEAdvertisementReceivedEventArgs) -> windows::core::Result<()> + Send>;

#[derive(Debug)]
struct ReceivedHandler {
    token: i64,
    gate: CallbackGate,
}

#[derive(Debug)]
pub struct BLEWatcher {
    watcher: BluetoothLEAdvertisementWatcher,
    received_token: Option<ReceivedHandler>,
    /// Whether the adapter reports Coded (long-range) PHY support. Only
    /// then is `UseCodedPhy` requested: the setter succeeds on any adapter,
    /// and on one without Coded PHY the scan starts but never reports.
    coded_phy_supported: bool,
}

impl From<windows::core::Error> for Error {
    fn from(err: windows::core::Error) -> Error {
        Error::Other(Box::new(err))
    }
}

#[derive(Default)]
struct MatchCache {
    addresses: HashSet<u64>,
}

impl MatchCache {
    fn record(&mut self, address: u64) {
        if self.addresses.len() < MATCH_CACHE_CAPACITY || self.addresses.contains(&address) {
            self.addresses.insert(address);
        }
    }

    fn contains(&self, address: u64) -> bool {
        self.addresses.contains(&address)
    }
}

impl BLEWatcher {
    pub fn new(coded_phy_supported: bool) -> Result<Self> {
        let ad = BluetoothLEAdvertisementFilter::new()?;
        let watcher = BluetoothLEAdvertisementWatcher::Create(&ad)?;
        Ok(BLEWatcher {
            watcher,
            received_token: None,
            coded_phy_supported,
        })
    }

    pub fn start(
        &mut self,
        filter: ScanFilter,
        on_received: AdvertisementEventHandler,
        on_error: Box<dyn Fn(&windows::core::Error) + Send>,
    ) -> Result<()> {
        self.remove_received_handler()?;
        let ScanFilter { services } = filter;

        // Clear any OS-level service UUID filter from a previous scan.
        // We intentionally do NOT set service UUIDs on the OS filter: on some
        // Windows BLE drivers the 128-bit UUID filter silently drops matching
        // advertisements. Software filtering in the handler is used instead.
        let ad = self.watcher.AdvertisementFilter()?.Advertisement()?;
        ad.ServiceUuids()?.Clear()?;

        self.watcher
            .SetScanningMode(BluetoothLEScanningMode::Active)?;
        let _ = self.watcher.SetAllowExtendedAdvertisements(true);
        // Also receive on the Coded (long-range) PHY, but only when the
        // adapter supports it. `SetUseCodedPhy(true)` is accepted (and
        // `Start` succeeds) on adapters without Coded PHY as well, and the
        // scan then delivers no advertisements at all, so the capability
        // check is the guard rather than the setter's result.
        if self.coded_phy_supported {
            let _ = self.watcher.SetUseCodedPhy(true);
        }
        debug!(
            "extended scanning enabled; coded PHY {}",
            if self.coded_phy_supported {
                "enabled"
            } else {
                "not supported by adapter, disabled"
            }
        );

        // Pre-convert the filter UUIDs once so the handler closure is cheap.
        let filter_guids: Vec<windows::core::GUID> = services.iter().map(utils::to_guid).collect();
        let matching_devices = Mutex::new(MatchCache::default());

        let gate = CallbackGate::new();
        let callback_gate = gate.clone();
        let handler: TypedEventHandler<
            BluetoothLEAdvertisementWatcher,
            BluetoothLEAdvertisementReceivedEventArgs,
        > = TypedEventHandler::new(
            move |_sender, args: Ref<BluetoothLEAdvertisementReceivedEventArgs>| {
                utils::callback_result_reported(
                    || {
                        let mut result = Ok(());
                        callback_gate.publish(|| {
                            result = (|| {
                                let args = args.ok()?;
                                // Software service-UUID filter.
                                if !filter_guids.is_empty() {
                                    let address = args.BluetoothAddress()?;
                                    let ad_uuids = args.Advertisement()?.ServiceUuids()?;
                                    let advertised = (0..ad_uuids.Size()?)
                                        .map(|index| ad_uuids.GetAt(index))
                                        .collect::<windows::core::Result<Vec<_>>>()?;
                                    let is_match =
                                        filter_guids.iter().any(|g| advertised.contains(g));

                                    let mut cache = matching_devices
                                        .lock()
                                        .unwrap_or_else(|error| error.into_inner());
                                    if is_match {
                                        cache.record(address);
                                    } else if args.AdvertisementType()?
                                        != BluetoothLEAdvertisementType::ScanResponse
                                        || !cache.contains(address)
                                    {
                                        return Ok(());
                                    }
                                }
                                on_received(args)?;
                                Ok(())
                            })();
                        });
                        result
                    },
                    |error| {
                        // A stopped/replaced watcher must not fail its next owner.
                        callback_gate.publish(|| on_error(error));
                    },
                )
            },
        );

        let token = match self.watcher.Received(&handler) {
            Ok(token) => token,
            Err(error) => {
                gate.retire();
                return Err(error.into());
            }
        };
        self.received_token = Some(ReceivedHandler { token, gate });
        if let Err(error) = self.watcher.Start() {
            let _ = self.remove_received_handler();
            return Err(error.into());
        }
        Ok(())
    }

    pub fn stop(&mut self) -> Result<()> {
        if let Some(handler) = &self.received_token {
            handler.gate.retire();
        }
        self.watcher.Stop()?;
        self.remove_received_handler()
    }

    fn remove_received_handler(&mut self) -> Result<()> {
        if let Some(handler) = &self.received_token {
            handler.gate.retire();
            self.watcher.RemoveReceived(handler.token)?;
            self.received_token = None;
        }
        Ok(())
    }
}

impl Drop for BLEWatcher {
    fn drop(&mut self) {
        if let Some(handler) = &self.received_token {
            handler.gate.retire();
        }
        if let Err(error) = self.watcher.Stop() {
            debug!("Drop: stop watcher {error:?}");
        }
        if let Err(error) = self.remove_received_handler() {
            debug!("Drop: remove watcher handler {error:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MATCH_CACHE_CAPACITY, MatchCache};

    #[test]
    fn match_cache_is_bounded() {
        let mut cache = MatchCache::default();
        for address in 0..MATCH_CACHE_CAPACITY as u64 {
            cache.record(address);
        }
        cache.record(MATCH_CACHE_CAPACITY as u64);

        assert_eq!(cache.addresses.len(), MATCH_CACHE_CAPACITY);
        assert!(cache.contains(0));
        assert!(!cache.contains(MATCH_CACHE_CAPACITY as u64));
    }
}
