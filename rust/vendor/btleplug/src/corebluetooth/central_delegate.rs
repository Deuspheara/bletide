// btleplug Source Code File
//
// Copyright 2020 Nonpolynomial Labs LLC. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.
//
// Some portions of this file are taken and/or modified from blurmac
// (https://github.com/servo/devices), using a BSD 3-Clause license under the
// following copyright:
//
// Copyright (c) 2017 Akos Kiss.
//
// Licensed under the BSD 3-Clause License
// <LICENSE.md or https://opensource.org/licenses/BSD-3-Clause>.
// This file may not be copied, modified, or distributed except
// according to those terms.

use super::utils::nsstring_to_string;
use super::utils::{core_bluetooth::cbuuid_to_uuid, nsuuid_to_uuid};
use futures::channel::mpsc::Sender;
use futures::sink::SinkExt;
use log::{error, trace, warn};
use objc2::runtime::{AnyObject, ProtocolObject};
use objc2::{AnyThread, DefinedClass, define_class, msg_send, rc::Retained};
use objc2_core_bluetooth::{
    CBAdvertisementDataLocalNameKey, CBAdvertisementDataManufacturerDataKey,
    CBAdvertisementDataServiceDataKey, CBAdvertisementDataServiceUUIDsKey,
    CBAdvertisementDataTxPowerLevelKey, CBCentralManager, CBCentralManagerDelegate,
    CBCharacteristic, CBCharacteristicProperties, CBDescriptor, CBManagerState, CBPeripheral,
    CBPeripheralDelegate, CBService, CBUUID,
};
use objc2_foundation::{
    NSArray, NSData, NSDictionary, NSError, NSNumber, NSObject, NSObjectProtocol, NSString,
};
use std::{
    collections::HashMap,
    fmt::{self, Debug, Formatter},
    ops::Deref,
};
use uuid::Uuid;

pub enum CentralDelegateEvent {
    CallbackFailed {
        callback: &'static str,
    },
    DidUpdateState {
        state: CBManagerState,
    },
    DiscoveredPeripheral {
        cbperipheral: Retained<CBPeripheral>,
        advertisement_name: Option<String>,
    },
    DiscoveredServices {
        peripheral_uuid: Uuid,
        services: HashMap<Uuid, Retained<CBService>>,
        error: Option<super::native_error::NativeError>,
    },
    ManufacturerData {
        peripheral_uuid: Uuid,
        manufacturer_id: u16,
        data: Vec<u8>,
        rssi: i16,
    },
    ServiceData {
        peripheral_uuid: Uuid,
        service_data: HashMap<Uuid, Vec<u8>>,
        rssi: i16,
    },
    Services {
        peripheral_uuid: Uuid,
        service_uuids: Vec<Uuid>,
        rssi: i16,
    },
    ServicesModified {
        peripheral_uuid: Uuid,
        invalidated_services: Vec<Uuid>,
    },
    // DiscoveredIncludedServices(Uuid, HashMap<Uuid, Retained<CBService>>),
    DiscoveredCharacteristics {
        peripheral_uuid: Uuid,
        service_uuid: Uuid,
        /// Characteristic UUID to CBCharacteristic
        characteristics: HashMap<Uuid, Retained<CBCharacteristic>>,
        /// Present if CB reported an error; `characteristics` is then an empty
        /// placeholder, not the service's real (possibly now-empty) set.
        error: Option<super::native_error::NativeError>,
    },
    DiscoveredCharacteristicDescriptors {
        peripheral_uuid: Uuid,
        service_uuid: Uuid,
        characteristic_uuid: Uuid,
        descriptors: HashMap<Uuid, Retained<CBDescriptor>>,
        /// Present if CB reported an error; `descriptors` is then an empty
        /// placeholder, not the characteristic's real (possibly now-empty) set.
        error: Option<super::native_error::NativeError>,
    },
    ConnectedDevice {
        peripheral_uuid: Uuid,
    },
    ConnectionFailed {
        peripheral_uuid: Uuid,
        error_description: Option<super::native_error::NativeError>,
    },
    DisconnectedDevice {
        peripheral_uuid: Uuid,
        error: Option<super::native_error::NativeError>,
    },
    CharacteristicNotificationStateUpdated {
        peripheral_uuid: Uuid,
        service_uuid: Uuid,
        characteristic_uuid: Uuid,
        error: Option<super::native_error::NativeError>,
        missing_cccd: bool,
    },
    CharacteristicNotified {
        peripheral_uuid: Uuid,
        service_uuid: Uuid,
        characteristic_uuid: Uuid,
        data: Vec<u8>,
        error: Option<super::native_error::NativeError>,
    },
    CharacteristicWritten {
        peripheral_uuid: Uuid,
        service_uuid: Uuid,
        characteristic_uuid: Uuid,
        error: Option<super::native_error::NativeError>,
    },
    DescriptorNotified {
        peripheral_uuid: Uuid,
        service_uuid: Uuid,
        characteristic_uuid: Uuid,
        descriptor_uuid: Uuid,
        data: Vec<u8>,
        error: Option<super::native_error::NativeError>,
    },
    DescriptorWritten {
        peripheral_uuid: Uuid,
        service_uuid: Uuid,
        characteristic_uuid: Uuid,
        descriptor_uuid: Uuid,
        error: Option<super::native_error::NativeError>,
    },
    TxPowerLevel {
        peripheral_uuid: Uuid,
        tx_power_level: i16,
    },
    DidReadRssi {
        peripheral_uuid: Uuid,
        rssi: i16,
        error: Option<super::native_error::NativeError>,
    },
    ReadyToSendWriteWithoutResponse {
        peripheral_uuid: Uuid,
    },
}

impl Debug for CentralDelegateEvent {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            CentralDelegateEvent::CallbackFailed { callback } => f
                .debug_struct("CallbackFailed")
                .field("callback", callback)
                .finish(),
            CentralDelegateEvent::DidUpdateState { state } => f
                .debug_struct("CentralDelegateEvent")
                .field("state", state)
                .finish(),
            CentralDelegateEvent::DiscoveredPeripheral {
                cbperipheral,
                advertisement_name,
            } => f
                .debug_struct("CentralDelegateEvent")
                .field("cbperipheral", cbperipheral.deref())
                .field("advertisement_name", advertisement_name)
                .finish(),
            CentralDelegateEvent::DiscoveredServices {
                peripheral_uuid,
                services,
                error,
            } => f
                .debug_struct("DiscoveredServices")
                .field("peripheral_uuid", peripheral_uuid)
                .field("services", &services.keys().collect::<Vec<_>>())
                .field("error", error)
                .finish(),
            CentralDelegateEvent::DiscoveredCharacteristics {
                peripheral_uuid,
                service_uuid,
                characteristics,
                error,
            } => f
                .debug_struct("DiscoveredCharacteristics")
                .field("peripheral_uuid", peripheral_uuid)
                .field("service_uuid", service_uuid)
                .field(
                    "characteristics",
                    &characteristics.keys().collect::<Vec<_>>(),
                )
                .field("error", error)
                .finish(),
            CentralDelegateEvent::DiscoveredCharacteristicDescriptors {
                peripheral_uuid,
                service_uuid,
                characteristic_uuid,
                descriptors,
                error,
            } => f
                .debug_struct("DiscoveredCharacteristicDescriptors")
                .field("peripheral_uuid", peripheral_uuid)
                .field("service_uuid", service_uuid)
                .field("characteristic_uuid", characteristic_uuid)
                .field("descriptors", &descriptors.keys().collect::<Vec<_>>())
                .field("error", error)
                .finish(),
            CentralDelegateEvent::ConnectedDevice { peripheral_uuid } => f
                .debug_struct("ConnectedDevice")
                .field("peripheral_uuid", peripheral_uuid)
                .finish(),
            CentralDelegateEvent::ConnectionFailed {
                peripheral_uuid,
                error_description,
            } => f
                .debug_struct("ConnectionFailed")
                .field("peripheral_uuid", peripheral_uuid)
                .field("error_description", error_description)
                .finish(),
            CentralDelegateEvent::DisconnectedDevice {
                peripheral_uuid,
                error,
            } => f
                .debug_struct("DisconnectedDevice")
                .field("peripheral_uuid", peripheral_uuid)
                .field("error", error)
                .finish(),
            CentralDelegateEvent::CharacteristicNotificationStateUpdated {
                peripheral_uuid,
                service_uuid,
                characteristic_uuid,
                ..
            } => f
                .debug_struct("CharacteristicNotificationStateUpdated")
                .field("peripheral_uuid", peripheral_uuid)
                .field("service_uuid", service_uuid)
                .field("characteristic_uuid", characteristic_uuid)
                .finish(),
            CentralDelegateEvent::CharacteristicNotified {
                peripheral_uuid,
                service_uuid,
                characteristic_uuid,
                data,
                ..
            } => f
                .debug_struct("CharacteristicNotified")
                .field("peripheral_uuid", peripheral_uuid)
                .field("service_uuid", service_uuid)
                .field("characteristic_uuid", characteristic_uuid)
                .field("data", data)
                .finish(),
            CentralDelegateEvent::CharacteristicWritten {
                peripheral_uuid,
                service_uuid,
                characteristic_uuid,
                ..
            } => f
                .debug_struct("CharacteristicWritten")
                .field("service_uuid", service_uuid)
                .field("peripheral_uuid", peripheral_uuid)
                .field("characteristic_uuid", characteristic_uuid)
                .finish(),
            CentralDelegateEvent::ManufacturerData {
                peripheral_uuid,
                manufacturer_id,
                data,
                rssi,
            } => f
                .debug_struct("ManufacturerData")
                .field("peripheral_uuid", peripheral_uuid)
                .field("manufacturer_id", manufacturer_id)
                .field("data", data)
                .field("rssi", rssi)
                .finish(),
            CentralDelegateEvent::ServiceData {
                peripheral_uuid,
                service_data,
                rssi,
            } => f
                .debug_struct("ServiceData")
                .field("peripheral_uuid", peripheral_uuid)
                .field("service_data", service_data)
                .field("rssi", rssi)
                .finish(),
            CentralDelegateEvent::Services {
                peripheral_uuid,
                service_uuids,
                rssi,
            } => f
                .debug_struct("Services")
                .field("peripheral_uuid", peripheral_uuid)
                .field("service_uuids", service_uuids)
                .field("rssi", rssi)
                .finish(),
            CentralDelegateEvent::ServicesModified {
                peripheral_uuid,
                invalidated_services,
            } => f
                .debug_struct("ServicesModified")
                .field("peripheral_uuid", peripheral_uuid)
                .field("invalidated_services", invalidated_services)
                .finish(),
            CentralDelegateEvent::DescriptorNotified {
                peripheral_uuid,
                service_uuid,
                characteristic_uuid,
                descriptor_uuid,
                data,
                ..
            } => f
                .debug_struct("DescriptorNotified")
                .field("peripheral_uuid", peripheral_uuid)
                .field("service_uuid", service_uuid)
                .field("characteristic_uuid", characteristic_uuid)
                .field("descriptor_uuid", descriptor_uuid)
                .field("data", data)
                .finish(),
            CentralDelegateEvent::DescriptorWritten {
                peripheral_uuid,
                service_uuid,
                characteristic_uuid,
                descriptor_uuid,
                ..
            } => f
                .debug_struct("DescriptorWritten")
                .field("service_uuid", service_uuid)
                .field("peripheral_uuid", peripheral_uuid)
                .field("characteristic_uuid", characteristic_uuid)
                .field("descriptor_uuid", descriptor_uuid)
                .finish(),
            CentralDelegateEvent::TxPowerLevel {
                peripheral_uuid,
                tx_power_level,
            } => f
                .debug_struct("TxPowerLevel")
                .field("peripheral_uuid", peripheral_uuid)
                .field("tx_power_level", tx_power_level)
                .finish(),
            CentralDelegateEvent::DidReadRssi {
                peripheral_uuid,
                rssi,
                ..
            } => f
                .debug_struct("DidReadRssi")
                .field("peripheral_uuid", peripheral_uuid)
                .field("rssi", rssi)
                .finish(),
            CentralDelegateEvent::ReadyToSendWriteWithoutResponse { peripheral_uuid } => f
                .debug_struct("ReadyToSendWriteWithoutResponse")
                .field("peripheral_uuid", peripheral_uuid)
                .finish(),
        }
    }
}

define_class!(
    #[derive(Debug)]
    #[unsafe(super(NSObject))]
    #[thread_kind = AnyThread]
    #[ivars = Sender<CentralDelegateEvent>]
    pub struct CentralDelegate;

    unsafe impl NSObjectProtocol for CentralDelegate {}

    unsafe impl CBCentralManagerDelegate for CentralDelegate {
        #[unsafe(method(centralManagerDidUpdateState:))]
        fn delegate_centralmanagerdidupdatestate(&self, central: &CBCentralManager) {
            self.callback_boundary("delegate_centralmanagerdidupdatestate", || {
                trace!("delegate_centralmanagerdidupdatestate");
                let state = unsafe { central.state() };
                self.send_event(CentralDelegateEvent::DidUpdateState { state });
            });
        }

        // #[method(centralManager:willRestoreState:)]
        // fn delegate_centralmanager_willrestorestate(&self, _central: &CBCentralManager, _dict: &NSDictionary<NSString, AnyObject>) {
        //     trace!("delegate_centralmanager_willrestorestate");
        // }

        #[unsafe(method(centralManager:didConnectPeripheral:))]
        fn delegate_centralmanager_didconnectperipheral(
            &self,
            _central: &CBCentralManager,
            peripheral: &CBPeripheral,
        ) {
            self.callback_boundary("delegate_centralmanager_didconnectperipheral", || {
                trace!(
                    "delegate_centralmanager_didconnectperipheral {}",
                    peripheral_debug(peripheral)
                );
                unsafe { peripheral.setDelegate(Some(ProtocolObject::from_ref(self))) };
                let id = unsafe { peripheral.identifier() };
                let peripheral_uuid = nsuuid_to_uuid(&id);
                self.send_event(CentralDelegateEvent::ConnectedDevice { peripheral_uuid });
            });
        }

        #[unsafe(method(centralManager:didDisconnectPeripheral:error:))]
        fn delegate_centralmanager_diddisconnectperipheral_error(
            &self,
            _central: &CBCentralManager,
            peripheral: &CBPeripheral,
            error: Option<&NSError>,
        ) {
            self.callback_boundary("delegate_centralmanager_diddisconnectperipheral_error", || {
                trace!(
                    "delegate_centralmanager_diddisconnectperipheral_error {} (error={:?})",
                    peripheral_debug(peripheral),
                    error
                );
                let id = unsafe { peripheral.identifier() };
                let peripheral_uuid = nsuuid_to_uuid(&id);
                self.send_event(CentralDelegateEvent::DisconnectedDevice {
                    peripheral_uuid,
                    error: error.map(super::native_error::NativeError::copy),
                });
            });
        }

        #[unsafe(method(centralManager:didFailToConnectPeripheral:error:))]
        fn delegate_centralmanager_didfailtoconnectperipheral_error(
            &self,
            _central: &CBCentralManager,
            peripheral: &CBPeripheral,
            error: Option<&NSError>,
        ) {
            self.callback_boundary("delegate_centralmanager_didfailtoconnectperipheral_error", || {
                trace!("delegate_centralmanager_didfailtoconnectperipheral_error");
                let id = unsafe { peripheral.identifier() };
                let peripheral_uuid = nsuuid_to_uuid(&id);
                let error_description = error.map(super::native_error::NativeError::copy);
                self.send_event(CentralDelegateEvent::ConnectionFailed {
                    peripheral_uuid,
                    error_description,
                });
            });
        }

        #[unsafe(method(centralManager:didDiscoverPeripheral:advertisementData:RSSI:))]
        fn delegate_centralmanager_diddiscoverperipheral_advertisementdata_rssi(
            &self,
            _central: &CBCentralManager,
            peripheral: &CBPeripheral,
            adv_data: &NSDictionary<NSString, AnyObject>,
            rssi: &NSNumber,
        ) {
            self.callback_boundary("delegate_centralmanager_diddiscoverperipheral_advertisementdata_rssi", || {
                trace!(
                    "delegate_centralmanager_diddiscoverperipheral_advertisementdata_rssi {}",
                    peripheral_debug(peripheral)
                );

                let advertisement_name = adv_data
                    .objectForKey(unsafe { CBAdvertisementDataLocalNameKey })
                    .and_then(|name| name.downcast::<NSString>().ok())
                    .and_then(|name| unsafe { nsstring_to_string(&*name as *const NSString) });

                let Some(cbperipheral) = (unsafe { Retained::retain(peripheral as *const _ as *mut _) }) else {
                    warn!("Advertisement peripheral could not be retained");
                    return;
                };
                self.send_event(CentralDelegateEvent::DiscoveredPeripheral {
                    cbperipheral,
                    advertisement_name,
                });

                let rssi_value = rssi.as_i16();

                let id = unsafe { peripheral.identifier() };
                let peripheral_uuid = nsuuid_to_uuid(&id);

                if let Some(value) = adv_data.objectForKey(unsafe { CBAdvertisementDataManufacturerDataKey }) {
                    match advertisement_manufacturer_data(&value) {
                        Ok((manufacturer_id, data)) => self.send_event(CentralDelegateEvent::ManufacturerData {
                            peripheral_uuid, manufacturer_id, data, rssi: rssi_value,
                        }),
                        Err(error) => warn!("Invalid manufacturer advertisement data: {error}"),
                    }
                }
                if let Some(value) = adv_data.objectForKey(unsafe { CBAdvertisementDataServiceDataKey }) {
                    match advertisement_service_data(&value) {
                        Ok(service_data) => self.send_event(CentralDelegateEvent::ServiceData {
                            peripheral_uuid, service_data, rssi: rssi_value,
                        }),
                        Err(error) => warn!("Invalid service advertisement data: {error}"),
                    }
                }
                if let Some(value) = adv_data.objectForKey(unsafe { CBAdvertisementDataServiceUUIDsKey }) {
                    match advertisement_services(&value) {
                        Ok(service_uuids) => self.send_event(CentralDelegateEvent::Services {
                            peripheral_uuid, service_uuids, rssi: rssi_value,
                        }),
                        Err(error) => warn!("Invalid advertised services: {error}"),
                    }
                }
                if let Some(value) = adv_data.objectForKey(unsafe { CBAdvertisementDataTxPowerLevelKey }) {
                    match advertisement_tx_power(&value) {
                        Ok(tx_power_level) => self.send_event(CentralDelegateEvent::TxPowerLevel {
                            peripheral_uuid, tx_power_level,
                        }),
                        Err(error) => warn!("Invalid advertised TX power: {error}"),
                    }
                }
            });
        }
    }

    unsafe impl CBPeripheralDelegate for CentralDelegate {
        #[unsafe(method(peripheral:didDiscoverServices:))]
        fn delegate_peripheral_diddiscoverservices(
            &self,
            peripheral: &CBPeripheral,
            error: Option<&NSError>,
        ) {
            self.callback_boundary("delegate_peripheral_diddiscoverservices", || {
                trace!(
                    "delegate_peripheral_diddiscoverservices {} {}",
                    peripheral_debug(peripheral),
                    localized_description(error)
                );
                let mut service_map = HashMap::new();
                if error.is_none() {
                    let services = unsafe { peripheral.services() }.unwrap_or_default();
                    for s in services {
                        // go ahead and ask for characteristics and other services
                        unsafe {
                            peripheral.discoverCharacteristics_forService(None, &s);
                            peripheral.discoverIncludedServices_forService(None, &s);
                        }

                        // Create the map entry we'll need to export.
                        let raw_uuid = unsafe { s.UUID() };
                        let uuid = cbuuid_to_uuid(&raw_uuid);
                        service_map.insert(uuid, s);
                    }
                }
                let id = unsafe { peripheral.identifier() };
                let peripheral_uuid = nsuuid_to_uuid(&id);
                self.send_event(CentralDelegateEvent::DiscoveredServices {
                    peripheral_uuid,
                    services: service_map,
                    error: error.map(super::native_error::NativeError::copy),
                });
            });
        }

        #[unsafe(method(peripheral:didDiscoverIncludedServicesForService:error:))]
        fn delegate_peripheral_diddiscoverincludedservicesforservice_error(
            &self,
            peripheral: &CBPeripheral,
            service: &CBService,
            error: Option<&NSError>,
        ) {
            self.callback_boundary("delegate_peripheral_diddiscoverincludedservicesforservice_error", || {
                trace!(
                    "delegate_peripheral_diddiscoverincludedservicesforservice_error {} {} {}",
                    peripheral_debug(peripheral),
                    service_debug(service),
                    localized_description(error)
                );
                if error.is_none() {
                    let includes = unsafe { service.includedServices() }.unwrap_or_default();
                    for s in includes {
                        unsafe { peripheral.discoverCharacteristics_forService(None, &s) };
                    }
                }
            });
        }

        #[unsafe(method(peripheral:didDiscoverCharacteristicsForService:error:))]
        fn delegate_peripheral_diddiscovercharacteristicsforservice_error(
            &self,
            peripheral: &CBPeripheral,
            service: &CBService,
            error: Option<&NSError>,
        ) {
            self.callback_boundary("delegate_peripheral_diddiscovercharacteristicsforservice_error", || {
                trace!(
                    "delegate_peripheral_diddiscovercharacteristicsforservice_error {} {} {}",
                    peripheral_debug(peripheral),
                    service_debug(service),
                    localized_description(error)
                );
                // Report errors so the owning discovery request fails explicitly.
                let mut characteristics = HashMap::new();
                if error.is_some() {
                    warn!(
                        "Error discovering characteristics for service {}, failing discovery: {}",
                        service_debug(service),
                        localized_description(error)
                    );
                } else {
                    let chars = unsafe { service.characteristics() }.unwrap_or_default();
                    for c in chars {
                        unsafe { peripheral.discoverDescriptorsForCharacteristic(&c) };
                        // Create the map entry we'll need to export.
                        let raw_uuid = unsafe { c.UUID() };
                        let uuid = cbuuid_to_uuid(&raw_uuid);
                        characteristics.insert(uuid, c);
                    }
                }
                let id = unsafe { peripheral.identifier() };
                let peripheral_uuid = nsuuid_to_uuid(&id);
                let raw_service_uuid = unsafe { service.UUID() };
                let service_uuid = cbuuid_to_uuid(&raw_service_uuid);
                self.send_event(CentralDelegateEvent::DiscoveredCharacteristics {
                    peripheral_uuid,
                    service_uuid,
                    characteristics,
                    error: error.map(super::native_error::NativeError::copy),
                });
            });
        }

        #[unsafe(method(peripheral:didDiscoverDescriptorsForCharacteristic:error:))]
        fn delegate_peripheral_diddiscoverdescriptorsforcharacteristic_error(
            &self,
            peripheral: &CBPeripheral,
            characteristic: &CBCharacteristic,
            error: Option<&NSError>,
        ) {
            self.callback_boundary("delegate_peripheral_diddiscoverdescriptorsforcharacteristic_error", || {
                trace!(
                    "delegate_peripheral_diddiscoverdescriptorsforcharacteristic_error {} {} {}",
                    peripheral_debug(peripheral),
                    characteristic_debug(characteristic),
                    localized_description(error)
                );
                // Send the event even on error when the characteristic is associated
                // with a service, so discover_services() can complete.
                let mut descriptors = HashMap::new();
                if error.is_some() {
                    warn!(
                        "Error discovering descriptors for characteristic {}, failing discovery: {}",
                        characteristic_debug(characteristic),
                        localized_description(error)
                    );
                }
                if error.is_none() {
                    let descs = unsafe { characteristic.descriptors() }.unwrap_or_default();
                    for d in descs {
                        let raw_uuid = unsafe { d.UUID() };
                        let uuid = cbuuid_to_uuid(&raw_uuid);
                        descriptors.insert(uuid, d);
                    }
                }
                let id = unsafe { peripheral.identifier() };
                let peripheral_uuid = nsuuid_to_uuid(&id);
                let Some(service) = (unsafe { characteristic.service() }) else {
                    warn!(
                        "Descriptor discovery completed for characteristic {} without an associated service",
                        characteristic_debug(characteristic)
                    );
                    return;
                };
                let raw_service_uuid = unsafe { service.UUID() };
                let service_uuid = cbuuid_to_uuid(&raw_service_uuid);
                let raw_char_uuid = unsafe { characteristic.UUID() };
                let characteristic_uuid = cbuuid_to_uuid(&raw_char_uuid);
                self.send_event(CentralDelegateEvent::DiscoveredCharacteristicDescriptors {
                    peripheral_uuid,
                    service_uuid,
                    characteristic_uuid,
                    descriptors,
                    error: error.map(super::native_error::NativeError::copy),
                });
            });
        }

        #[unsafe(method(peripheral:didUpdateValueForCharacteristic:error:))]
        fn delegate_peripheral_didupdatevalueforcharacteristic_error(
            &self,
            peripheral: &CBPeripheral,
            characteristic: &CBCharacteristic,
            error: Option<&NSError>,
        ) {
            self.callback_boundary("delegate_peripheral_didupdatevalueforcharacteristic_error", || {
                trace!(
                    "delegate_peripheral_didupdatevalueforcharacteristic_error {} {} {}",
                    peripheral_debug(peripheral),
                    characteristic_debug(characteristic),
                    localized_description(error)
                );
                let Some(service) = (unsafe { characteristic.service() }) else {
                    warn!(
                        "Characteristic value update for {} has no associated service",
                        characteristic_debug(characteristic)
                    );
                    return;
                };
                let id = unsafe { peripheral.identifier() };
                let peripheral_uuid = nsuuid_to_uuid(&id);
                let raw_service_uuid = unsafe { service.UUID() };
                let service_uuid = cbuuid_to_uuid(&raw_service_uuid);
                let raw_char_uuid = unsafe { characteristic.UUID() };
                let characteristic_uuid = cbuuid_to_uuid(&raw_char_uuid);
                self.send_event(CentralDelegateEvent::CharacteristicNotified {
                    peripheral_uuid,
                    service_uuid,
                    characteristic_uuid,
                    data: get_characteristic_value(characteristic),
                    error: error.map(super::native_error::NativeError::copy),
                });
            });
        }

        #[unsafe(method(peripheral:didWriteValueForCharacteristic:error:))]
        fn delegate_peripheral_didwritevalueforcharacteristic_error(
            &self,
            peripheral: &CBPeripheral,
            characteristic: &CBCharacteristic,
            error: Option<&NSError>,
        ) {
            self.callback_boundary("delegate_peripheral_didwritevalueforcharacteristic_error", || {
                trace!(
                    "delegate_peripheral_didwritevalueforcharacteristic_error {} {} {}",
                    peripheral_debug(peripheral),
                    characteristic_debug(characteristic),
                    localized_description(error)
                );
                let Some(service) = (unsafe { characteristic.service() }) else {
                    warn!("Write callback has no service");
                    return;
                };
                self.send_event(CentralDelegateEvent::CharacteristicWritten {
                    peripheral_uuid: nsuuid_to_uuid(&*unsafe { peripheral.identifier() }),
                    service_uuid: cbuuid_to_uuid(&*unsafe { service.UUID() }),
                    characteristic_uuid: cbuuid_to_uuid(&*unsafe { characteristic.UUID() }),
                    error: error.map(super::native_error::NativeError::copy),
                });
            });
        }

        #[unsafe(method(peripheral:didUpdateNotificationStateForCharacteristic:error:))]
        fn delegate_peripheral_didupdatenotificationstateforcharacteristic_error(
            &self,
            peripheral: &CBPeripheral,
            characteristic: &CBCharacteristic,
            error: Option<&NSError>,
        ) {
            self.callback_boundary("delegate_peripheral_didupdatenotificationstateforcharacteristic_error", || {
                trace!("delegate_peripheral_didupdatenotificationstateforcharacteristic_error");
                let id = unsafe { peripheral.identifier() };
                let peripheral_uuid = nsuuid_to_uuid(&id);
                let Some(service) = (unsafe { characteristic.service() }) else {
                    warn!(
                        "Notification state update for {} has no associated service",
                        characteristic_debug(characteristic)
                    );
                    return;
                };
                let raw_service_uuid = unsafe { service.UUID() };
                let service_uuid = cbuuid_to_uuid(&raw_service_uuid);
                let raw_char_uuid = unsafe { characteristic.UUID() };
                let characteristic_uuid = cbuuid_to_uuid(&raw_char_uuid);
                // The callback only reports the outcome of a setNotifyValue
                // request; isNotifying describes the current state and does not
                // identify the requested direction, so the event carries no
                // direction information.
                self.send_event(CentralDelegateEvent::CharacteristicNotificationStateUpdated {
                    peripheral_uuid,
                    service_uuid,
                    characteristic_uuid,
                    // Preserve the error. Only the owning request's explicit
                    // policy can tolerate this precise missing-CCCD condition.
                    missing_cccd: error.is_some_and(|e|
                        unsafe { characteristic.properties() }.contains(CBCharacteristicProperties::Notify)
                        && e.domain().to_string() == "CBATTErrorDomain" && e.code() == 10),
                    error: error.map(super::native_error::NativeError::copy),
                });
            });
        }

        #[unsafe(method(peripheral:didReadRSSI:error:))]
        fn delegate_peripheral_didreadrssi_error(
            &self,
            peripheral: &CBPeripheral,
            rssi: &NSNumber,
            error: Option<&NSError>,
        ) {
            self.callback_boundary("delegate_peripheral_didreadrssi_error", || {
                trace!(
                    "delegate_peripheral_didreadrssi_error {}",
                    peripheral_debug(peripheral)
                );
                let id = unsafe { peripheral.identifier() };
                let peripheral_uuid = nsuuid_to_uuid(&id);
                let rssi_value = rssi.as_i16();
                self.send_event(CentralDelegateEvent::DidReadRssi {
                    peripheral_uuid,
                    rssi: rssi_value,
                    error: error.map(super::native_error::NativeError::copy),
                });
            });
        }

        #[unsafe(method(peripheral:didUpdateValueForDescriptor:error:))]
        fn delegate_peripheral_didupdatevaluefordescriptor_error(
            &self,
            peripheral: &CBPeripheral,
            descriptor: &CBDescriptor,
            error: Option<&NSError>,
        ) {
            self.callback_boundary("delegate_peripheral_didupdatevaluefordescriptor_error", || {
                trace!(
                    "delegate_peripheral_didupdatevaluefordescriptor_error {} {} {}",
                    peripheral_debug(peripheral),
                    descriptor_debug(descriptor),
                    localized_description(error)
                );
                let Some(characteristic) = (unsafe { descriptor.characteristic() }) else {
                    warn!("Descriptor read callback has no characteristic");
                    return;
                };
                let Some(service) = (unsafe { characteristic.service() }) else {
                    warn!("Descriptor read callback has no service");
                    return;
                };
                let result = match error {
                    Some(error) => Err(super::native_error::NativeError::copy(error)),
                    None => get_descriptor_value(descriptor).map_err(super::native_error::NativeError::local),
                };
                let (data, error) = match result {
                    Ok(data) => (data, None),
                    Err(error) => (Vec::new(), Some(error)),
                };
                self.send_event(CentralDelegateEvent::DescriptorNotified {
                    peripheral_uuid: nsuuid_to_uuid(&*unsafe { peripheral.identifier() }),
                    service_uuid: cbuuid_to_uuid(&*unsafe { service.UUID() }),
                    characteristic_uuid: cbuuid_to_uuid(&*unsafe { characteristic.UUID() }),
                    descriptor_uuid: cbuuid_to_uuid(&*unsafe { descriptor.UUID() }),
                    data,
                    error,
                });
            });
        }

        #[unsafe(method(peripheral:didWriteValueForDescriptor:error:))]
        fn delegate_peripheral_didwritevaluefordescriptor_error(
            &self,
            peripheral: &CBPeripheral,
            descriptor: &CBDescriptor,
            error: Option<&NSError>,
        ) {
            self.callback_boundary("delegate_peripheral_didwritevaluefordescriptor_error", || {
                trace!(
                    "delegate_peripheral_didwritevaluefordescriptor_error {} {} {}",
                    peripheral_debug(peripheral),
                    descriptor_debug(descriptor),
                    localized_description(error)
                );
                let Some(characteristic) = (unsafe { descriptor.characteristic() }) else {
                    warn!("Descriptor write callback has no characteristic");
                    return;
                };
                let Some(service) = (unsafe { characteristic.service() }) else {
                    warn!("Write callback has no service");
                    return;
                };
                self.send_event(CentralDelegateEvent::DescriptorWritten {
                    peripheral_uuid: nsuuid_to_uuid(&*unsafe { peripheral.identifier() }),
                    service_uuid: cbuuid_to_uuid(&*unsafe { service.UUID() }),
                    characteristic_uuid: cbuuid_to_uuid(&*unsafe { characteristic.UUID() }),
                    descriptor_uuid: cbuuid_to_uuid(&*unsafe { descriptor.UUID() }),
                    error: error.map(super::native_error::NativeError::copy),
                });
            });
        }

        #[unsafe(method(peripheral:didModifyServices:))]
        fn delegate_peripheral_didmodifyservices(
            &self,
            peripheral: &CBPeripheral,
            invalidated_services: &NSArray<CBService>,
        ) {
            self.callback_boundary("delegate_peripheral_didmodifyservices", || {
                trace!(
                    "delegate_peripheral_didmodifyservices {}",
                    peripheral_debug(peripheral),
                );
                // This is a corebluetooth-only event that makes peripheral services unusable until discovery has been performed again.
                // https://developer.apple.com/documentation/corebluetooth/cbperipheraldelegate/peripheral(_:didmodifyservices:)?language=objc
                // Trigger the removal of internal corebluetooth peripheral discovered services. It is also expected that
                // discover_services() will be performed again on the peripheral at the API level as soon as is practical.
                let invalidated_service_uuids: Vec<Uuid> = invalidated_services
                    .iter()
                    .map(|s| {
                        let raw_uuid = unsafe { s.UUID() };
                        cbuuid_to_uuid(&raw_uuid)
                    })
                    .collect();
                let id = unsafe { peripheral.identifier() };
                let peripheral_uuid = nsuuid_to_uuid(&id);
                self.send_event(CentralDelegateEvent::ServicesModified {
                    peripheral_uuid,
                    invalidated_services: invalidated_service_uuids,
                });
            });
        }

        #[unsafe(method(peripheralIsReadyToSendWriteWithoutResponse:))]
        fn delegate_peripheral_is_ready_to_send_write_without_response(
            &self,
            peripheral: &CBPeripheral,
        ) {
            self.callback_boundary("delegate_peripheral_is_ready_to_send_write_without_response", || {
                trace!(
                    "delegate_peripheral_is_ready_to_send_write_without_response {}",
                    peripheral_debug(peripheral)
                );
                let id = unsafe { peripheral.identifier() };
                let peripheral_uuid = nsuuid_to_uuid(&id);
                self.send_event(CentralDelegateEvent::ReadyToSendWriteWithoutResponse {
                    peripheral_uuid,
                });
            });
        }
    }
);

impl CentralDelegate {
    pub fn new(sender: Sender<CentralDelegateEvent>) -> Retained<Self> {
        let this = CentralDelegate::alloc().set_ivars(sender);
        unsafe { msg_send![super(this), init] }
    }

    fn callback_boundary(&self, callback: &'static str, work: impl FnOnce()) {
        let result = super::callback_boundary::invoke(
            || {
                work();
                Ok::<(), ()>(())
            },
            || (),
        );
        if result.is_err() {
            // Report a terminal failure through the owned event path. Receiver
            // closure during shutdown is safe; send_event already handles it.
            let reported = super::callback_boundary::invoke(
                || {
                    self.send_event(CentralDelegateEvent::CallbackFailed { callback });
                    Ok::<(), ()>(())
                },
                || (),
            );
            if reported.is_err() {
                // A second panic while reporting cannot be delivered safely.
                // Do not let either panic escape through Objective-C.
                std::process::abort();
            }
        }
    }

    fn send_event(&self, event: CentralDelegateEvent) {
        let mut sender = self.ivars().clone();
        futures::executor::block_on(async {
            if let Err(e) = sender.send(event).await {
                error!("Error sending delegate event: {}", e);
            }
        });
    }
}

fn localized_description(error: Option<&NSError>) -> String {
    if let Some(error) = error {
        error.localizedDescription().to_string()
    } else {
        "".to_string()
    }
}

fn get_characteristic_value(characteristic: &CBCharacteristic) -> Vec<u8> {
    trace!("Getting data!");
    let v = unsafe { characteristic.value() }.map(|value| value.to_vec());
    trace!("BluetoothGATTCharacteristic::get_value -> {:?}", v);
    v.unwrap_or_default()
}

fn advertisement_manufacturer_data(value: &AnyObject) -> Result<(u16, Vec<u8>), &'static str> {
    let data = value
        .downcast_ref::<NSData>()
        .ok_or("Expected NSData")?
        .to_vec();
    let [low, high, payload @ ..] = data.as_slice() else {
        return Err("Missing two-byte manufacturer identifier");
    };
    Ok((u16::from_le_bytes([*low, *high]), payload.to_vec()))
}

fn advertisement_tx_power(value: &AnyObject) -> Result<i16, &'static str> {
    value
        .downcast_ref::<NSNumber>()
        .map(NSNumber::as_i16)
        .ok_or("Expected NSNumber")
}

fn advertisement_service_data(value: &AnyObject) -> Result<HashMap<Uuid, Vec<u8>>, &'static str> {
    // Erase container generics first: checking NSDictionary alone does not prove
    // the runtime classes of its keys or values.
    let dictionary = value
        .downcast_ref::<NSDictionary>()
        .ok_or("Expected NSDictionary")?;
    let mut result = HashMap::new();
    for key in dictionary.keys() {
        let uuid = key
            .downcast_ref::<CBUUID>()
            .ok_or("Expected CBUUID service-data key")?;
        let value = dictionary
            .objectForKey(&key)
            .ok_or("Missing service-data value")?;
        let data = value
            .downcast_ref::<NSData>()
            .ok_or("Expected NSData service-data value")?;
        if result.insert(cbuuid_to_uuid(uuid), data.to_vec()).is_some() {
            return Err("Duplicate canonical service-data UUID");
        }
    }
    Ok(result)
}

fn advertisement_services(value: &AnyObject) -> Result<Vec<Uuid>, &'static str> {
    let array = value.downcast_ref::<NSArray>().ok_or("Expected NSArray")?;
    array
        .iter()
        .map(|value| {
            let uuid = value
                .downcast_ref::<CBUUID>()
                .ok_or("Expected CBUUID service-list entry")?;
            Ok(cbuuid_to_uuid(uuid))
        })
        .collect()
}

#[cfg(test)]
mod advertisement_value_tests {
    use super::*;

    #[test]
    fn manufacturer_identifier_is_little_endian_and_payload_is_copied() {
        let value = NSData::with_bytes(&[0x34, 0x12, 0, 255, 128]);
        assert_eq!(
            advertisement_manufacturer_data(&value),
            Ok((0x1234, vec![0, 255, 128]))
        );
        let empty = NSData::with_bytes(&[0, 0]);
        assert_eq!(advertisement_manufacturer_data(&empty), Ok((0, Vec::new())));
    }

    #[test]
    fn malformed_manufacturer_data_and_tx_power_are_controlled_errors() {
        for bytes in [vec![], vec![1]] {
            let value = NSData::with_bytes(&bytes);
            assert!(advertisement_manufacturer_data(&value).is_err());
        }
        let wrong = NSString::from_str("wrong type");
        assert!(advertisement_manufacturer_data(&wrong).is_err());
        assert!(advertisement_tx_power(&wrong).is_err());
        let power = NSNumber::new_i16(-12);
        assert_eq!(advertisement_tx_power(&power), Ok(-12));
    }

    #[test]
    fn service_containers_validate_runtime_entries_and_preserve_bytes() {
        let uuid = unsafe { CBUUID::UUIDWithString(&NSString::from_str("180F")) };
        let data = NSData::with_bytes(&[0, 255]);
        let dictionary = NSDictionary::<CBUUID, NSData>::from_slices(&[&*uuid], &[&data]);
        let expected = cbuuid_to_uuid(&uuid);
        assert_eq!(
            advertisement_service_data(&dictionary),
            Ok(HashMap::from([(expected, vec![0, 255])]))
        );
        let array = NSArray::from_slice(&[&*uuid]);
        assert_eq!(advertisement_services(&array), Ok(vec![expected]));
        let empty = NSArray::<AnyObject>::from_slice(&[]);
        assert_eq!(advertisement_services(&empty), Ok(Vec::new()));
    }

    #[test]
    fn wrong_container_key_value_and_list_entry_types_are_errors() {
        let wrong = NSString::from_str("wrong type");
        assert!(advertisement_service_data(&wrong).is_err());
        assert!(advertisement_services(&wrong).is_err());
        let bad_keys = NSDictionary::<NSString, NSString>::from_slices(&[&*wrong], &[&wrong]);
        assert!(advertisement_service_data(&bad_keys).is_err());
        let uuid = unsafe { CBUUID::UUIDWithString(&NSString::from_str("180F")) };
        let bad_values = NSDictionary::<CBUUID, NSString>::from_slices(&[&*uuid], &[&wrong]);
        assert!(advertisement_service_data(&bad_values).is_err());
        let bad_list = NSArray::from_slice(&[&*wrong]);
        assert!(advertisement_services(&bad_list).is_err());
    }
}

fn get_descriptor_value(descriptor: &CBDescriptor) -> Result<Vec<u8>, String> {
    let value = unsafe { descriptor.value() };
    descriptor_value_bytes(value.as_deref())
}

fn descriptor_value_bytes(value: Option<&AnyObject>) -> Result<Vec<u8>, String> {
    let value = value.ok_or_else(|| "Descriptor read returned no value".to_string())?;
    // Foundation class clusters may use private subclasses. Runtime checked
    // casts recognize those subclasses without assuming their class names.
    if let Some(value) = value.downcast_ref::<NSString>() {
        return Ok(value.to_string().into_bytes());
    }
    if let Some(value) = value.downcast_ref::<NSData>() {
        return Ok(value.to_vec());
    }
    if let Some(value) = value.downcast_ref::<NSNumber>() {
        return Ok(value.stringValue().to_string().into_bytes());
    }
    Err(format!(
        "Unsupported descriptor value class: {:?}",
        value.class()
    ))
}

#[cfg(test)]
mod descriptor_value_tests {
    use super::*;

    #[test]
    fn foundation_string_and_number_values_preserve_existing_bytes() {
        let text = NSString::from_str("descriptor λ");
        assert_eq!(
            descriptor_value_bytes(Some(&text)),
            Ok("descriptor λ".as_bytes().to_vec())
        );
        let number = NSNumber::new_i16(-42);
        assert_eq!(descriptor_value_bytes(Some(&number)), Ok(b"-42".to_vec()));
    }

    #[test]
    fn foundation_data_values_copy_arbitrary_and_empty_bytes() {
        let data = NSData::with_bytes(&[0, 255, 128, 1]);
        assert_eq!(
            descriptor_value_bytes(Some(&data)),
            Ok(vec![0, 255, 128, 1])
        );
        let empty = NSData::with_bytes(&[]);
        assert_eq!(descriptor_value_bytes(Some(&empty)), Ok(Vec::new()));
    }

    #[test]
    fn absent_value_is_an_error_instead_of_successful_empty_data() {
        assert_eq!(
            descriptor_value_bytes(None),
            Err("Descriptor read returned no value".into())
        );
    }

    #[test]
    fn unsupported_foundation_object_is_an_error_instead_of_successful_empty_data() {
        let object = NSObject::new();
        let error = descriptor_value_bytes(Some(&object)).unwrap_err();
        assert!(error.contains("Unsupported descriptor value class"));
        assert!(error.contains("NSObject"));
    }
}

fn peripheral_debug(peripheral: &CBPeripheral) -> String {
    let uuid = unsafe { peripheral.identifier() }.UUIDString();
    match unsafe { peripheral.name() } {
        Some(name) => {
            format!("CBPeripheral({}, {})", name, uuid)
        }
        _ => {
            format!("CBPeripheral({})", uuid)
        }
    }
}

fn service_debug(service: &CBService) -> String {
    let uuid = unsafe { service.UUID().UUIDString() };
    format!("CBService({})", uuid)
}

fn characteristic_debug(characteristic: &CBCharacteristic) -> String {
    let uuid = unsafe { characteristic.UUID().UUIDString() };
    format!("CBCharacteristic({})", uuid)
}

fn descriptor_debug(descriptor: &CBDescriptor) -> String {
    let uuid = unsafe { descriptor.UUID().UUIDString() };
    format!("CBDescriptor({})", uuid)
}

#[cfg(test)]
mod callback_policy_tests {
    use super::*;
    use futures::channel::mpsc;

    define_class!(
        #[unsafe(super(NSObject))]
        #[thread_kind = AnyThread]
        #[ivars = Retained<CentralDelegate>]
        struct CallbackProbe;
        unsafe impl NSObjectProtocol for CallbackProbe {}
        impl CallbackProbe {
            #[unsafe(method(probePanic))]
            fn probe_panic(&self) {
                self.ivars().callback_boundary("probe_panic", || panic!("injected callback panic"));
            }
            #[unsafe(method(probePayloadPanic))]
            fn probe_payload_panic(&self) {
                struct Payload;
                impl Drop for Payload {
                    fn drop(&mut self) { panic!("injected payload destructor panic"); }
                }
                self.ivars().callback_boundary("probe_payload_panic", || std::panic::panic_any(Payload));
            }
        }
    );

    #[test]
    fn objective_c_callback_panics_return_and_publish_terminal_cause() {
        for (selector, name) in [
            (objc2::sel!(probePanic), "probe_panic"),
            (objc2::sel!(probePayloadPanic), "probe_payload_panic"),
        ] {
            let (sender, mut receiver) = mpsc::channel(1);
            let delegate = CentralDelegate::new(sender);
            let this = CallbackProbe::alloc().set_ivars(delegate);
            let probe: Retained<CallbackProbe> = unsafe { msg_send![super(this), init] };
            // Use Objective-C dynamic dispatch, including its generated foreign
            // trampoline, rather than calling the Rust policy method directly.
            unsafe {
                objc2::runtime::MessageReceiver::send_message::<(), ()>(&*probe, selector, ());
            }
            assert!(
                matches!(receiver.try_recv(), Ok(CentralDelegateEvent::CallbackFailed { callback }) if callback == name)
            );
            assert!(receiver.try_recv().is_err());
        }
    }
}
