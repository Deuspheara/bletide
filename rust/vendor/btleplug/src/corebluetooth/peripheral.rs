// btleplug Source Code File
//
// Copyright 2020 Nonpolynomial Labs LLC. All rights reserved.
//
// Licensed under the BSD 3-Clause license. See LICENSE file in the project root
// for full license information.

use super::internal::{
    CoreBluetoothMessage, CoreBluetoothReply, CoreBluetoothReplyFuture, PeripheralEventInternal,
};
use super::tasks::Tasks;
use crate::{
    Error, Result,
    api::{
        self, BDAddr, CentralEvent, CharPropFlags, Characteristic, Descriptor,
        PeripheralProperties, Service, ValueNotification, WriteType,
    },
    common::{adapter_manager::AdapterManager, util::notifications_stream_from_broadcast_receiver},
};
use async_trait::async_trait;
use futures::channel::mpsc::{Receiver, SendError, Sender};
use futures::sink::SinkExt;
use futures::stream::{Stream, StreamExt};
use log::*;
use objc2_core_bluetooth::CBPeripheralState;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "serde")]
use serde_cr as serde;
use std::sync::Weak;
use std::{
    collections::{BTreeSet, HashMap},
    fmt::{self, Debug, Display, Formatter},
    pin::Pin,
    sync::{Arc, Mutex, atomic::AtomicU16},
};
use tokio::sync::broadcast;
use uuid::Uuid;

#[cfg_attr(
    feature = "serde",
    derive(Serialize, Deserialize),
    serde(crate = "serde_cr")
)]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PeripheralId(Uuid);

impl Display for PeripheralId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// Implementation of [api::Peripheral](crate::api::Peripheral).
#[derive(Clone)]
pub struct Peripheral {
    shared: Arc<Shared>,
}

struct Shared {
    notifications_channel: broadcast::Sender<ValueNotification>,
    manager: Weak<AdapterManager<Peripheral>>,
    uuid: Uuid,
    services: Mutex<BTreeSet<Service>>,
    properties: Mutex<PeripheralProperties>,
    message_sender: Sender<CoreBluetoothMessage>,
    mtu: AtomicU16,
    // We're not actually holding a peripheral object here, that's held out in
    // the objc thread. We'll just communicate with it through our
    // receiver/sender pair.
}

impl Shared {
    fn emit_event(&self, event: CentralEvent) {
        match self.manager.upgrade() {
            Some(manager) => {
                manager.emit(event);
            }
            _ => {
                trace!("Could not emit an event. AdapterManager has been dropped");
            }
        }
    }
}

impl Peripheral {
    async fn set_notifications_compat(
        &self,
        characteristic: &Characteristic,
        enabled: bool,
    ) -> Result<()> {
        if !characteristic.properties.contains(CharPropFlags::NOTIFY) {
            return Err(Error::NotSupported(
                "Nonstandard CCCD policy requires notify".into(),
            ));
        }
        let fut = CoreBluetoothReplyFuture::default();
        self.shared
            .message_sender
            .to_owned()
            .send(CoreBluetoothMessage::SetNotificationsWithCompatibility {
                peripheral_uuid: self.shared.uuid,
                service_uuid: characteristic.service_uuid,
                characteristic_uuid: characteristic.uuid,
                enabled,
                future: fut.get_state_clone(),
            })
            .await?;
        match fut.await {
            CoreBluetoothReply::Ok => Ok(()),
            CoreBluetoothReply::NativeErr(error) => Err(Error::Other(Box::new(error))),
            CoreBluetoothReply::Err(msg) => Err(Error::RuntimeError(msg)),
            _ => Err(Error::RuntimeError(
                "Unexpected notification setup reply".into(),
            )),
        }
    }

    // This calls tokio::task::spawn, so it must be called from the context of a Tokio Runtime.
    pub(super) fn new(
        uuid: Uuid,
        local_name: Option<String>,
        advertisement_name: Option<String>,
        manager: Weak<AdapterManager<Self>>,
        event_receiver: Receiver<PeripheralEventInternal>,
        message_sender: Sender<CoreBluetoothMessage>,
        tasks: &Tasks,
    ) -> Self {
        // Since we're building the object, we have an active advertisement.
        // Build properties now.
        let properties = Mutex::from(PeripheralProperties {
            address: BDAddr::default(),
            address_type: None,
            local_name,
            advertisement_name,
            appearance: None,
            tx_power_level: None,
            rssi: None,
            manufacturer_data: HashMap::new(),
            service_data: HashMap::new(),
            services: Vec::new(),
            class: None,
        });
        let (notifications_channel, _) = broadcast::channel(16);

        let shared = Arc::new(Shared {
            properties,
            manager,
            services: Mutex::new(BTreeSet::new()),
            notifications_channel,
            uuid,
            message_sender,
            mtu: AtomicU16::new(crate::api::DEFAULT_MTU_SIZE),
        });
        let shared_clone = shared.clone();
        tasks.spawn(async move {
            let mut event_receiver = event_receiver;
            let shared = shared_clone;

            loop {
                match event_receiver.next().await {
                    Some(PeripheralEventInternal::Notification(uuid, service_uuid, data)) => {
                        let notification = ValueNotification {
                            uuid,
                            service_uuid,
                            value: data,
                        };

                        // Note: we ignore send errors here which may happen while there are no
                        // receivers...
                        let _ = shared.notifications_channel.send(notification);
                    }
                    Some(PeripheralEventInternal::ManufacturerData(
                        manufacturer_id,
                        data,
                        rssi,
                    )) => {
                        let mut properties = shared.properties.lock().unwrap();
                        properties.rssi = Some(rssi);
                        properties
                            .manufacturer_data
                            .insert(manufacturer_id, data.clone());
                        shared.emit_event(CentralEvent::ManufacturerDataAdvertisement {
                            id: shared.uuid.into(),
                            manufacturer_data: properties.manufacturer_data.clone(),
                        });
                    }
                    Some(PeripheralEventInternal::ServiceData(service_data, rssi)) => {
                        let mut properties = shared.properties.lock().unwrap();
                        properties.rssi = Some(rssi);
                        properties.service_data.extend(service_data.clone());

                        shared.emit_event(CentralEvent::ServiceDataAdvertisement {
                            id: shared.uuid.into(),
                            service_data,
                        });
                    }
                    Some(PeripheralEventInternal::Services(services, rssi)) => {
                        let mut properties = shared.properties.lock().unwrap();
                        properties.rssi = Some(rssi);
                        properties.services = services.clone();

                        shared.emit_event(CentralEvent::ServicesAdvertisement {
                            id: shared.uuid.into(),
                            services,
                        });
                    }
                    Some(PeripheralEventInternal::ServicesModified) => {
                        shared.services.lock().unwrap().clear();
                        shared.emit_event(CentralEvent::DeviceServicesModified(shared.uuid.into()));
                    }
                    Some(PeripheralEventInternal::TxPowerLevel(tx_power_level)) => {
                        let mut properties = shared.properties.lock().unwrap();
                        properties.tx_power_level = Some(tx_power_level);
                    }
                    Some(PeripheralEventInternal::RssiRead(rssi)) => {
                        shared.emit_event(CentralEvent::RssiUpdate {
                            id: shared.uuid.into(),
                            rssi,
                        });
                    }
                    None => {
                        info!("Event receiver died, breaking out of corebluetooth device loop.");
                        break;
                    }
                }
            }
        });
        Self { shared }
    }

    pub(super) fn update_name(
        &self,
        local_name: Option<String>,
        advertisement_name: Option<String>,
    ) {
        if let Ok(mut props) = self.shared.properties.lock() {
            let PeripheralProperties {
                local_name: current_local_name,
                advertisement_name: current_advertisement_name,
                ..
            } = &mut *props;
            merge_names(
                current_local_name,
                current_advertisement_name,
                local_name,
                advertisement_name,
            );
        }
    }
}

fn merge_names(
    local_name: &mut Option<String>,
    advertisement_name: &mut Option<String>,
    new_local_name: Option<String>,
    new_advertisement_name: Option<String>,
) {
    if let Some(name) = new_advertisement_name {
        *local_name = Some(name.clone());
        *advertisement_name = Some(name);
    } else if advertisement_name.is_none()
        && let Some(name) = new_local_name
    {
        *local_name = Some(name);
    }
}

impl Display for Peripheral {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        // let connected = if self.is_connected() { " connected" } else { "" };
        // let properties = self.properties.lock().unwrap();
        // write!(f, "{} {}{}", self.address, properties.local_name.clone()
        //     .unwrap_or_else(|| "(unknown)".to_string()), connected)
        write!(f, "Peripheral")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advertisement_name_takes_precedence_over_gap_name() {
        let mut local_name = Some("Longer GAP name".to_string());
        let mut advertisement_name = None;

        merge_names(
            &mut local_name,
            &mut advertisement_name,
            Some("Short GAP".to_string()),
            Some("Complete".to_string()),
        );

        assert_eq!(local_name.as_deref(), Some("Complete"));
        assert_eq!(advertisement_name.as_deref(), Some("Complete"));
    }

    #[test]
    fn absent_advertisement_does_not_erase_or_override_it() {
        let mut local_name = Some("Complete".to_string());
        let mut advertisement_name = Some("Complete".to_string());

        merge_names(
            &mut local_name,
            &mut advertisement_name,
            Some("Different GAP name".to_string()),
            None,
        );

        assert_eq!(local_name.as_deref(), Some("Complete"));
        assert_eq!(advertisement_name.as_deref(), Some("Complete"));
    }

    /// Drive the public Peripheral subscribe/unsubscribe path with a fake
    /// message receiver that refuses the request, so the real reply future
    /// resolves through the production error mapping.
    async fn refused_notification_request_maps_to_runtime_error(
        enabled: bool,
        options: Option<api::SubscriptionOptions>,
        native: bool,
    ) {
        use futures::channel::mpsc;
        use std::time::Duration;

        const TIMEOUT: Duration = Duration::from_secs(2);

        let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
        let service_uuid = Uuid::from_u128(0x0000180f_0000_1000_8000_00805f9b34fb);
        let characteristic_uuid = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
        let (message_sender, mut message_receiver) = mpsc::channel(1);
        let (_event_sender, event_receiver) = mpsc::channel(1);
        let peripheral = Peripheral::new(
            peripheral_uuid,
            None,
            None,
            Weak::<AdapterManager<Peripheral>>::new(),
            event_receiver,
            message_sender,
            &Tasks::default(),
        );
        let characteristic = Characteristic {
            uuid: characteristic_uuid,
            service_uuid,
            properties: CharPropFlags::NOTIFY,
            descriptors: Default::default(),
        };

        let task_peripheral = peripheral.clone();
        let task_characteristic = characteristic.clone();
        let request = tokio::spawn(async move {
            match (enabled, options) {
                (true, Some(options)) => {
                    api::Peripheral::subscribe_with_options(
                        &task_peripheral,
                        &task_characteristic,
                        options,
                    )
                    .await
                }
                (false, Some(options)) => {
                    api::Peripheral::unsubscribe_with_options(
                        &task_peripheral,
                        &task_characteristic,
                        options,
                    )
                    .await
                }
                (true, None) => {
                    api::Peripheral::subscribe(&task_peripheral, &task_characteristic).await
                }
                (false, None) => {
                    api::Peripheral::unsubscribe(&task_peripheral, &task_characteristic).await
                }
            }
        });

        let message = tokio::time::timeout(TIMEOUT, message_receiver.next())
            .await
            .expect("notification request did not send a message")
            .expect("message channel closed");
        assert!(!request.is_finished(), "setup must await the native reply");
        let compat =
            options.is_some_and(|options| options.setup_mode == api::NotificationSetupMode::Compat);
        let future = match message {
            CoreBluetoothMessage::Subscribe { future, .. } if enabled && !compat => future,
            CoreBluetoothMessage::Unsubscribe { future, .. } if !enabled && !compat => future,
            CoreBluetoothMessage::SetNotificationsWithCompatibility {
                future,
                enabled: actual,
                ..
            } if compat => {
                assert_eq!(actual, enabled);
                future
            }
            message => panic!("unexpected message: {message:?}"),
        };

        let error_text = "The operation couldn’t be completed. (ATT error 15.)".to_string();
        crate::corebluetooth::future::set_reply(
            &future,
            if native {
                CoreBluetoothReply::NativeErr(super::super::native_error::NativeError {
                    message: error_text.clone(),
                    native_code: Some("CBATTErrorDomain:15".into()),
                })
            } else {
                CoreBluetoothReply::Err(error_text.clone())
            },
        );

        let result = tokio::time::timeout(TIMEOUT, request)
            .await
            .expect("notification request did not complete")
            .expect("notification request task panicked");
        match result {
            Err(error) if native => {
                assert_eq!(error.native_code().as_deref(), Some("CBATTErrorDomain:15"));
                assert_eq!(error.to_string(), format!("Runtime Error: {error_text}"));
            }
            Err(Error::RuntimeError(actual)) => assert_eq!(actual, error_text),
            result => panic!("unexpected notification request result: {result:?}"),
        }
    }

    #[tokio::test]
    async fn refused_subscribe_maps_to_public_runtime_error() {
        refused_notification_request_maps_to_runtime_error(true, None, false).await;
    }

    #[tokio::test]
    async fn refused_unsubscribe_maps_to_public_runtime_error() {
        refused_notification_request_maps_to_runtime_error(false, None, false).await;
    }

    #[tokio::test]
    async fn notification_native_errors_survive_standard_and_compat_replies() {
        for options in [
            None,
            Some(api::SubscriptionOptions::default()),
            Some(api::SubscriptionOptions {
                setup_mode: api::NotificationSetupMode::Compat,
            }),
        ] {
            for enabled in [true, false] {
                refused_notification_request_maps_to_runtime_error(enabled, options, true).await;
            }
        }
    }

    #[tokio::test]
    async fn options_preserve_standard_setup_and_compat_error_readiness() {
        for options in [
            api::SubscriptionOptions::default(),
            api::SubscriptionOptions {
                setup_mode: api::NotificationSetupMode::Compat,
            },
        ] {
            for enabled in [true, false] {
                refused_notification_request_maps_to_runtime_error(enabled, Some(options), false)
                    .await;
            }
        }
    }

    async fn exercise_lifecycle_reply(query: bool, reply: CoreBluetoothReply) {
        use futures::channel::mpsc;
        use std::time::Duration;
        let uuid = Uuid::from_u128(789);
        let manager = Arc::new(AdapterManager::<Peripheral>::default());
        let tasks = Tasks::default();
        let (sender, mut receiver) = mpsc::channel(1);
        let (_events, events) = mpsc::channel(1);
        let peripheral = Peripheral::new(
            uuid,
            None,
            None,
            Arc::downgrade(&manager),
            events,
            sender,
            &tasks,
        );
        manager.add_peripheral(peripheral.clone());
        let successful_disconnect = !query && matches!(reply, CoreBluetoothReply::Ok);
        let expected_cause = match &reply {
            CoreBluetoothReply::Err(cause) => Some(cause.clone()),
            _ => None,
        };
        let expected_state = match &reply {
            CoreBluetoothReply::State(state) if query => {
                Some(*state == CBPeripheralState::Connected)
            }
            _ => None,
        };
        let worker = tokio::spawn(async move {
            if query {
                api::Peripheral::is_connected(&peripheral).await
            } else {
                api::Peripheral::disconnect(&peripheral)
                    .await
                    .map(|()| false)
            }
        });
        let message = tokio::time::timeout(Duration::from_secs(2), receiver.next())
            .await
            .unwrap()
            .unwrap();
        let future = match message {
            CoreBluetoothMessage::IsConnected { future, .. } if query => future,
            CoreBluetoothMessage::DisconnectDevice { future, .. } if !query => future,
            other => panic!("Unexpected lifecycle command: {other:?}"),
        };
        crate::corebluetooth::future::set_reply(&future, reply);
        let result = tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .unwrap();
        tasks.close().await.unwrap();
        assert_eq!(
            manager.peripheral(&uuid.into()).is_none(),
            successful_disconnect,
            "Only an acknowledged disconnect may evict the cached peripheral"
        );
        let result = result.expect("A platform reply must not panic");
        if let Some(cause) = expected_cause {
            assert!(matches!(result, Err(Error::RuntimeError(actual)) if actual == cause));
        } else if let Some(state) = expected_state {
            assert_eq!(result.unwrap(), state);
        } else if successful_disconnect {
            assert!(!result.unwrap());
        } else {
            assert!(
                matches!(result, Err(Error::RuntimeError(_))),
                "Unexpected replies must be errors"
            );
        }
    }

    #[tokio::test]
    async fn disconnect_reply_preserves_failure_and_cache_until_ack() {
        for reply in [
            CoreBluetoothReply::Err("native disconnect failed".into()),
            CoreBluetoothReply::ReadResult(vec![0xaa]),
            CoreBluetoothReply::Ok,
        ] {
            exercise_lifecycle_reply(false, reply).await;
        }
    }

    #[tokio::test]
    async fn connection_state_reply_returns_original_error_instead_of_panicking() {
        for reply in [
            CoreBluetoothReply::Err("executor closed".into()),
            CoreBluetoothReply::Ok,
            CoreBluetoothReply::State(CBPeripheralState::Connected),
            CoreBluetoothReply::State(CBPeripheralState::Disconnected),
        ] {
            exercise_lifecycle_reply(true, reply).await;
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum GattOperation {
        Connect,
        Discover,
        Read,
        Write,
        Subscribe,
        Unsubscribe,
        Rssi,
        ReadDescriptor,
        WriteDescriptor,
    }

    async fn exercise_gatt_reply(operation: GattOperation, reply: CoreBluetoothReply) {
        use std::time::Duration;
        let tasks = Tasks::default();
        let (sender, mut receiver) = futures::channel::mpsc::channel(1);
        let (_events, events) = futures::channel::mpsc::channel(1);
        let peripheral = Peripheral::new(
            Uuid::from_u128(790),
            None,
            None,
            Weak::<AdapterManager<Peripheral>>::new(),
            events,
            sender,
            &tasks,
        );
        let characteristic = Characteristic {
            uuid: Uuid::from_u128(791),
            service_uuid: Uuid::from_u128(792),
            properties: CharPropFlags::READ | CharPropFlags::WRITE | CharPropFlags::NOTIFY,
            descriptors: Default::default(),
        };
        let descriptor = Descriptor {
            uuid: Uuid::from_u128(794),
            service_uuid: characteristic.service_uuid,
            characteristic_uuid: characteristic.uuid,
        };
        // Failed/unexpected replies must not publish discovery or reset MTU.
        peripheral
            .shared
            .mtu
            .store(185, std::sync::atomic::Ordering::Relaxed);
        let cached_service = Service {
            uuid: Uuid::from_u128(793),
            primary: true,
            characteristics: BTreeSet::new(),
        };
        peripheral
            .shared
            .services
            .lock()
            .unwrap()
            .insert(cached_service.clone());
        let observed = peripheral.clone();
        let cause = match &reply {
            CoreBluetoothReply::Err(cause) => Some(cause.clone()),
            CoreBluetoothReply::NativeErr(error) => Some(error.message.clone()),
            _ => None,
        };
        let native = match &reply {
            CoreBluetoothReply::NativeErr(error) => error.native_code.clone(),
            _ => None,
        };
        let unexpected = matches!(reply, CoreBluetoothReply::State(_));
        let failed = cause.is_some() || unexpected;
        let worker = tokio::spawn(async move {
            match operation {
                GattOperation::Connect => {
                    api::Peripheral::connect(&peripheral).await.map(|()| vec![])
                }
                GattOperation::Discover => api::Peripheral::discover_services(&peripheral)
                    .await
                    .map(|()| vec![]),
                GattOperation::Read => api::Peripheral::read(&peripheral, &characteristic).await,
                GattOperation::Write => api::Peripheral::write(
                    &peripheral,
                    &characteristic,
                    &[0xaa],
                    WriteType::WithResponse,
                )
                .await
                .map(|()| vec![]),
                GattOperation::Subscribe => {
                    api::Peripheral::subscribe(&peripheral, &characteristic)
                        .await
                        .map(|()| vec![])
                }
                GattOperation::Unsubscribe => {
                    api::Peripheral::unsubscribe(&peripheral, &characteristic)
                        .await
                        .map(|()| vec![])
                }
                GattOperation::ReadDescriptor => {
                    api::Peripheral::read_descriptor(&peripheral, &descriptor).await
                }
                GattOperation::WriteDescriptor => {
                    api::Peripheral::write_descriptor(&peripheral, &descriptor, &[0xaa])
                        .await
                        .map(|()| vec![])
                }
                GattOperation::Rssi => api::Peripheral::read_rssi(&peripheral)
                    .await
                    .map(|rssi| rssi.to_le_bytes().to_vec()),
            }
        });
        let command = tokio::time::timeout(Duration::from_secs(2), receiver.next())
            .await
            .unwrap()
            .unwrap();
        let future = match (operation, command) {
            (GattOperation::Connect, CoreBluetoothMessage::ConnectDevice { future, .. })
            | (GattOperation::Discover, CoreBluetoothMessage::DiscoverServices { future, .. })
            | (GattOperation::Read, CoreBluetoothMessage::ReadValue { future, .. })
            | (GattOperation::Write, CoreBluetoothMessage::WriteValue { future, .. })
            | (GattOperation::Subscribe, CoreBluetoothMessage::Subscribe { future, .. })
            | (GattOperation::Unsubscribe, CoreBluetoothMessage::Unsubscribe { future, .. })
            | (GattOperation::Rssi, CoreBluetoothMessage::ReadRssi { future, .. })
            | (
                GattOperation::ReadDescriptor,
                CoreBluetoothMessage::ReadDescriptorValue { future, .. },
            )
            | (
                GattOperation::WriteDescriptor,
                CoreBluetoothMessage::WriteDescriptorValue { future, .. },
            ) => future,
            other => panic!("Unexpected GATT command: {other:?}"),
        };
        crate::corebluetooth::future::set_reply(&future, reply);
        let joined = tokio::time::timeout(Duration::from_secs(2), worker)
            .await
            .unwrap();
        tasks.close().await.unwrap();
        let result = joined.expect("GATT replies must return a Result without panicking");
        if let Some(native) = native {
            let error = result.expect_err("native error must fail the awaited operation");
            assert_eq!(error.native_code().as_deref(), Some(native.as_str()));
            assert_eq!(
                error.to_string(),
                format!("Runtime Error: {}", cause.unwrap())
            );
        } else if let Some(cause) = cause {
            assert!(matches!(result, Err(Error::RuntimeError(actual)) if actual == cause));
        } else if unexpected {
            assert!(matches!(result, Err(Error::RuntimeError(_))));
        } else {
            let expected = match operation {
                GattOperation::Read | GattOperation::ReadDescriptor => vec![0xaa, 0xbb],
                GattOperation::Rssi => (-67_i16).to_le_bytes().to_vec(),
                _ => vec![],
            };
            assert_eq!(result.unwrap(), expected);
        }
        let expected_mtu = match (failed, operation) {
            (false, GattOperation::Connect) => api::DEFAULT_MTU_SIZE,
            (false, GattOperation::Discover) => 247,
            _ => 185,
        };
        assert_eq!(api::Peripheral::mtu(&observed), expected_mtu);
        let expected_services = if !failed && matches!(operation, GattOperation::Discover) {
            BTreeSet::new()
        } else {
            BTreeSet::from([cached_service])
        };
        assert_eq!(api::Peripheral::services(&observed), expected_services);
    }

    #[tokio::test]
    async fn owned_gatt_native_errors_reach_awaited_backend_operations() {
        for operation in [
            GattOperation::Connect,
            GattOperation::Discover,
            GattOperation::Read,
            GattOperation::Write,
            GattOperation::ReadDescriptor,
            GattOperation::WriteDescriptor,
            GattOperation::Rssi,
        ] {
            let native = objc2_foundation::NSError::new(
                15,
                &objc2_foundation::NSString::from_str("CBATTErrorDomain"),
            );
            let copied = super::super::native_error::NativeError::copy(&native);
            drop(native);
            exercise_gatt_reply(operation, CoreBluetoothReply::NativeErr(copied)).await;
        }
    }

    macro_rules! gatt_reply_regression {
        ($name:ident, $operation:ident, $success:expr) => {
            #[tokio::test]
            async fn $name() {
                for reply in [
                    CoreBluetoothReply::Err("native GATT failure".into()),
                    CoreBluetoothReply::State(CBPeripheralState::Disconnected),
                    $success,
                ] {
                    exercise_gatt_reply(GattOperation::$operation, reply).await;
                }
            }
        };
    }
    gatt_reply_regression!(
        descriptor_read_reply_is_fallible,
        ReadDescriptor,
        CoreBluetoothReply::ReadResult(vec![0xaa, 0xbb])
    );
    gatt_reply_regression!(
        descriptor_write_reply_is_fallible,
        WriteDescriptor,
        CoreBluetoothReply::Ok
    );
    gatt_reply_regression!(
        connect_reply_is_fallible,
        Connect,
        CoreBluetoothReply::Connected
    );
    gatt_reply_regression!(
        discovery_reply_is_fallible,
        Discover,
        CoreBluetoothReply::ServicesDiscovered(BTreeSet::new(), 247)
    );
    gatt_reply_regression!(
        read_reply_is_fallible,
        Read,
        CoreBluetoothReply::ReadResult(vec![0xaa, 0xbb])
    );
    gatt_reply_regression!(write_reply_is_fallible, Write, CoreBluetoothReply::Ok);
    gatt_reply_regression!(
        subscribe_reply_is_fallible,
        Subscribe,
        CoreBluetoothReply::Ok
    );
    gatt_reply_regression!(
        unsubscribe_reply_is_fallible,
        Unsubscribe,
        CoreBluetoothReply::Ok
    );
    gatt_reply_regression!(
        rssi_reply_is_fallible,
        Rssi,
        CoreBluetoothReply::ReadRssi(-67)
    );

    #[test]
    fn gap_name_is_used_until_an_advertisement_name_arrives() {
        let mut local_name = None;
        let mut advertisement_name = None;

        merge_names(
            &mut local_name,
            &mut advertisement_name,
            Some("GAP name".to_string()),
            None,
        );

        assert_eq!(local_name.as_deref(), Some("GAP name"));
        assert_eq!(advertisement_name, None);
    }
}

impl Debug for Peripheral {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("Peripheral")
            .field("uuid", &self.shared.uuid)
            .field("services", &self.shared.services)
            .field("properties", &self.shared.properties)
            .field("message_sender", &self.shared.message_sender)
            .finish()
    }
}

#[async_trait]
impl api::Peripheral for Peripheral {
    fn id(&self) -> PeripheralId {
        PeripheralId(self.shared.uuid)
    }

    fn address(&self) -> BDAddr {
        BDAddr::default()
    }

    fn mtu(&self) -> u16 {
        self.shared.mtu.load(std::sync::atomic::Ordering::Relaxed)
    }

    async fn properties(&self) -> Result<Option<PeripheralProperties>> {
        Ok(Some(
            self.shared
                .properties
                .lock()
                .map_err(Into::<Error>::into)?
                .clone(),
        ))
    }

    fn services(&self) -> BTreeSet<Service> {
        self.shared.services.lock().unwrap().clone()
    }

    async fn is_connected(&self) -> Result<bool> {
        let fut = CoreBluetoothReplyFuture::default();
        self.shared
            .message_sender
            .to_owned()
            .send(CoreBluetoothMessage::IsConnected {
                peripheral_uuid: self.shared.uuid,
                future: fut.get_state_clone(),
            })
            .await?;
        match fut.await {
            CoreBluetoothReply::State(state) => match state {
                CBPeripheralState::Connected => Ok(true),
                _ => Ok(false),
            },
            CoreBluetoothReply::Err(message) => Err(Error::RuntimeError(message)),
            _ => Err(Error::RuntimeError(
                "Unexpected connection state reply".into(),
            )),
        }
    }

    async fn connect(&self) -> Result<()> {
        let fut = CoreBluetoothReplyFuture::default();
        self.shared
            .message_sender
            .to_owned()
            .send(CoreBluetoothMessage::ConnectDevice {
                peripheral_uuid: self.shared.uuid,
                future: fut.get_state_clone(),
            })
            .await?;
        match fut.await {
            CoreBluetoothReply::Connected => {
                self.shared
                    .mtu
                    .store(api::DEFAULT_MTU_SIZE, std::sync::atomic::Ordering::Relaxed);
                self.shared
                    .emit_event(CentralEvent::DeviceConnected(self.shared.uuid.into()));
            }
            CoreBluetoothReply::NativeErr(error) => return Err(Error::Other(Box::new(error))),
            CoreBluetoothReply::Err(msg) => return Err(Error::RuntimeError(msg)),
            _ => return Err(Error::RuntimeError("Unexpected connect reply".into())),
        }
        trace!("Device connected!");
        Ok(())
    }

    async fn disconnect(&self) -> Result<()> {
        let fut = CoreBluetoothReplyFuture::default();
        self.shared
            .message_sender
            .to_owned()
            .send(CoreBluetoothMessage::DisconnectDevice {
                peripheral_uuid: self.shared.uuid,
                future: fut.get_state_clone(),
            })
            .await?;
        match fut.await {
            CoreBluetoothReply::Ok => {
                self.shared
                    .emit_event(CentralEvent::DeviceDisconnected(self.shared.uuid.into()));
                trace!("Device disconnected!");
            }
            CoreBluetoothReply::Err(message) => return Err(Error::RuntimeError(message)),
            _ => return Err(Error::RuntimeError("Unexpected disconnect reply".into())),
        }
        Ok(())
    }

    async fn discover_services(&self) -> Result<()> {
        let fut = CoreBluetoothReplyFuture::default();
        self.shared
            .message_sender
            .to_owned()
            .send(CoreBluetoothMessage::DiscoverServices {
                peripheral_uuid: self.shared.uuid,
                future: fut.get_state_clone(),
            })
            .await?;
        match fut.await {
            CoreBluetoothReply::ServicesDiscovered(services, mtu) => {
                *(self.shared.services.lock().map_err(Into::<Error>::into)?) = services;
                self.shared
                    .mtu
                    .store(mtu, std::sync::atomic::Ordering::Relaxed);
                return Ok(());
            }
            CoreBluetoothReply::NativeErr(error) => return Err(Error::Other(Box::new(error))),
            CoreBluetoothReply::Err(msg) => return Err(Error::RuntimeError(msg)),
            _ => Err(Error::RuntimeError(
                "Unexpected service discovery reply".into(),
            )),
        }
    }

    async fn write(
        &self,
        characteristic: &Characteristic,
        data: &[u8],
        mut write_type: WriteType,
    ) -> Result<()> {
        let fut = CoreBluetoothReplyFuture::default();
        // If we get WriteWithoutResponse for a characteristic that only
        // supports WriteWithResponse, slam the type to WriteWithResponse.
        // Otherwise we won't handle the future correctly.
        if write_type == WriteType::WithoutResponse
            && !characteristic
                .properties
                .contains(CharPropFlags::WRITE_WITHOUT_RESPONSE)
        {
            write_type = WriteType::WithResponse
        }
        self.shared
            .message_sender
            .to_owned()
            .send(CoreBluetoothMessage::WriteValue {
                peripheral_uuid: self.shared.uuid,
                service_uuid: characteristic.service_uuid,
                characteristic_uuid: characteristic.uuid,
                data: Vec::from(data),
                write_type,
                future: fut.get_state_clone(),
            })
            .await?;
        match fut.await {
            CoreBluetoothReply::Ok => {}
            CoreBluetoothReply::NativeErr(error) => return Err(Error::Other(Box::new(error))),
            CoreBluetoothReply::Err(msg) => return Err(Error::RuntimeError(msg)),
            _ => return Err(Error::RuntimeError("Unexpected write reply".into())),
        }
        Ok(())
    }

    async fn read(&self, characteristic: &Characteristic) -> Result<Vec<u8>> {
        let fut = CoreBluetoothReplyFuture::default();
        self.shared
            .message_sender
            .to_owned()
            .send(CoreBluetoothMessage::ReadValue {
                peripheral_uuid: self.shared.uuid,
                service_uuid: characteristic.service_uuid,
                characteristic_uuid: characteristic.uuid,
                future: fut.get_state_clone(),
            })
            .await?;
        match fut.await {
            CoreBluetoothReply::ReadResult(chars) => Ok(chars),
            CoreBluetoothReply::NativeErr(error) => return Err(Error::Other(Box::new(error))),
            CoreBluetoothReply::Err(msg) => return Err(Error::RuntimeError(msg)),
            _ => Err(Error::RuntimeError("Unexpected read reply".into())),
        }
    }

    async fn subscribe_with_options(
        &self,
        characteristic: &Characteristic,
        options: api::SubscriptionOptions,
    ) -> Result<()> {
        match options.setup_mode {
            api::NotificationSetupMode::Standard => self.subscribe(characteristic).await,
            api::NotificationSetupMode::Compat => {
                self.set_notifications_compat(characteristic, true).await
            }
        }
    }

    async fn unsubscribe_with_options(
        &self,
        characteristic: &Characteristic,
        options: api::SubscriptionOptions,
    ) -> Result<()> {
        match options.setup_mode {
            api::NotificationSetupMode::Standard => self.unsubscribe(characteristic).await,
            api::NotificationSetupMode::Compat => {
                self.set_notifications_compat(characteristic, false).await
            }
        }
    }

    async fn subscribe(&self, characteristic: &Characteristic) -> Result<()> {
        let fut = CoreBluetoothReplyFuture::default();
        self.shared
            .message_sender
            .to_owned()
            .send(CoreBluetoothMessage::Subscribe {
                peripheral_uuid: self.shared.uuid,
                service_uuid: characteristic.service_uuid,
                characteristic_uuid: characteristic.uuid,
                future: fut.get_state_clone(),
            })
            .await?;
        match fut.await {
            CoreBluetoothReply::Ok => trace!("subscribed!"),
            CoreBluetoothReply::NativeErr(error) => return Err(Error::Other(Box::new(error))),
            CoreBluetoothReply::Err(msg) => return Err(Error::RuntimeError(msg)),
            _ => return Err(Error::RuntimeError("Unexpected subscribe reply".into())),
        }
        Ok(())
    }

    async fn unsubscribe(&self, characteristic: &Characteristic) -> Result<()> {
        let fut = CoreBluetoothReplyFuture::default();
        self.shared
            .message_sender
            .to_owned()
            .send(CoreBluetoothMessage::Unsubscribe {
                peripheral_uuid: self.shared.uuid,
                service_uuid: characteristic.service_uuid,
                characteristic_uuid: characteristic.uuid,
                future: fut.get_state_clone(),
            })
            .await?;
        match fut.await {
            CoreBluetoothReply::Ok => {}
            CoreBluetoothReply::NativeErr(error) => return Err(Error::Other(Box::new(error))),
            CoreBluetoothReply::Err(msg) => return Err(Error::RuntimeError(msg)),
            _ => return Err(Error::RuntimeError("Unexpected unsubscribe reply".into())),
        }
        Ok(())
    }

    async fn notification_results(
        &self,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<ValueNotification>> + Send>>> {
        Ok(
            crate::common::util::notification_results_from_broadcast_receiver(
                self.shared.notifications_channel.subscribe(),
            ),
        )
    }

    async fn notifications(&self) -> Result<Pin<Box<dyn Stream<Item = ValueNotification> + Send>>> {
        let receiver = self.shared.notifications_channel.subscribe();
        Ok(notifications_stream_from_broadcast_receiver(receiver))
    }

    async fn write_descriptor(&self, descriptor: &Descriptor, data: &[u8]) -> Result<()> {
        let fut = CoreBluetoothReplyFuture::default();
        self.shared
            .message_sender
            .to_owned()
            .send(CoreBluetoothMessage::WriteDescriptorValue {
                peripheral_uuid: self.shared.uuid,
                service_uuid: descriptor.service_uuid,
                characteristic_uuid: descriptor.characteristic_uuid,
                descriptor_uuid: descriptor.uuid,
                data: Vec::from(data),
                future: fut.get_state_clone(),
            })
            .await?;
        match fut.await {
            CoreBluetoothReply::Ok => {}
            CoreBluetoothReply::NativeErr(error) => return Err(Error::Other(Box::new(error))),
            CoreBluetoothReply::Err(msg) => return Err(Error::RuntimeError(msg)),
            reply => {
                return Err(Error::RuntimeError(format!(
                    "Unexpected reply: {:?}",
                    reply
                )));
            }
        }
        Ok(())
    }

    async fn read_rssi(&self) -> Result<i16> {
        let fut = CoreBluetoothReplyFuture::default();
        self.shared
            .message_sender
            .to_owned()
            .send(CoreBluetoothMessage::ReadRssi {
                peripheral_uuid: self.shared.uuid,
                future: fut.get_state_clone(),
            })
            .await?;
        match fut.await {
            CoreBluetoothReply::ReadRssi(rssi) => Ok(rssi),
            CoreBluetoothReply::NativeErr(error) => return Err(Error::Other(Box::new(error))),
            CoreBluetoothReply::Err(msg) => Err(Error::RuntimeError(msg)),
            _ => Err(Error::RuntimeError("Unexpected RSSI reply".into())),
        }
    }

    async fn read_descriptor(&self, descriptor: &Descriptor) -> Result<Vec<u8>> {
        let fut = CoreBluetoothReplyFuture::default();
        self.shared
            .message_sender
            .to_owned()
            .send(CoreBluetoothMessage::ReadDescriptorValue {
                peripheral_uuid: self.shared.uuid,
                service_uuid: descriptor.service_uuid,
                characteristic_uuid: descriptor.characteristic_uuid,
                descriptor_uuid: descriptor.uuid,
                future: fut.get_state_clone(),
            })
            .await?;
        match fut.await {
            CoreBluetoothReply::ReadResult(chars) => Ok(chars),
            CoreBluetoothReply::NativeErr(error) => return Err(Error::Other(Box::new(error))),
            CoreBluetoothReply::Err(msg) => Err(Error::RuntimeError(msg)),
            _ => Err(Error::RuntimeError(
                "Unexpected reply for descriptor read".into(),
            )),
        }
    }
}

impl From<Uuid> for PeripheralId {
    fn from(uuid: Uuid) -> Self {
        PeripheralId(uuid)
    }
}

impl From<SendError> for Error {
    fn from(_: SendError) -> Self {
        Error::Other("Channel closed".to_string().into())
    }
}
