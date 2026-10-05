use super::*;
use futures::StreamExt;
use objc2::{DefinedClass, define_class};
use objc2_core_bluetooth::{
    CBAdvertisementDataManufacturerDataKey, CBAttributePermissions, CBCentralManagerDelegate,
    CBMutableCharacteristic, CBMutableDescriptor, CBMutableService, CBPeripheralDelegate,
};
use objc2_foundation::{NSError, NSObjectProtocol, NSString, ns_string};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::Poll;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq)]
struct RecordedSetNotify {
    characteristic_uuid: Uuid,
    enabled: bool,
}

struct TestPeripheralState {
    identifier: Retained<NSUUID>,
    set_notify_calls: Mutex<Vec<RecordedSetNotify>>,
    state: Mutex<CBPeripheralState>,
}

define_class!(
    #[unsafe(super(CBPeripheral))]
    #[thread_kind = AnyThread]
    #[ivars = TestPeripheralState]
    struct TestPeripheral;

    unsafe impl NSObjectProtocol for TestPeripheral {}

    impl TestPeripheral {
        #[unsafe(method_id(identifier))]
        fn identifier(&self) -> Retained<NSUUID> {
            self.ivars().identifier.clone()
        }

        #[unsafe(method_id(name))]
        fn name(&self) -> Option<Retained<NSString>> {
            None
        }

        #[unsafe(method(state))]
        fn controlled_state(&self) -> CBPeripheralState {
            *self.ivars().state.lock().unwrap()
        }

        #[unsafe(method(maximumWriteValueLengthForType:))]
        fn maximum_write_value_length_for_type(
            &self,
            _write_type: CBCharacteristicWriteType,
        ) -> usize {
            0
        }

        #[unsafe(method(setNotifyValue:forCharacteristic:))]
        fn set_notify_value_for_characteristic(
            &self,
            enabled: bool,
            characteristic: &CBCharacteristic,
        ) {
            let raw_uuid = unsafe { characteristic.UUID() };
            self.ivars()
                .set_notify_calls
                .lock()
                .unwrap()
                .push(RecordedSetNotify {
                    characteristic_uuid: cbuuid_to_uuid(&raw_uuid),
                    enabled,
                });
        }
    }
);

impl TestPeripheral {
    fn new(identifier: Retained<NSUUID>) -> Retained<Self> {
        let this = Self::alloc().set_ivars(TestPeripheralState {
            identifier,
            set_notify_calls: Mutex::new(Vec::new()),
            state: Mutex::new(CBPeripheralState::Disconnected),
        });
        unsafe { msg_send![super(this), init] }
    }

    fn set_notify_calls(&self) -> Vec<RecordedSetNotify> {
        self.ivars().set_notify_calls.lock().unwrap().clone()
    }
}

define_class!(
    #[unsafe(super(CBMutableCharacteristic))]
    #[thread_kind = AnyThread]
    #[ivars = AtomicBool]
    struct TestCharacteristic;

    unsafe impl NSObjectProtocol for TestCharacteristic {}

    impl TestCharacteristic {
        #[unsafe(method(isNotifying))]
        fn is_notifying(&self) -> bool {
            self.ivars().load(Ordering::SeqCst)
        }
    }
);

impl TestCharacteristic {
    fn new(uuid: &CBUUID, properties: CBCharacteristicProperties) -> Retained<Self> {
        let this = Self::alloc().set_ivars(AtomicBool::new(false));
        unsafe {
            msg_send![super(this),
                initWithType: uuid,
                properties: properties,
                value: None::<&NSData>,
                permissions: CBAttributePermissions::Readable
            ]
        }
    }
}

const CALLBACK_TIMEOUT: Duration = Duration::from_secs(2);

struct FixtureCharacteristic {
    uuid: Uuid,
    characteristic: Retained<TestCharacteristic>,
}

struct NotificationFixture {
    peripheral_uuid: Uuid,
    service_uuid: Uuid,
    peripheral: Retained<TestPeripheral>,
    internal: PeripheralInternal,
    delegate: Retained<CentralDelegate>,
    delegate_receiver: Receiver<CentralDelegateEvent>,
    characteristics: Vec<FixtureCharacteristic>,
}

impl NotificationFixture {
    fn new(characteristic_uuids: &[Uuid]) -> Self {
        Self::with_properties(
            characteristic_uuids,
            CBCharacteristicProperties::Notify | CBCharacteristicProperties::Read,
        )
    }
    fn with_properties(
        characteristic_uuids: &[Uuid],
        properties: CBCharacteristicProperties,
    ) -> Self {
        let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
        let service_uuid = Uuid::from_u128(0x0000180f_0000_1000_8000_00805f9b34fb);
        let peripheral_uuid_string = NSString::from_str(&peripheral_uuid.to_string());
        let peripheral_identifier =
            NSUUID::initWithUUIDString(NSUUID::alloc(), &peripheral_uuid_string)
                .expect("valid peripheral UUID");
        let peripheral = TestPeripheral::new(peripheral_identifier);
        // Permanently retain the test peripheral so the fixture can drop
        // at any point, including during a panicking assertion, without
        // ever running CBPeripheral's private destruction path.
        std::mem::forget(peripheral.clone());

        let characteristics: Vec<FixtureCharacteristic> = characteristic_uuids
            .iter()
            .map(|&uuid| {
                let characteristic_cbuuid = uuid_to_cbuuid(uuid);
                let characteristic = TestCharacteristic::new(&characteristic_cbuuid, properties);
                FixtureCharacteristic {
                    uuid,
                    characteristic,
                }
            })
            .collect();
        let native_characteristics = characteristics
            .iter()
            .map(|fixture_characteristic| {
                Retained::into_super(Retained::into_super(
                    fixture_characteristic.characteristic.clone(),
                ))
            })
            .collect::<Vec<Retained<CBCharacteristic>>>();

        let service_cbuuid = uuid_to_cbuuid(service_uuid);
        let service = unsafe {
            CBMutableService::initWithType_primary(CBMutableService::alloc(), &service_cbuuid, true)
        };
        let attached_characteristics = NSArray::from_retained_slice(&native_characteristics);
        unsafe { service.setCharacteristics(Some(&attached_characteristics)) };

        let (event_sender, _) = mpsc::channel(1);
        let mut internal =
            PeripheralInternal::new(Retained::into_super(peripheral.clone()), event_sender);
        internal.services.insert(
            service_uuid,
            ServiceInternal {
                cbservice: Retained::into_super(service),
                characteristics: native_characteristics
                    .into_iter()
                    .zip(characteristic_uuids)
                    .map(|(characteristic, &uuid)| {
                        (uuid, CharacteristicInternal::new(characteristic))
                    })
                    .collect(),
                discovered: true,
            },
        );

        let (delegate_sender, delegate_receiver) = mpsc::channel(4);
        let delegate = CentralDelegate::new(delegate_sender);

        Self {
            peripheral_uuid,
            service_uuid,
            peripheral,
            internal,
            delegate,
            delegate_receiver,
            characteristics,
        }
    }

    fn set_notify_calls(&self) -> Vec<RecordedSetNotify> {
        self.peripheral.set_notify_calls()
    }
}

/// Run the real Objective-C notification-state delegate method, drain the
/// resulting delegate event from the channel, and dispatch it through the
/// production handler. Returns the callback's localized error text, if any.
async fn deliver_notification_state_callback(
    fixture: &mut NotificationFixture,
    characteristic_index: usize,
    error: Option<&NSError>,
    context: &str,
) -> Option<String> {
    let peripheral = &fixture.peripheral;
    let characteristic = &fixture.characteristics[characteristic_index].characteristic;
    unsafe {
        fixture
            .delegate
            .peripheral_didUpdateNotificationStateForCharacteristic_error(
                peripheral,
                characteristic,
                error,
            );
    }
    let event = tokio::time::timeout(CALLBACK_TIMEOUT, fixture.delegate_receiver.next())
        .await
        .unwrap_or_else(|_| panic!("{context}: notification state callback did not emit an event"))
        .unwrap_or_else(|| panic!("{context}: delegate event channel closed"));
    let CentralDelegateEvent::CharacteristicNotificationStateUpdated {
        peripheral_uuid,
        service_uuid,
        characteristic_uuid,
        error: event_error,
        missing_cccd,
    } = event
    else {
        panic!("{context}: unexpected delegate event: {event:?}");
    };
    assert_eq!(peripheral_uuid, fixture.peripheral_uuid, "{context}");
    assert_eq!(service_uuid, fixture.service_uuid, "{context}");
    assert_eq!(
        characteristic_uuid, fixture.characteristics[characteristic_index].uuid,
        "{context}"
    );
    let expected_error = error.map(|error| error.localizedDescription().to_string());
    assert_eq!(
        event_error.as_ref().map(|error| error.message.clone()),
        expected_error,
        "{context}"
    );
    assert_eq!(
        event_error
            .as_ref()
            .and_then(|error| error.native_code.clone()),
        error.map(|error| format!("{}:{}", error.domain(), error.code())),
        "{context}"
    );
    fixture.internal.on_notification_state_updated(
        service_uuid,
        characteristic_uuid,
        event_error.clone(),
        missing_cccd,
    );
    event_error.map(|error| error.message)
}

async fn assert_pending(future: &mut CoreBluetoothReplyFuture, context: &str) {
    assert!(
        matches!(futures::poll!(future), Poll::Pending),
        "{context}: future unexpectedly completed"
    );
}

fn notification_error(att_error: bool) -> Retained<NSError> {
    if att_error {
        // Error code 15 mirrors the ATT "Insufficient Encryption" refusal
        // from the issue report.
        NSError::new(15, ns_string!("ATT"))
    } else {
        NSError::new(1, ns_string!("BtlePlugCoreBluetoothTests"))
    }
}

#[test]
fn maximum_write_value_length_is_converted_to_att_mtu() {
    assert_eq!(maximum_write_value_length_to_att_mtu(20), Ok(23));
    assert_eq!(maximum_write_value_length_to_att_mtu(512), Ok(515));
    assert_eq!(
        maximum_write_value_length_to_att_mtu(u16::MAX as usize - 3),
        Ok(u16::MAX)
    );
}

#[test]
fn zero_maximum_write_value_length_uses_default_mtu() {
    assert_eq!(
        maximum_write_value_length_to_att_mtu(0),
        Ok(crate::api::DEFAULT_MTU_SIZE)
    );
}

#[test]
fn unrepresentable_maximum_write_value_length_is_rejected() {
    assert!(maximum_write_value_length_to_att_mtu(u16::MAX as usize).is_err());
    assert!(maximum_write_value_length_to_att_mtu(usize::MAX).is_err());
}

#[tokio::test]
async fn characteristic_discovery_error_preserves_pending_characteristic_read() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);
    let service_uuid = Uuid::from_u128(0x0000180f_0000_1000_8000_00805f9b34fb);
    let characteristic_uuid = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
    let characteristic_cbuuid = uuid_to_cbuuid(characteristic_uuid);
    let characteristic = unsafe {
        CBMutableCharacteristic::initWithType_properties_value_permissions(
            CBMutableCharacteristic::alloc(),
            &characteristic_cbuuid,
            CBCharacteristicProperties::Read,
            None,
            CBAttributePermissions::Readable,
        )
    };
    let characteristic: Retained<CBCharacteristic> = Retained::into_super(characteristic);
    let mut characteristic_internal = CharacteristicInternal::new(characteristic);
    characteristic_internal.discovered = true;
    let mut pending_read = CoreBluetoothReplyFuture::default();
    characteristic_internal
        .read_future_state
        .push_back(pending_read.get_state_clone());
    let service = unsafe {
        CBMutableService::initWithType_primary(
            CBMutableService::alloc(),
            &uuid_to_cbuuid(service_uuid),
            true,
        )
    };
    internal.services.insert(
        service_uuid,
        ServiceInternal {
            cbservice: Retained::into_super(service),
            characteristics: HashMap::from([(characteristic_uuid, characteristic_internal)]),
            discovered: true,
        },
    );

    // A late/duplicate error callback (#167) must not prune the
    // characteristic or its in-flight read.
    internal.set_characteristics(
        service_uuid,
        HashMap::new(),
        Some(super::super::native_error::NativeError::local(
            "controlled discovery failure".into(),
        )),
    );

    assert_pending(&mut pending_read, "pending read after characteristic error").await;
    let service = internal
        .services
        .get(&service_uuid)
        .expect("service preserved");
    assert!(
        service.characteristics.contains_key(&characteristic_uuid),
        "characteristic must survive an error callback"
    );

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[tokio::test]
async fn descriptor_discovery_error_preserves_pending_descriptor_read() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);
    let service_uuid = Uuid::from_u128(0x0000180f_0000_1000_8000_00805f9b34fb);
    let characteristic_uuid = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
    let characteristic_cbuuid = uuid_to_cbuuid(characteristic_uuid);
    let characteristic = unsafe {
        CBMutableCharacteristic::initWithType_properties_value_permissions(
            CBMutableCharacteristic::alloc(),
            &characteristic_cbuuid,
            CBCharacteristicProperties::Read,
            None,
            CBAttributePermissions::Readable,
        )
    };
    let characteristic: Retained<CBCharacteristic> = Retained::into_super(characteristic);
    let mut characteristic_internal = CharacteristicInternal::new(characteristic);
    characteristic_internal.discovered = true;
    let descriptor_uuid = Uuid::from_u128(0x00002902_0000_1000_8000_00805f9b34fb);
    let descriptor_cbuuid = uuid_to_cbuuid(descriptor_uuid);
    let descriptor_value = NSData::from_vec(vec![0u8]);
    let descriptor = unsafe {
        CBMutableDescriptor::initWithType_value(
            CBMutableDescriptor::alloc(),
            &descriptor_cbuuid,
            Some(&descriptor_value),
        )
    };
    let mut descriptor_internal = DescriptorInternal::new(Retained::into_super(descriptor));
    let mut pending_read = CoreBluetoothReplyFuture::default();
    descriptor_internal
        .read_future_state
        .push_back(pending_read.get_state_clone());
    characteristic_internal
        .descriptors
        .insert(descriptor_uuid, descriptor_internal);
    let service = unsafe {
        CBMutableService::initWithType_primary(
            CBMutableService::alloc(),
            &uuid_to_cbuuid(service_uuid),
            true,
        )
    };
    internal.services.insert(
        service_uuid,
        ServiceInternal {
            cbservice: Retained::into_super(service),
            characteristics: HashMap::from([(characteristic_uuid, characteristic_internal)]),
            discovered: true,
        },
    );

    // A late/duplicate error callback (#167) must not prune the
    // descriptor or its in-flight read.
    internal.set_characteristic_descriptors(
        service_uuid,
        characteristic_uuid,
        HashMap::new(),
        Some(super::super::native_error::NativeError::local(
            "controlled discovery failure".into(),
        )),
    );

    assert_pending(&mut pending_read, "pending read after descriptor error").await;
    let service = internal.services.get(&service_uuid).expect("service");
    let characteristic = service
        .characteristics
        .get(&characteristic_uuid)
        .expect("characteristic");
    assert!(
        characteristic.descriptors.contains_key(&descriptor_uuid),
        "descriptor must survive an error callback"
    );

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[tokio::test]
async fn descriptor_discovery_error_fails_with_native_diagnostics() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let peripheral_uuid_string = NSString::from_str(&peripheral_uuid.to_string());
    let peripheral_identifier =
        NSUUID::initWithUUIDString(NSUUID::alloc(), &peripheral_uuid_string)
            .expect("valid peripheral UUID");
    let peripheral = TestPeripheral::new(peripheral_identifier);
    let service_uuid = Uuid::from_u128(0x0000180f_0000_1000_8000_00805f9b34fb);
    let characteristic_uuid = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
    let service_cbuuid = uuid_to_cbuuid(service_uuid);
    let characteristic_cbuuid = uuid_to_cbuuid(characteristic_uuid);
    let characteristic = unsafe {
        CBMutableCharacteristic::initWithType_properties_value_permissions(
            CBMutableCharacteristic::alloc(),
            &characteristic_cbuuid,
            CBCharacteristicProperties::Read,
            None,
            CBAttributePermissions::Readable,
        )
    };
    let service = unsafe {
        CBMutableService::initWithType_primary(CBMutableService::alloc(), &service_cbuuid, true)
    };
    let characteristic: Retained<CBCharacteristic> = Retained::into_super(characteristic);
    let characteristics = NSArray::from_retained_slice(std::slice::from_ref(&characteristic));
    unsafe { service.setCharacteristics(Some(&characteristics)) };
    let service: Retained<CBService> = Retained::into_super(service);

    let (event_sender, _) = mpsc::channel(1);
    let mut internal =
        PeripheralInternal::new(Retained::into_super(peripheral.clone()), event_sender);
    internal.services.insert(
        service_uuid,
        ServiceInternal {
            cbservice: service,
            characteristics: HashMap::from([(
                characteristic_uuid,
                CharacteristicInternal::new(characteristic.clone()),
            )]),
            discovered: false,
        },
    );
    let discovery = CoreBluetoothReplyFuture::default();
    internal
        .services_discovered_future_state
        .push_back(discovery.get_state_clone());

    let (delegate_sender, mut delegate_receiver) = mpsc::channel(1);
    let delegate = CentralDelegate::new(delegate_sender);
    let error = NSError::new(1, ns_string!("BtlePlugCoreBluetoothTests"));
    unsafe {
        delegate.peripheral_didDiscoverDescriptorsForCharacteristic_error(
            &peripheral,
            &characteristic,
            Some(&error),
        );
    }

    let event = tokio::time::timeout(Duration::from_secs(1), delegate_receiver.next())
        .await
        .expect("descriptor error callback did not emit an event")
        .expect("delegate event channel closed");
    let CentralDelegateEvent::DiscoveredCharacteristicDescriptors {
        peripheral_uuid: event_peripheral_uuid,
        service_uuid: event_service_uuid,
        characteristic_uuid: event_characteristic_uuid,
        descriptors,
        error: event_error,
    } = event
    else {
        panic!("unexpected delegate event: {event:?}");
    };
    assert_eq!(event_peripheral_uuid, peripheral_uuid);
    assert_eq!(event_service_uuid, service_uuid);
    assert_eq!(event_characteristic_uuid, characteristic_uuid);
    assert!(descriptors.is_empty());
    assert!(event_error.is_some());

    internal.set_characteristic_descriptors(
        event_service_uuid,
        event_characteristic_uuid,
        descriptors,
        event_error,
    );
    let reply = tokio::time::timeout(Duration::from_secs(1), discovery)
        .await
        .expect("service discovery remained pending after descriptor error");
    let CoreBluetoothReply::NativeErr(cause) = reply else {
        panic!("discovery failure was reported as success: {reply:?}");
    };
    assert_eq!(
        cause.native_code.as_deref(),
        Some("BtlePlugCoreBluetoothTests:1")
    );
    assert_eq!(cause.message, error.localizedDescription().to_string());

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[tokio::test]
async fn characteristic_discovery_error_fails_with_native_diagnostics() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);
    let service_uuid = Uuid::from_u128(0x0000180f_0000_1000_8000_00805f9b34fb);
    let service_cbuuid = uuid_to_cbuuid(service_uuid);
    let service = unsafe {
        CBMutableService::initWithType_primary(CBMutableService::alloc(), &service_cbuuid, true)
    };
    let service: Retained<CBService> = Retained::into_super(service);
    internal.services.insert(
        service_uuid,
        ServiceInternal {
            cbservice: service.clone(),
            characteristics: HashMap::new(),
            discovered: false,
        },
    );
    let discovery = CoreBluetoothReplyFuture::default();
    internal
        .services_discovered_future_state
        .push_back(discovery.get_state_clone());

    let (delegate_sender, mut delegate_receiver) = mpsc::channel(1);
    let delegate = CentralDelegate::new(delegate_sender);
    let error = NSError::new(1, ns_string!("BtlePlugCoreBluetoothTests"));
    unsafe {
        delegate.peripheral_didDiscoverCharacteristicsForService_error(
            &peripheral,
            &service,
            Some(&error),
        );
    }

    let event = tokio::time::timeout(Duration::from_secs(1), delegate_receiver.next())
        .await
        .expect("characteristic discovery error callback did not emit an event")
        .expect("delegate event channel closed");
    let CentralDelegateEvent::DiscoveredCharacteristics {
        peripheral_uuid: event_peripheral_uuid,
        service_uuid: event_service_uuid,
        characteristics,
        error: event_error,
    } = event
    else {
        panic!("unexpected delegate event: {event:?}");
    };
    assert_eq!(event_peripheral_uuid, peripheral_uuid);
    assert_eq!(event_service_uuid, service_uuid);
    assert!(characteristics.is_empty());
    assert!(event_error.is_some());

    internal.set_characteristics(event_service_uuid, characteristics, event_error);
    let reply = tokio::time::timeout(Duration::from_secs(1), discovery)
        .await
        .expect("service discovery remained pending after characteristic discovery error");
    let CoreBluetoothReply::NativeErr(cause) = reply else {
        panic!("discovery failure was reported as success: {reply:?}");
    };
    assert_eq!(
        cause.native_code.as_deref(),
        Some("BtlePlugCoreBluetoothTests:1")
    );
    assert_eq!(cause.message, error.localizedDescription().to_string());

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[tokio::test]
async fn service_discovery_error_emits_error_event() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, internal) = new_test_internal(peripheral_uuid);

    let (delegate_sender, mut delegate_receiver) = mpsc::channel(1);
    let delegate = CentralDelegate::new(delegate_sender);
    let error = NSError::new(1, ns_string!("BtlePlugCoreBluetoothTests"));
    unsafe {
        delegate.peripheral_didDiscoverServices(&peripheral, Some(&error));
    }

    let event = tokio::time::timeout(Duration::from_secs(1), delegate_receiver.next())
        .await
        .expect("service discovery error callback did not emit an event")
        .expect("delegate event channel closed");
    let CentralDelegateEvent::DiscoveredServices {
        peripheral_uuid: event_peripheral_uuid,
        services,
        error: event_error,
    } = event
    else {
        panic!("unexpected delegate event: {event:?}");
    };
    assert_eq!(event_peripheral_uuid, peripheral_uuid);
    assert!(services.is_empty());
    assert!(event_error.is_some());

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

fn new_test_internal(peripheral_uuid: Uuid) -> (Retained<TestPeripheral>, PeripheralInternal) {
    let peripheral_uuid_string = NSString::from_str(&peripheral_uuid.to_string());
    let peripheral_identifier =
        NSUUID::initWithUUIDString(NSUUID::alloc(), &peripheral_uuid_string)
            .expect("valid peripheral UUID");
    let peripheral = TestPeripheral::new(peripheral_identifier);
    let (event_sender, _) = mpsc::channel(1);
    let internal = PeripheralInternal::new(Retained::into_super(peripheral.clone()), event_sender);
    (peripheral, internal)
}

#[test]
fn set_characteristics_for_unknown_service_does_not_panic() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);
    assert!(internal.services.is_empty());

    internal.set_characteristics(
        Uuid::from_u128(0x0000180f_0000_1000_8000_00805f9b34fb),
        HashMap::new(),
        None,
    );
    assert!(internal.services.is_empty());

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[test]
fn check_discovered_with_no_waiting_future_does_not_panic() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);
    let service_uuid = Uuid::from_u128(0x0000180f_0000_1000_8000_00805f9b34fb);
    let service_cbuuid = uuid_to_cbuuid(service_uuid);
    let service = unsafe {
        CBMutableService::initWithType_primary(CBMutableService::alloc(), &service_cbuuid, true)
    };
    internal.services.insert(
        service_uuid,
        ServiceInternal {
            cbservice: Retained::into_super(service),
            characteristics: HashMap::new(),
            discovered: true,
        },
    );
    assert!(internal.services_discovered_future_state.is_empty());

    internal.check_discovered();

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[tokio::test]
async fn descriptors_for_unknown_service_leave_discovery_pending() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);
    let service_uuid = Uuid::from_u128(0x0000180f_0000_1000_8000_00805f9b34fb);
    let characteristic_uuid = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
    let service_cbuuid = uuid_to_cbuuid(service_uuid);
    let characteristic_cbuuid = uuid_to_cbuuid(characteristic_uuid);
    let characteristic = unsafe {
        CBMutableCharacteristic::initWithType_properties_value_permissions(
            CBMutableCharacteristic::alloc(),
            &characteristic_cbuuid,
            CBCharacteristicProperties::Read,
            None,
            CBAttributePermissions::Readable,
        )
    };
    let service = unsafe {
        CBMutableService::initWithType_primary(CBMutableService::alloc(), &service_cbuuid, true)
    };
    let characteristic: Retained<CBCharacteristic> = Retained::into_super(characteristic);
    let characteristics = NSArray::from_retained_slice(std::slice::from_ref(&characteristic));
    unsafe { service.setCharacteristics(Some(&characteristics)) };
    let service: Retained<CBService> = Retained::into_super(service);
    internal.services.insert(
        service_uuid,
        ServiceInternal {
            cbservice: service,
            characteristics: HashMap::from([(
                characteristic_uuid,
                CharacteristicInternal::new(characteristic),
            )]),
            discovered: false,
        },
    );
    let mut discovery = CoreBluetoothReplyFuture::default();
    internal
        .services_discovered_future_state
        .push_back(discovery.get_state_clone());

    // A descriptor callback for a service this peripheral has never seen
    // (e.g. an included service dropped by set_characteristics) must not
    // fail the still-pending service discovery.
    let handled = internal.set_characteristic_descriptors(
        Uuid::from_u128(0x0000ffff_0000_1000_8000_00805f9b34fb),
        characteristic_uuid,
        HashMap::new(),
        None,
    );
    assert!(handled);

    assert_pending(
        &mut discovery,
        "discovery after unknown-service descriptors",
    )
    .await;

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[tokio::test]
async fn notification_callback_error_completes_requested_operation() {
    for enabled in [true, false] {
        for att_error in [false, true] {
            let context = format!("enabled={enabled} att_error={att_error}");
            let characteristic_uuid = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
            let mut fixture = NotificationFixture::new(&[characteristic_uuid]);
            let future = CoreBluetoothReplyFuture::default();
            fixture.internal.queue_notification_request(
                fixture.service_uuid,
                characteristic_uuid,
                enabled,
                future.get_state_clone(),
            );
            assert_eq!(
                fixture.set_notify_calls(),
                vec![RecordedSetNotify {
                    characteristic_uuid,
                    enabled,
                }],
                "{context}"
            );
            let error = notification_error(att_error);
            let expected_error =
                deliver_notification_state_callback(&mut fixture, 0, Some(&error), &context)
                    .await
                    .expect("{context}: error callback carried no error");
            let reply = tokio::time::timeout(CALLBACK_TIMEOUT, future)
                .await
                .unwrap_or_else(|_| {
                    panic!("{context}: notification error did not complete the request")
                });
            match reply {
                CoreBluetoothReply::NativeErr(actual) => {
                    assert_eq!(actual.message, expected_error, "{context}")
                }
                reply => panic!("{context}: unexpected reply: {reply:?}"),
            }
        }
    }
}

#[tokio::test]
async fn notification_callback_success_completes_requested_operation() {
    let characteristic_uuid = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
    let mut fixture = NotificationFixture::new(&[characteristic_uuid]);

    let enable = CoreBluetoothReplyFuture::default();
    fixture.internal.queue_notification_request(
        fixture.service_uuid,
        characteristic_uuid,
        true,
        enable.get_state_clone(),
    );
    deliver_notification_state_callback(&mut fixture, 0, None, "enable").await;
    assert!(
        matches!(
            tokio::time::timeout(CALLBACK_TIMEOUT, enable)
                .await
                .expect("enable did not complete"),
            CoreBluetoothReply::Ok
        ),
        "enable reply was not Ok"
    );

    let disable = CoreBluetoothReplyFuture::default();
    fixture.internal.queue_notification_request(
        fixture.service_uuid,
        characteristic_uuid,
        false,
        disable.get_state_clone(),
    );
    deliver_notification_state_callback(&mut fixture, 0, None, "disable").await;
    assert!(
        matches!(
            tokio::time::timeout(CALLBACK_TIMEOUT, disable)
                .await
                .expect("disable did not complete"),
            CoreBluetoothReply::Ok
        ),
        "disable reply was not Ok"
    );

    // Repeated same-direction requests stay independent native calls and
    // futures; no coalescing.
    let first_repeat = CoreBluetoothReplyFuture::default();
    let mut second_repeat = CoreBluetoothReplyFuture::default();
    fixture.internal.queue_notification_request(
        fixture.service_uuid,
        characteristic_uuid,
        true,
        first_repeat.get_state_clone(),
    );
    fixture.internal.queue_notification_request(
        fixture.service_uuid,
        characteristic_uuid,
        true,
        second_repeat.get_state_clone(),
    );
    assert_eq!(fixture.set_notify_calls().len(), 3);
    deliver_notification_state_callback(&mut fixture, 0, None, "first repeat").await;
    assert!(
        matches!(
            tokio::time::timeout(CALLBACK_TIMEOUT, first_repeat)
                .await
                .expect("first repeat did not complete"),
            CoreBluetoothReply::Ok
        ),
        "first repeat reply was not Ok"
    );
    assert_pending(&mut second_repeat, "second repeat").await;
    deliver_notification_state_callback(&mut fixture, 0, None, "second repeat").await;
    assert!(
        matches!(
            tokio::time::timeout(CALLBACK_TIMEOUT, second_repeat)
                .await
                .expect("second repeat did not complete"),
            CoreBluetoothReply::Ok
        ),
        "second repeat reply was not Ok"
    );
    assert_eq!(
        fixture.set_notify_calls(),
        vec![
            RecordedSetNotify {
                characteristic_uuid,
                enabled: true
            },
            RecordedSetNotify {
                characteristic_uuid,
                enabled: false
            },
            RecordedSetNotify {
                characteristic_uuid,
                enabled: true
            },
            RecordedSetNotify {
                characteristic_uuid,
                enabled: true
            },
        ]
    );
}

#[tokio::test]
async fn notification_requests_are_serialized_per_characteristic() {
    let characteristic_uuid = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
    let mut fixture = NotificationFixture::new(&[characteristic_uuid]);

    let first = CoreBluetoothReplyFuture::default();
    let mut second = CoreBluetoothReplyFuture::default();
    let mut third = CoreBluetoothReplyFuture::default();
    fixture.internal.queue_notification_request(
        fixture.service_uuid,
        characteristic_uuid,
        true,
        first.get_state_clone(),
    );
    assert_eq!(
        fixture.set_notify_calls(),
        vec![RecordedSetNotify {
            characteristic_uuid,
            enabled: true
        }]
    );
    fixture.internal.queue_notification_request(
        fixture.service_uuid,
        characteristic_uuid,
        false,
        second.get_state_clone(),
    );
    fixture.internal.queue_notification_request(
        fixture.service_uuid,
        characteristic_uuid,
        true,
        third.get_state_clone(),
    );
    assert_eq!(
        fixture.set_notify_calls().len(),
        1,
        "only the queue head may be submitted"
    );
    assert_pending(&mut second, "second").await;
    assert_pending(&mut third, "third").await;

    deliver_notification_state_callback(&mut fixture, 0, None, "first").await;
    assert!(
        matches!(
            tokio::time::timeout(CALLBACK_TIMEOUT, first)
                .await
                .expect("first did not complete"),
            CoreBluetoothReply::Ok
        ),
        "first reply was not Ok"
    );
    assert_eq!(
        fixture.set_notify_calls(),
        vec![
            RecordedSetNotify {
                characteristic_uuid,
                enabled: true
            },
            RecordedSetNotify {
                characteristic_uuid,
                enabled: false
            },
        ]
    );
    assert_pending(&mut third, "third").await;

    let error = notification_error(true);
    let expected_error =
        deliver_notification_state_callback(&mut fixture, 0, Some(&error), "second")
            .await
            .expect("second callback carried no error");
    let second_reply = tokio::time::timeout(CALLBACK_TIMEOUT, second)
        .await
        .expect("second did not complete");
    assert!(
        matches!(second_reply, CoreBluetoothReply::NativeErr(ref actual) if actual.message == expected_error),
        "second reply was not the callback error: {second_reply:?}"
    );
    assert_eq!(
        fixture.set_notify_calls().len(),
        3,
        "completing the head must submit exactly the next request"
    );
    assert_pending(&mut third, "third after second callback").await;

    deliver_notification_state_callback(&mut fixture, 0, None, "third").await;
    assert!(
        matches!(
            tokio::time::timeout(CALLBACK_TIMEOUT, third)
                .await
                .expect("third did not complete"),
            CoreBluetoothReply::Ok
        ),
        "third reply was not Ok"
    );
    assert_eq!(fixture.set_notify_calls().len(), 3);
}

#[tokio::test]
async fn notification_requests_on_distinct_characteristics_are_independent() {
    let characteristic_a = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
    let characteristic_b = Uuid::from_u128(0x00002a37_0000_1000_8000_00805f9b34fb);
    let mut fixture = NotificationFixture::new(&[characteristic_a, characteristic_b]);

    let future_a = CoreBluetoothReplyFuture::default();
    let mut future_b = CoreBluetoothReplyFuture::default();
    let mut future_b_second = CoreBluetoothReplyFuture::default();
    fixture.internal.queue_notification_request(
        fixture.service_uuid,
        characteristic_a,
        true,
        future_a.get_state_clone(),
    );
    assert_eq!(
        fixture.set_notify_calls(),
        vec![RecordedSetNotify {
            characteristic_uuid: characteristic_a,
            enabled: true
        }]
    );
    fixture.internal.queue_notification_request(
        fixture.service_uuid,
        characteristic_b,
        true,
        future_b.get_state_clone(),
    );
    assert_eq!(
        fixture.set_notify_calls(),
        vec![
            RecordedSetNotify {
                characteristic_uuid: characteristic_a,
                enabled: true
            },
            RecordedSetNotify {
                characteristic_uuid: characteristic_b,
                enabled: true
            },
        ]
    );
    fixture.internal.queue_notification_request(
        fixture.service_uuid,
        characteristic_b,
        false,
        future_b_second.get_state_clone(),
    );
    assert_eq!(
        fixture.set_notify_calls().len(),
        2,
        "each characteristic must own its queue"
    );

    let error = notification_error(true);
    let expected_error = deliver_notification_state_callback(&mut fixture, 0, Some(&error), "a")
        .await
        .expect("a callback carried no error");
    let reply_a = tokio::time::timeout(CALLBACK_TIMEOUT, future_a)
        .await
        .expect("a did not complete");
    assert!(
        matches!(reply_a, CoreBluetoothReply::NativeErr(ref actual) if actual.message == expected_error),
        "a reply was not the callback error: {reply_a:?}"
    );
    assert_pending(&mut future_b, "b").await;
    assert_pending(&mut future_b_second, "b second").await;
    assert_eq!(
        fixture.set_notify_calls().len(),
        2,
        "a callback for one characteristic must not advance another"
    );

    deliver_notification_state_callback(&mut fixture, 1, None, "b").await;
    assert!(
        matches!(
            tokio::time::timeout(CALLBACK_TIMEOUT, future_b)
                .await
                .expect("b did not complete"),
            CoreBluetoothReply::Ok
        ),
        "b reply was not Ok"
    );
    assert_eq!(fixture.set_notify_calls().len(), 3);
    assert_pending(&mut future_b_second, "b second after first b callback").await;
    deliver_notification_state_callback(&mut fixture, 1, None, "b second").await;
    assert!(
        matches!(
            tokio::time::timeout(CALLBACK_TIMEOUT, future_b_second)
                .await
                .expect("b second did not complete"),
            CoreBluetoothReply::Ok
        ),
        "b second reply was not Ok"
    );
}

#[tokio::test]
async fn cancelled_notification_waiter_does_not_steal_next_reply() {
    let characteristic_uuid = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
    let mut fixture = NotificationFixture::new(&[characteristic_uuid]);

    let dropped_waiter = CoreBluetoothReplyFuture::default();
    let dropped_state = dropped_waiter.get_state_clone();
    drop(dropped_waiter);
    fixture.internal.queue_notification_request(
        fixture.service_uuid,
        characteristic_uuid,
        true,
        dropped_state,
    );

    let mut survivor = CoreBluetoothReplyFuture::default();
    fixture.internal.queue_notification_request(
        fixture.service_uuid,
        characteristic_uuid,
        false,
        survivor.get_state_clone(),
    );
    assert_eq!(
        fixture.set_notify_calls(),
        vec![RecordedSetNotify {
            characteristic_uuid,
            enabled: true
        }],
        "the second request must not be submitted while the first is in flight"
    );

    deliver_notification_state_callback(&mut fixture, 0, None, "dropped waiter").await;
    assert_eq!(
        fixture.set_notify_calls(),
        vec![
            RecordedSetNotify {
                characteristic_uuid,
                enabled: true
            },
            RecordedSetNotify {
                characteristic_uuid,
                enabled: false
            },
        ],
        "the dropped waiter's callback must submit the next request"
    );
    assert_pending(&mut survivor, "survivor").await;

    deliver_notification_state_callback(&mut fixture, 0, None, "survivor").await;
    assert!(
        matches!(
            tokio::time::timeout(CALLBACK_TIMEOUT, survivor)
                .await
                .expect("survivor did not complete"),
            CoreBluetoothReply::Ok
        ),
        "survivor reply was not Ok"
    );
}

#[tokio::test]
async fn disconnect_drains_active_and_queued_notification_requests() {
    let characteristic_uuid = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
    let mut fixture = NotificationFixture::new(&[characteristic_uuid]);

    let first = CoreBluetoothReplyFuture::default();
    let second = CoreBluetoothReplyFuture::default();
    let third = CoreBluetoothReplyFuture::default();
    fixture.internal.queue_notification_request(
        fixture.service_uuid,
        characteristic_uuid,
        true,
        first.get_state_clone(),
    );
    fixture.internal.queue_notification_request(
        fixture.service_uuid,
        characteristic_uuid,
        false,
        second.get_state_clone(),
    );
    fixture.internal.queue_notification_request(
        fixture.service_uuid,
        characteristic_uuid,
        true,
        third.get_state_clone(),
    );
    assert_eq!(fixture.set_notify_calls().len(), 1);

    fixture
        .internal
        .drain_pending_operations("Device disconnected");

    for (name, future) in [("first", first), ("second", second), ("third", third)] {
        let reply = tokio::time::timeout(CALLBACK_TIMEOUT, future)
            .await
            .unwrap_or_else(|_| panic!("{name} did not drain"));
        assert!(
            matches!(reply, CoreBluetoothReply::Err(ref message) if message == "Device disconnected"),
            "{name}: unexpected drain reply: {reply:?}"
        );
    }
    assert_eq!(
        fixture.set_notify_calls().len(),
        1,
        "draining must not submit more requests"
    );

    deliver_notification_state_callback(&mut fixture, 0, None, "late callback after drain").await;
    assert_eq!(
        fixture.set_notify_calls().len(),
        1,
        "a callback after draining must submit nothing"
    );
}

#[tokio::test]
async fn unexpected_notification_callback_is_ignored() {
    let characteristic_a = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
    let mut fixture = NotificationFixture::new(&[characteristic_a]);
    let error = notification_error(true);
    deliver_notification_state_callback(&mut fixture, 0, Some(&error), "callback with empty queue")
        .await;
    assert!(fixture.set_notify_calls().is_empty());

    let characteristic_b = Uuid::from_u128(0x00002a37_0000_1000_8000_00805f9b34fb);
    let mut fixture = NotificationFixture::new(&[characteristic_a, characteristic_b]);
    let mut future_a = CoreBluetoothReplyFuture::default();
    fixture.internal.queue_notification_request(
        fixture.service_uuid,
        characteristic_a,
        true,
        future_a.get_state_clone(),
    );
    deliver_notification_state_callback(
        &mut fixture,
        1,
        None,
        "callback for characteristic without a pending request",
    )
    .await;
    assert_eq!(
        fixture.set_notify_calls().len(),
        1,
        "unexpected callback must not consume another characteristic's request"
    );
    assert_pending(&mut future_a, "a after unrelated callback").await;
    deliver_notification_state_callback(&mut fixture, 0, None, "a").await;
    assert!(
        matches!(
            tokio::time::timeout(CALLBACK_TIMEOUT, future_a)
                .await
                .expect("a did not complete"),
            CoreBluetoothReply::Ok
        ),
        "a reply was not Ok"
    );
}

#[tokio::test]
async fn discover_services_error_completes_discovery_with_error() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);
    let discovery = CoreBluetoothReplyFuture::default();
    internal
        .services_discovered_future_state
        .push_back(discovery.get_state_clone());

    internal.set_discovered_services(
        HashMap::new(),
        Some(super::super::native_error::NativeError::local(
            "boom".into(),
        )),
    );

    let reply = tokio::time::timeout(Duration::from_secs(1), discovery)
        .await
        .expect("discovery remained pending after a service discovery error");
    assert!(
        matches!(reply, CoreBluetoothReply::NativeErr(error) if error.message == "boom"),
        "expected an error reply carrying the delegate's error"
    );
    assert!(internal.services.is_empty());

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[test]
fn discover_services_error_with_no_waiting_future_does_not_panic() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);
    assert!(internal.services_discovered_future_state.is_empty());

    internal.set_discovered_services(
        HashMap::new(),
        Some(super::super::native_error::NativeError::local(
            "boom".into(),
        )),
    );

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[tokio::test]
async fn discover_services_with_zero_services_completes_immediately() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);
    let discovery = CoreBluetoothReplyFuture::default();
    internal
        .services_discovered_future_state
        .push_back(discovery.get_state_clone());

    internal.set_discovered_services(HashMap::new(), None);

    let reply = tokio::time::timeout(Duration::from_secs(1), discovery)
        .await
        .expect("discovery of an empty service set never completed");
    let CoreBluetoothReply::ServicesDiscovered(services, _mtu) = reply else {
        panic!("unexpected discovery reply: {reply:?}");
    };
    assert!(services.is_empty());

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[tokio::test]
async fn discover_services_with_pending_services_waits_for_characteristics() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);
    let service_uuid = Uuid::from_u128(0x0000180f_0000_1000_8000_00805f9b34fb);
    let service_cbuuid = uuid_to_cbuuid(service_uuid);
    let service = unsafe {
        CBMutableService::initWithType_primary(CBMutableService::alloc(), &service_cbuuid, true)
    };
    let service: Retained<CBService> = Retained::into_super(service);
    let mut service_map = HashMap::new();
    service_map.insert(service_uuid, service);

    let mut discovery = CoreBluetoothReplyFuture::default();
    internal
        .services_discovered_future_state
        .push_back(discovery.get_state_clone());

    internal.set_discovered_services(service_map, None);

    assert_pending(&mut discovery, "discovery with an undiscovered service").await;

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[tokio::test]
async fn repeated_discovery_after_completion_does_not_hang() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);
    let service_uuid = Uuid::from_u128(0x0000180f_0000_1000_8000_00805f9b34fb);
    let service_cbuuid = uuid_to_cbuuid(service_uuid);
    let service = unsafe {
        CBMutableService::initWithType_primary(CBMutableService::alloc(), &service_cbuuid, true)
    };
    let service: Retained<CBService> = Retained::into_super(service);
    let mut service_map = HashMap::new();
    service_map.insert(service_uuid, service);

    let first = CoreBluetoothReplyFuture::default();
    internal
        .services_discovered_future_state
        .push_back(first.get_state_clone());
    internal.set_discovered_services(service_map.clone(), None);
    internal.set_characteristics(service_uuid, HashMap::new(), None);
    tokio::time::timeout(Duration::from_secs(1), first)
        .await
        .expect("first discovery did not complete");
    assert!(internal.services_discovered_future_state.is_empty());

    let second = CoreBluetoothReplyFuture::default();
    internal
        .services_discovered_future_state
        .push_back(second.get_state_clone());
    internal.set_discovered_services(service_map, None);
    internal.set_characteristics(service_uuid, HashMap::new(), None);
    let reply = tokio::time::timeout(Duration::from_secs(1), second)
        .await
        .expect("second discovery hung");
    assert!(matches!(
        reply,
        CoreBluetoothReply::ServicesDiscovered(_, _)
    ));

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[tokio::test]
async fn concurrent_discovery_waiters_both_complete() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);
    let service_uuid = Uuid::from_u128(0x0000180f_0000_1000_8000_00805f9b34fb);
    let service_cbuuid = uuid_to_cbuuid(service_uuid);
    let service = unsafe {
        CBMutableService::initWithType_primary(CBMutableService::alloc(), &service_cbuuid, true)
    };
    let service: Retained<CBService> = Retained::into_super(service);
    let mut service_map = HashMap::new();
    service_map.insert(service_uuid, service);

    let first = CoreBluetoothReplyFuture::default();
    let second = CoreBluetoothReplyFuture::default();
    internal
        .services_discovered_future_state
        .push_back(first.get_state_clone());
    internal
        .services_discovered_future_state
        .push_back(second.get_state_clone());

    internal.set_discovered_services(service_map, None);
    internal.set_characteristics(service_uuid, HashMap::new(), None);

    for (name, future) in [("first", first), ("second", second)] {
        let reply = tokio::time::timeout(Duration::from_secs(1), future)
            .await
            .unwrap_or_else(|_| panic!("{name} waiter never completed"));
        assert!(
            matches!(reply, CoreBluetoothReply::ServicesDiscovered(_, _)),
            "{name}: unexpected reply: {reply:?}"
        );
    }
    assert!(internal.services_discovered_future_state.is_empty());

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[tokio::test]
async fn concurrent_connect_waiters_both_complete() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);

    let first = CoreBluetoothReplyFuture::default();
    let second = CoreBluetoothReplyFuture::default();
    internal
        .connected_future_state
        .push_back(first.get_state_clone());
    internal
        .connected_future_state
        .push_back(second.get_state_clone());

    internal.complete_connect(CoreBluetoothReply::Connected);

    for (name, future) in [("first", first), ("second", second)] {
        let reply = tokio::time::timeout(Duration::from_secs(1), future)
            .await
            .unwrap_or_else(|_| panic!("{name} waiter never completed"));
        assert!(
            matches!(reply, CoreBluetoothReply::Connected),
            "{name}: unexpected reply: {reply:?}"
        );
    }
    assert!(internal.connected_future_state.is_empty());

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[tokio::test]
async fn drain_pending_operations_errors_all_queued_waiters() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);

    let connect_a = CoreBluetoothReplyFuture::default();
    let connect_b = CoreBluetoothReplyFuture::default();
    let discover_a = CoreBluetoothReplyFuture::default();
    let discover_b = CoreBluetoothReplyFuture::default();
    internal
        .connected_future_state
        .push_back(connect_a.get_state_clone());
    internal
        .connected_future_state
        .push_back(connect_b.get_state_clone());
    internal
        .services_discovered_future_state
        .push_back(discover_a.get_state_clone());
    internal
        .services_discovered_future_state
        .push_back(discover_b.get_state_clone());

    internal.drain_pending_operations("Device disconnected");

    for (name, future) in [
        ("connect_a", connect_a),
        ("connect_b", connect_b),
        ("discover_a", discover_a),
        ("discover_b", discover_b),
    ] {
        let reply = tokio::time::timeout(Duration::from_secs(1), future)
            .await
            .unwrap_or_else(|_| panic!("{name} did not drain"));
        assert!(
            matches!(reply, CoreBluetoothReply::Err(ref message) if message == "Device disconnected"),
            "{name}: unexpected drain reply: {reply:?}"
        );
    }
    assert!(internal.connected_future_state.is_empty());
    assert!(internal.services_discovered_future_state.is_empty());

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[tokio::test]
async fn rediscovery_preserves_pending_characteristic_read() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);
    let service_uuid = Uuid::from_u128(0x0000180f_0000_1000_8000_00805f9b34fb);
    let characteristic_uuid = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
    let service_cbuuid = uuid_to_cbuuid(service_uuid);
    let characteristic_cbuuid = uuid_to_cbuuid(characteristic_uuid);
    let characteristic = unsafe {
        CBMutableCharacteristic::initWithType_properties_value_permissions(
            CBMutableCharacteristic::alloc(),
            &characteristic_cbuuid,
            CBCharacteristicProperties::Read,
            None,
            CBAttributePermissions::Readable,
        )
    };
    let characteristic: Retained<CBCharacteristic> = Retained::into_super(characteristic);
    let service = unsafe {
        CBMutableService::initWithType_primary(CBMutableService::alloc(), &service_cbuuid, true)
    };
    let service: Retained<CBService> = Retained::into_super(service);
    internal.services.insert(
        service_uuid,
        ServiceInternal {
            cbservice: service,
            characteristics: HashMap::from([(
                characteristic_uuid,
                CharacteristicInternal::new(characteristic),
            )]),
            discovered: true,
        },
    );

    let mut read_future = CoreBluetoothReplyFuture::default();
    internal
        .services
        .get_mut(&service_uuid)
        .unwrap()
        .characteristics
        .get_mut(&characteristic_uuid)
        .unwrap()
        .read_future_state
        .push_back(read_future.get_state_clone());

    // Re-discovery hands back a new CBService object for the same UUID.
    let rediscovered_service = unsafe {
        CBMutableService::initWithType_primary(CBMutableService::alloc(), &service_cbuuid, true)
    };
    let rediscovered_service: Retained<CBService> = Retained::into_super(rediscovered_service);
    internal.set_discovered_services(HashMap::from([(service_uuid, rediscovered_service)]), None);

    assert_pending(&mut read_future, "read future after rediscovery").await;
    let preserved_service = internal
        .services
        .get(&service_uuid)
        .expect("service preserved");
    assert!(
        !preserved_service.discovered,
        "service must await re-discovery before completing again"
    );
    let preserved_characteristic = preserved_service
        .characteristics
        .get(&characteristic_uuid)
        .expect("characteristic preserved across rediscovery");
    assert_eq!(preserved_characteristic.read_future_state.len(), 1);

    // Simulate the read value arriving after rediscovery.
    let state = internal
        .services
        .get_mut(&service_uuid)
        .unwrap()
        .characteristics
        .get_mut(&characteristic_uuid)
        .unwrap()
        .read_future_state
        .pop_front()
        .expect("preserved read future");
    crate::corebluetooth::future::set_reply(&state, CoreBluetoothReply::ReadResult(vec![1, 2, 3]));
    let reply = tokio::time::timeout(Duration::from_secs(1), read_future)
        .await
        .expect("preserved read future never completed");
    assert!(
        matches!(reply, CoreBluetoothReply::ReadResult(ref data) if *data == vec![1, 2, 3]),
        "unexpected reply: {reply:?}"
    );

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[tokio::test]
async fn rediscovery_errors_pending_read_of_removed_service() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);
    let removed_service_uuid = Uuid::from_u128(0x0000180f_0000_1000_8000_00805f9b34fb);
    let remaining_service_uuid = Uuid::from_u128(0x00001801_0000_1000_8000_00805f9b34fb);
    let characteristic_uuid = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
    let characteristic_cbuuid = uuid_to_cbuuid(characteristic_uuid);
    let characteristic = unsafe {
        CBMutableCharacteristic::initWithType_properties_value_permissions(
            CBMutableCharacteristic::alloc(),
            &characteristic_cbuuid,
            CBCharacteristicProperties::Read,
            None,
            CBAttributePermissions::Readable,
        )
    };
    let characteristic: Retained<CBCharacteristic> = Retained::into_super(characteristic);
    let removed_service = unsafe {
        CBMutableService::initWithType_primary(
            CBMutableService::alloc(),
            &uuid_to_cbuuid(removed_service_uuid),
            true,
        )
    };
    let removed_service: Retained<CBService> = Retained::into_super(removed_service);
    internal.services.insert(
        removed_service_uuid,
        ServiceInternal {
            cbservice: removed_service,
            characteristics: HashMap::from([(
                characteristic_uuid,
                CharacteristicInternal::new(characteristic),
            )]),
            discovered: true,
        },
    );

    let read_future = CoreBluetoothReplyFuture::default();
    internal
        .services
        .get_mut(&removed_service_uuid)
        .unwrap()
        .characteristics
        .get_mut(&characteristic_uuid)
        .unwrap()
        .read_future_state
        .push_back(read_future.get_state_clone());

    let remaining_service = unsafe {
        CBMutableService::initWithType_primary(
            CBMutableService::alloc(),
            &uuid_to_cbuuid(remaining_service_uuid),
            true,
        )
    };
    let remaining_service: Retained<CBService> = Retained::into_super(remaining_service);
    internal.set_discovered_services(
        HashMap::from([(remaining_service_uuid, remaining_service)]),
        None,
    );

    assert!(!internal.services.contains_key(&removed_service_uuid));
    let reply = tokio::time::timeout(Duration::from_secs(1), read_future)
        .await
        .expect("removed service's pending read never completed");
    assert!(
        matches!(reply, CoreBluetoothReply::Err(_)),
        "unexpected reply: {reply:?}"
    );

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[tokio::test]
async fn drain_pending_operations_errors_service_and_characteristic_queues() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);
    let service_uuid = Uuid::from_u128(0x0000180f_0000_1000_8000_00805f9b34fb);
    let characteristic_uuid = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
    let characteristic_cbuuid = uuid_to_cbuuid(characteristic_uuid);
    let characteristic = unsafe {
        CBMutableCharacteristic::initWithType_properties_value_permissions(
            CBMutableCharacteristic::alloc(),
            &characteristic_cbuuid,
            CBCharacteristicProperties::Read,
            None,
            CBAttributePermissions::Readable,
        )
    };
    let characteristic: Retained<CBCharacteristic> = Retained::into_super(characteristic);
    let mut characteristic_internal = CharacteristicInternal::new(characteristic);

    let service = unsafe {
        CBMutableService::initWithType_primary(
            CBMutableService::alloc(),
            &uuid_to_cbuuid(service_uuid),
            true,
        )
    };
    let service: Retained<CBService> = Retained::into_super(service);

    let char_read = CoreBluetoothReplyFuture::default();
    let char_write = CoreBluetoothReplyFuture::default();
    characteristic_internal
        .read_future_state
        .push_back(char_read.get_state_clone());
    characteristic_internal
        .write_future_state
        .push_back(char_write.get_state_clone());

    internal.services.insert(
        service_uuid,
        ServiceInternal {
            cbservice: service,
            characteristics: HashMap::from([(characteristic_uuid, characteristic_internal)]),
            discovered: true,
        },
    );

    internal.drain_pending_operations("Peripheral cleared");

    for (name, future) in [("char_read", char_read), ("char_write", char_write)] {
        let reply = tokio::time::timeout(Duration::from_secs(1), future)
            .await
            .unwrap_or_else(|_| panic!("{name} did not drain"));
        assert!(
            matches!(reply, CoreBluetoothReply::Err(ref message) if message == "Peripheral cleared"),
            "{name}: unexpected drain reply: {reply:?}"
        );
    }

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[tokio::test]
async fn set_characteristics_removes_one_while_preserving_another() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);
    let service_uuid = Uuid::from_u128(0x0000180f_0000_1000_8000_00805f9b34fb);
    let removed_uuid = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
    let kept_uuid = Uuid::from_u128(0x00002a00_0000_1000_8000_00805f9b34fb);
    let make_characteristic = |uuid: Uuid| -> Retained<CBCharacteristic> {
        let cbuuid = uuid_to_cbuuid(uuid);
        let characteristic = unsafe {
            CBMutableCharacteristic::initWithType_properties_value_permissions(
                CBMutableCharacteristic::alloc(),
                &cbuuid,
                CBCharacteristicProperties::Read,
                None,
                CBAttributePermissions::Readable,
            )
        };
        Retained::into_super(characteristic)
    };
    let service = unsafe {
        CBMutableService::initWithType_primary(
            CBMutableService::alloc(),
            &uuid_to_cbuuid(service_uuid),
            true,
        )
    };
    let service: Retained<CBService> = Retained::into_super(service);

    let removed_write = CoreBluetoothReplyFuture::default();
    let mut removed_characteristic = CharacteristicInternal::new(make_characteristic(removed_uuid));
    removed_characteristic
        .write_future_state
        .push_back(removed_write.get_state_clone());
    let kept_read = CoreBluetoothReplyFuture::default();
    let mut kept_characteristic = CharacteristicInternal::new(make_characteristic(kept_uuid));
    kept_characteristic.discovered = true;
    kept_characteristic
        .read_future_state
        .push_back(kept_read.get_state_clone());

    internal.services.insert(
        service_uuid,
        ServiceInternal {
            cbservice: service,
            characteristics: HashMap::from([
                (removed_uuid, removed_characteristic),
                (kept_uuid, kept_characteristic),
            ]),
            discovered: true,
        },
    );

    // Re-discovery only reports the characteristic being kept.
    internal.set_characteristics(
        service_uuid,
        HashMap::from([(kept_uuid, make_characteristic(kept_uuid))]),
        None,
    );

    let reply = tokio::time::timeout(Duration::from_secs(1), removed_write)
        .await
        .expect("removed characteristic's pending write never completed");
    assert!(matches!(reply, CoreBluetoothReply::Err(_)));

    let service = internal.services.get(&service_uuid).expect("service");
    assert!(!service.characteristics.contains_key(&removed_uuid));
    let kept = service
        .characteristics
        .get(&kept_uuid)
        .expect("kept characteristic preserved");
    assert!(
        !kept.discovered,
        "CB redrives descriptor discovery, so the gate must reset"
    );
    assert_eq!(
        kept.read_future_state.len(),
        1,
        "kept read future preserved"
    );

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[tokio::test]
async fn remove_services_errors_only_the_invalidated_services() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);
    let removed_uuid = Uuid::from_u128(0x0000180f_0000_1000_8000_00805f9b34fb);
    let kept_uuid = Uuid::from_u128(0x00001801_0000_1000_8000_00805f9b34fb);

    let make_service = |uuid: Uuid| -> Retained<CBService> {
        let service = unsafe {
            CBMutableService::initWithType_primary(
                CBMutableService::alloc(),
                &uuid_to_cbuuid(uuid),
                true,
            )
        };
        Retained::into_super(service)
    };

    let removed_read = CoreBluetoothReplyFuture::default();
    let characteristic_uuid = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
    let characteristic_cbuuid = uuid_to_cbuuid(characteristic_uuid);
    let mut removed_characteristic = CharacteristicInternal::new(unsafe {
        Retained::into_super(
            CBMutableCharacteristic::initWithType_properties_value_permissions(
                CBMutableCharacteristic::alloc(),
                &characteristic_cbuuid,
                CBCharacteristicProperties::Read,
                None,
                CBAttributePermissions::Readable,
            ),
        )
    });
    removed_characteristic
        .read_future_state
        .push_back(removed_read.get_state_clone());
    internal.services.insert(
        removed_uuid,
        ServiceInternal {
            cbservice: make_service(removed_uuid),
            characteristics: HashMap::from([(characteristic_uuid, removed_characteristic)]),
            discovered: true,
        },
    );
    internal.services.insert(
        kept_uuid,
        ServiceInternal {
            cbservice: make_service(kept_uuid),
            characteristics: HashMap::new(),
            discovered: true,
        },
    );

    internal.remove_services(&[removed_uuid], "Service invalidated; rediscovery required");

    assert!(!internal.services.contains_key(&removed_uuid));
    assert!(internal.services.contains_key(&kept_uuid));
    let reply = tokio::time::timeout(Duration::from_secs(1), removed_read)
        .await
        .expect("removed service's pending read never completed");
    assert!(matches!(reply, CoreBluetoothReply::Err(_)));

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[tokio::test]
async fn remove_services_reports_no_round_in_progress_for_stale_services() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);
    let kept_uuid = Uuid::from_u128(0x0000180f_0000_1000_8000_00805f9b34fb);
    let removed_uuid = Uuid::from_u128(0x00001801_0000_1000_8000_00805f9b34fb);

    let make_service = |uuid: Uuid| -> Retained<CBService> {
        let service = unsafe {
            CBMutableService::initWithType_primary(
                CBMutableService::alloc(),
                &uuid_to_cbuuid(uuid),
                true,
            )
        };
        Retained::into_super(service)
    };

    // Both services are fully discovered, left over from a previous round.
    internal.services.insert(
        kept_uuid,
        ServiceInternal {
            cbservice: make_service(kept_uuid),
            characteristics: HashMap::new(),
            discovered: true,
        },
    );
    internal.services.insert(
        removed_uuid,
        ServiceInternal {
            cbservice: make_service(removed_uuid),
            characteristics: HashMap::new(),
            discovered: true,
        },
    );

    // discover_services() queues a future without resetting any
    // discovered flags; didDiscoverServices hasn't arrived yet.
    let mut discovery = CoreBluetoothReplyFuture::default();
    internal
        .services_discovered_future_state
        .push_back(discovery.get_state_clone());

    // didModifyServices arrives first and invalidates one service. This
    // mirrors on_services_modified: check_discovered() only runs when
    // remove_services reports a round was actually in progress.
    let was_mid_round =
        internal.remove_services(&[removed_uuid], "Service invalidated; rediscovery required");
    if was_mid_round {
        internal.check_discovered();
    }

    assert!(
        !was_mid_round,
        "the removed service was a stale leftover, not part of an in-progress round"
    );
    assert_pending(
        &mut discovery,
        "discovery must not complete with a stale leftover set",
    )
    .await;

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

#[tokio::test]
async fn remove_services_drains_matching_write_without_response_queue() {
    let peripheral_uuid = Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
    let (peripheral, mut internal) = new_test_internal(peripheral_uuid);
    let removed_service_uuid = Uuid::from_u128(0x0000180f_0000_1000_8000_00805f9b34fb);
    let kept_service_uuid = Uuid::from_u128(0x00001801_0000_1000_8000_00805f9b34fb);
    let characteristic_uuid = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);

    let removed_pending = CoreBluetoothReplyFuture::default();
    let mut kept_pending = CoreBluetoothReplyFuture::default();
    internal
        .write_without_response_queue
        .push_back(PendingWriteWithoutResponse {
            service_uuid: removed_service_uuid,
            characteristic_uuid,
            data: vec![1],
            fut: removed_pending.get_state_clone(),
        });
    internal
        .write_without_response_queue
        .push_back(PendingWriteWithoutResponse {
            service_uuid: kept_service_uuid,
            characteristic_uuid,
            data: vec![2],
            fut: kept_pending.get_state_clone(),
        });

    internal.remove_services(
        &[removed_service_uuid],
        "Service invalidated; rediscovery required",
    );

    let reply = tokio::time::timeout(Duration::from_secs(1), removed_pending)
        .await
        .expect("removed service's queued write-without-response never completed");
    assert!(matches!(reply, CoreBluetoothReply::Err(_)));
    assert_eq!(
        internal.write_without_response_queue.len(),
        1,
        "the kept service's queued entry must survive"
    );
    assert_pending(
        &mut kept_pending,
        "kept service's queued write-without-response",
    )
    .await;

    std::mem::forget(internal);
    std::mem::forget(peripheral);
}

/// Deliver a real `centralManager:didDiscoverPeripheral:advertisementData:RSSI:`
/// callback on `internal`'s own delegate/manager, exactly as CBqueue would.
///
/// When `manufacturer_data` is given, the advertisement dictionary also
/// carries a `CBAdvertisementDataManufacturerDataKey` entry, so the same
/// callback can be used to exercise gating of the advertisement-derived
/// events (`ManufacturerData`, `ServiceData`, `Services`, `TxPowerLevel`)
/// that `centralManager:didDiscoverPeripheral:...` also produces.
fn deliver_discovered_peripheral(
    internal: &CoreBluetoothInternal,
    peripheral: &TestPeripheral,
    manufacturer_data: Option<(u16, &[u8])>,
) {
    let adv_data: Retained<NSMutableDictionary<NSString, AnyObject>> = NSMutableDictionary::new();
    if let Some((manufacturer_id, data)) = manufacturer_data {
        let mut bytes = manufacturer_id.to_le_bytes().to_vec();
        bytes.extend_from_slice(data);
        adv_data.insert(
            unsafe { CBAdvertisementDataManufacturerDataKey },
            &*Retained::into_super(NSData::from_vec(bytes)),
        );
    }
    let rssi = NSNumber::new_i16(-50);
    unsafe {
        internal
            .delegate
            .centralManager_didDiscoverPeripheral_advertisementData_RSSI(
                &internal.manager,
                peripheral,
                &adv_data,
                &rssi,
            );
    }
}

fn test_peripheral_with_uuid(uuid: Uuid) -> Retained<TestPeripheral> {
    let uuid_string = NSString::from_str(&uuid.to_string());
    let identifier = NSUUID::initWithUUIDString(NSUUID::alloc(), &uuid_string).expect("valid UUID");
    TestPeripheral::new(identifier)
}

/// Drain `internal`'s delegate/message channels by repeatedly calling
/// `wait_for_message()` under a short timeout until one times out (i.e.
/// nothing is pending any more). `CBCentralManager`'s
/// `initWithDelegate:queue:` posts an initial `centralManagerDidUpdateState:`
/// on CBqueue that can otherwise land in the delegate channel at an
/// unpredictable time relative to an event a test injects directly, so tests
/// settle the channel before and after injecting to stay timing-independent.
async fn settle(internal: &mut CoreBluetoothInternal) {
    loop {
        if tokio::time::timeout(Duration::from_millis(200), internal.wait_for_message())
            .await
            .is_err()
        {
            break;
        }
    }
}

/// Drain every event currently buffered on `event_receiver` without blocking.
fn drain_events(
    event_receiver: &mut mpsc::Receiver<CoreBluetoothEvent>,
) -> Vec<CoreBluetoothEvent> {
    let mut events = Vec::new();
    while let Ok(event) = event_receiver.try_recv() {
        events.push(event);
    }
    events
}

#[tokio::test]
#[ignore = "requires CoreBluetooth (creates a real CBCentralManager)"]
async fn discovered_peripheral_is_ignored_while_not_scanning() {
    let (_msg_sender, msg_receiver) = mpsc::channel::<CoreBluetoothMessage>(4);
    let (event_sender, mut event_receiver) = mpsc::channel::<CoreBluetoothEvent>(4);
    let mut internal = CoreBluetoothInternal::new(msg_receiver, event_sender);
    settle(&mut internal).await;
    assert!(!internal.scanning, "internal must start out not scanning");

    // Simulate a DiscoveredPeripheral callback that was queued on CBqueue
    // before stop_discovery() took effect (or that arrives with no scan
    // ever started).
    let uuid = Uuid::from_u128(0x22222222_2222_2222_2222_222222222222);
    let peripheral = test_peripheral_with_uuid(uuid);
    deliver_discovered_peripheral(&internal, &peripheral, None);
    settle(&mut internal).await;

    let peripherals_empty = internal.peripherals.is_empty();
    let device_discovered = drain_events(&mut event_receiver)
        .into_iter()
        .find(|event| matches!(event, CoreBluetoothEvent::DeviceDiscovered { .. }));

    std::mem::forget(internal);
    std::mem::forget(peripheral);

    assert!(
        peripherals_empty,
        "a DiscoveredPeripheral event received while not scanning must not add a peripheral"
    );
    assert!(
        device_discovered.is_none(),
        "no DeviceDiscovered event should have been dispatched, got {device_discovered:?}"
    );
}

#[tokio::test]
#[ignore = "requires CoreBluetooth (creates a real CBCentralManager)"]
async fn discovered_peripheral_is_processed_while_scanning() {
    let (_msg_sender, msg_receiver) = mpsc::channel::<CoreBluetoothMessage>(4);
    let (event_sender, mut event_receiver) = mpsc::channel::<CoreBluetoothEvent>(4);
    let mut internal = CoreBluetoothInternal::new(msg_receiver, event_sender);
    settle(&mut internal).await;
    // Set after settling: a PoweredOff initial state would otherwise clear
    // `scanning` right back to false via on_adapter_powered_off().
    internal.scanning = true;

    let uuid = Uuid::from_u128(0x33333333_3333_3333_3333_333333333333);
    let peripheral = test_peripheral_with_uuid(uuid);
    deliver_discovered_peripheral(&internal, &peripheral, None);
    settle(&mut internal).await;

    let peripheral_known = internal.peripherals.contains_key(&uuid);
    let device_discovered = drain_events(&mut event_receiver)
        .into_iter()
        .find(|event| matches!(event, CoreBluetoothEvent::DeviceDiscovered { .. }));

    std::mem::forget(internal);
    std::mem::forget(peripheral);

    assert!(
        peripheral_known,
        "a DiscoveredPeripheral event received while scanning must add the peripheral"
    );
    match device_discovered {
        Some(CoreBluetoothEvent::DeviceDiscovered {
            uuid: event_uuid, ..
        }) => {
            assert_eq!(
                event_uuid, uuid,
                "DeviceDiscovered event was for the wrong peripheral"
            );
        }
        other => panic!("expected a DeviceDiscovered event, got {other:?}"),
    }
}

#[tokio::test]
#[ignore = "requires CoreBluetooth (creates a real CBCentralManager)"]
async fn stop_discovery_causes_late_discovered_peripheral_to_be_ignored() {
    let (_msg_sender, msg_receiver) = mpsc::channel::<CoreBluetoothMessage>(4);
    let (event_sender, mut event_receiver) = mpsc::channel::<CoreBluetoothEvent>(4);
    let mut internal = CoreBluetoothInternal::new(msg_receiver, event_sender);
    settle(&mut internal).await;

    // Mirrors the regression scenario: a scan was running (so this
    // callback was legitimately queued on CBqueue), stop_discovery() has
    // since run, and only then does the queued callback get processed.
    internal.scanning = true;
    let uuid = Uuid::from_u128(0x44444444_4444_4444_4444_444444444444);
    let peripheral = test_peripheral_with_uuid(uuid);
    deliver_discovered_peripheral(&internal, &peripheral, None);
    internal.stop_discovery();
    settle(&mut internal).await;

    let peripherals_empty = internal.peripherals.is_empty();
    let device_discovered = drain_events(&mut event_receiver)
        .into_iter()
        .find(|event| matches!(event, CoreBluetoothEvent::DeviceDiscovered { .. }));

    std::mem::forget(internal);
    std::mem::forget(peripheral);

    assert!(
        peripherals_empty,
        "a callback queued before stop_discovery() must not re-add the peripheral once processed after it"
    );
    assert!(
        device_discovered.is_none(),
        "no DeviceDiscovered event should have been dispatched, got {device_discovered:?}"
    );
}

#[tokio::test]
#[ignore = "requires CoreBluetooth (creates a real CBCentralManager)"]
async fn late_advertisement_data_for_known_peripheral_is_ignored_after_stop_scan() {
    let (_msg_sender, msg_receiver) = mpsc::channel::<CoreBluetoothMessage>(4);
    let (event_sender, mut event_receiver) = mpsc::channel::<CoreBluetoothEvent>(4);
    let mut internal = CoreBluetoothInternal::new(msg_receiver, event_sender);
    settle(&mut internal).await;
    internal.scanning = true;

    // Discover the peripheral while scanning, so it becomes known and we get
    // its per-peripheral event channel out of the DeviceDiscovered event.
    let uuid = Uuid::from_u128(0x55555555_5555_5555_5555_555555555555);
    let peripheral = test_peripheral_with_uuid(uuid);
    deliver_discovered_peripheral(&internal, &peripheral, None);
    settle(&mut internal).await;

    let device_discovered = drain_events(&mut event_receiver)
        .into_iter()
        .find(|event| matches!(event, CoreBluetoothEvent::DeviceDiscovered { .. }));
    let mut peripheral_event_receiver = match device_discovered {
        Some(CoreBluetoothEvent::DeviceDiscovered {
            uuid: event_uuid,
            event_receiver,
            ..
        }) if event_uuid == uuid => event_receiver,
        other => {
            std::mem::forget(internal);
            std::mem::forget(peripheral);
            panic!("expected a DeviceDiscovered event for the injected peripheral, got {other:?}");
        }
    };

    // Now simulate stop_scan(): the peripheral remains known, but a
    // callback carrying fresh advertisement data for it that was queued
    // before stop_scan() must not be forwarded once processed after it.
    internal.stop_discovery();
    deliver_discovered_peripheral(&internal, &peripheral, Some((0x1234, &[0xAA, 0xBB])));
    settle(&mut internal).await;

    let peripheral_still_known = internal.peripherals.contains_key(&uuid);
    let manufacturer_event = peripheral_event_receiver.try_recv().ok();

    std::mem::forget(internal);
    std::mem::forget(peripheral);

    assert!(
        peripheral_still_known,
        "the peripheral should remain known after stop_discovery()"
    );
    assert!(
        manufacturer_event.is_none(),
        "a late ManufacturerData advertisement callback must not reach an already-known peripheral after stop_scan(), got {manufacturer_event:?}"
    );
}

#[test]
#[ignore = "requires a real CoreBluetooth manager and Bluetooth authorization"]
fn shutdown_drains_native_callback_blocked_on_full_event_channel() {
    use std::future::Future;
    use std::sync::Arc;

    struct Callback {
        sender: Sender<CentralDelegateEvent>,
        blocked: std::sync::mpsc::Sender<()>,
        completed: Arc<AtomicBool>,
        closed: Arc<AtomicBool>,
    }
    unsafe extern "C" fn callback(context: *mut std::ffi::c_void) {
        // Ownership is transferred exactly once by dispatch_async_f below.
        let mut callback = unsafe { Box::from_raw(context.cast::<Callback>()) };
        while callback
            .sender
            .try_send(CentralDelegateEvent::DidUpdateState {
                state: CBManagerState::PoweredOn,
            })
            .is_ok()
        {}
        let send = callback.sender.send(CentralDelegateEvent::DidUpdateState {
            state: CBManagerState::PoweredOn,
        });
        let mut send = std::pin::pin!(send);
        let mut signalled = false;
        let result = futures::executor::block_on(futures::future::poll_fn(|cx| {
            let result = send.as_mut().poll(cx);
            if result.is_pending() && !signalled {
                signalled = true;
                let _ = callback.blocked.send(());
            }
            result
        }));
        callback.closed.store(result.is_err(), Ordering::Release);
        callback.completed.store(true, Ordering::Release);
    }

    for _ in 0..100 {
        let (_, commands) = mpsc::channel(1);
        let (events, _) = mpsc::channel(1);
        let internal = CoreBluetoothInternal::new(commands, events);
        let (blocked, observed) = std::sync::mpsc::channel();
        let completed = Arc::new(AtomicBool::new(false));
        let closed = Arc::new(AtomicBool::new(false));
        let context = Box::new(Callback {
            sender: internal.delegate.ivars().clone(),
            blocked,
            completed: completed.clone(),
            closed: closed.clone(),
        });
        unsafe {
            ffi::dispatch_async_f(internal.queue.0, Box::into_raw(context).cast(), callback);
        }
        let blocked = observed.recv_timeout(Duration::from_secs(10));
        // Drop must close the channel before waiting for this native callback.
        drop(internal);
        blocked.expect("native callback must encounter actual channel backpressure");
        assert!(completed.load(Ordering::Acquire));
        assert!(closed.load(Ordering::Acquire));
    }
}

#[test]
#[ignore = "requires a real CoreBluetooth manager and Bluetooth authorization"]
fn shutdown_rejects_every_buffered_command_reply() {
    let (mut commands, receiver) = mpsc::channel(32);
    let (events, _events) = mpsc::channel(1);
    let internal = CoreBluetoothInternal::new(receiver, events);
    let id = Uuid::nil();
    let mut futures = Vec::new();
    for kind in 0..14 {
        let reply = CoreBluetoothReplyFuture::default();
        let future = reply.get_state_clone();
        let command = match kind {
            0 => CoreBluetoothMessage::GetAdapterState { future },
            1 => CoreBluetoothMessage::ConnectDevice {
                peripheral_uuid: id,
                future,
            },
            2 => CoreBluetoothMessage::DisconnectDevice {
                peripheral_uuid: id,
                future,
            },
            3 => CoreBluetoothMessage::ReadValue {
                peripheral_uuid: id,
                service_uuid: id,
                characteristic_uuid: id,
                future,
            },
            4 => CoreBluetoothMessage::WriteValue {
                peripheral_uuid: id,
                service_uuid: id,
                characteristic_uuid: id,
                data: vec![0, 255],
                write_type: WriteType::WithResponse,
                future,
            },
            5 => CoreBluetoothMessage::Subscribe {
                peripheral_uuid: id,
                service_uuid: id,
                characteristic_uuid: id,
                future,
            },
            6 => CoreBluetoothMessage::Unsubscribe {
                peripheral_uuid: id,
                service_uuid: id,
                characteristic_uuid: id,
                future,
            },
            7 => CoreBluetoothMessage::IsConnected {
                peripheral_uuid: id,
                future,
            },
            8 => CoreBluetoothMessage::ReadDescriptorValue {
                peripheral_uuid: id,
                service_uuid: id,
                characteristic_uuid: id,
                descriptor_uuid: id,
                future,
            },
            9 => CoreBluetoothMessage::WriteDescriptorValue {
                peripheral_uuid: id,
                service_uuid: id,
                characteristic_uuid: id,
                descriptor_uuid: id,
                data: vec![0, 255],
                future,
            },
            10 => CoreBluetoothMessage::DiscoverServices {
                peripheral_uuid: id,
                future,
            },
            11 => CoreBluetoothMessage::ReadRssi {
                peripheral_uuid: id,
                future,
            },
            12 => CoreBluetoothMessage::RetrievePeripherals {
                options: RetrievePeripheralsOptions::default(),
                future,
            },
            13 => CoreBluetoothMessage::ClearPeripherals { future },
            _ => unreachable!(),
        };
        commands.try_send(command).unwrap();
        futures.push(reply);
    }
    commands
        .try_send(CoreBluetoothMessage::StartScanning {
            filter: ScanFilter::default(),
        })
        .unwrap();
    commands
        .try_send(CoreBluetoothMessage::StopScanning)
        .unwrap();
    drop(internal);
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    for (kind, mut future) in futures.into_iter().enumerate() {
        assert!(
            matches!(
                std::future::Future::poll(std::pin::Pin::new(&mut future), &mut context),
                Poll::Ready(CoreBluetoothReply::Err(message)) if message == "Apple adapter shut down"
            ),
            "buffered command {kind} did not receive a shutdown error"
        );
    }
    assert!(
        commands
            .try_send(CoreBluetoothMessage::StopScanning)
            .is_err()
    );
}

#[test]
fn reply_events_complete_once_across_dispatch_cancellation_and_receiver_loss() {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Waker};

    fn event(kind: usize, state: CoreBluetoothReplyStateShared) -> CoreBluetoothEvent {
        let future = EventReply::new(state);
        match kind {
            0 => CoreBluetoothEvent::PeripheralsCleared { future },
            1 => CoreBluetoothEvent::RetrievedPeripherals {
                peripherals: vec![],
                future,
            },
            _ => unreachable!(),
        }
    }
    fn complete(event: CoreBluetoothEvent) {
        match event {
            CoreBluetoothEvent::PeripheralsCleared { future }
            | CoreBluetoothEvent::RetrievedPeripherals { future, .. } => {
                future.complete(CoreBluetoothReply::Ok)
            }
            _ => unreachable!(),
        }
    }
    for kind in 0..2 {
        for path in 0..5 {
            let (events, mut receiver) = mpsc::channel(if path == 4 { 1 } else { 0 });
            let mut reply = CoreBluetoothReplyFuture::default();
            let event = event(kind, reply.get_state_clone());
            let mut context = Context::from_waker(Waker::noop());
            match path {
                0 => {
                    let mut dispatch = Box::pin(dispatch_event(&events, event));
                    assert!(dispatch.as_mut().poll(&mut context).is_pending());
                    assert!(Pin::new(&mut reply).poll(&mut context).is_pending());
                    drop(dispatch);
                    // Cancellation must terminate the reply even if Sink::send
                    // already put the event in the receiver's buffer.
                    assert!(matches!(
                        Pin::new(&mut reply).poll(&mut context),
                        Poll::Ready(CoreBluetoothReply::Err(_))
                    ));
                    if let Ok(late) = receiver.try_recv() {
                        complete(late);
                    }
                    assert!(Pin::new(&mut reply).poll(&mut context).is_pending());
                }
                1 => {
                    drop(receiver);
                    futures::executor::block_on(dispatch_event(&events, event));
                    assert!(matches!(
                        Pin::new(&mut reply).poll(&mut context),
                        Poll::Ready(CoreBluetoothReply::Err(_))
                    ));
                }
                4 => {
                    futures::executor::block_on(dispatch_event(&events, event));
                    assert!(Pin::new(&mut reply).poll(&mut context).is_pending());
                    drop(receiver);
                    assert!(matches!(
                        Pin::new(&mut reply).poll(&mut context),
                        Poll::Ready(CoreBluetoothReply::Err(_))
                    ));
                }
                2 | 3 => {
                    let (_, delivered) = futures::executor::block_on(async {
                        futures::join!(dispatch_event(&events, event), receiver.next())
                    });
                    let delivered = delivered.unwrap();
                    assert!(Pin::new(&mut reply).poll(&mut context).is_pending());
                    if path == 2 {
                        drop(delivered);
                        assert!(matches!(
                            Pin::new(&mut reply).poll(&mut context),
                            Poll::Ready(CoreBluetoothReply::Err(_))
                        ));
                    } else {
                        complete(delivered);
                        assert!(matches!(
                            Pin::new(&mut reply).poll(&mut context),
                            Poll::Ready(CoreBluetoothReply::Ok)
                        ));
                    }
                    assert!(Pin::new(&mut reply).poll(&mut context).is_pending());
                }
                _ => unreachable!(),
            }
        }
    }
}

// Mutable CoreBluetooth attributes throw when their remote-parent getters are
// used. Test subclasses override those getters to inject nullable remote values.
define_class!(
    #[unsafe(super(CBMutableCharacteristic))]
    #[thread_kind = AnyThread]
    #[ivars = ()]
    struct UnparentedTestCharacteristic;
    unsafe impl NSObjectProtocol for UnparentedTestCharacteristic {}
    impl UnparentedTestCharacteristic {
        #[unsafe(method_id(service))]
        fn missing_service(&self) -> Option<Retained<CBService>> { None }
    }
);
impl UnparentedTestCharacteristic {
    fn new(uuid: &CBUUID) -> Retained<Self> {
        let this = Self::alloc().set_ivars(());
        unsafe {
            msg_send![super(this), initWithType: uuid,
            properties: CBCharacteristicProperties::Write,
            value: None::<&NSData>, permissions: CBAttributePermissions::Writeable]
        }
    }
}

define_class!(
    #[unsafe(super(CBMutableDescriptor))]
    #[thread_kind = AnyThread]
    #[ivars = Option<Retained<CBCharacteristic>>]
    struct UnparentedTestDescriptor;
    unsafe impl NSObjectProtocol for UnparentedTestDescriptor {}
    impl UnparentedTestDescriptor {
        #[unsafe(method_id(characteristic))]
        fn injected_characteristic(&self) -> Option<Retained<CBCharacteristic>> { self.ivars().clone() }
    }
);
impl UnparentedTestDescriptor {
    fn new(uuid: &CBUUID, parent: Option<Retained<CBCharacteristic>>) -> Retained<Self> {
        let this = Self::alloc().set_ivars(parent);
        let value = NSString::from_str("test descriptor");
        unsafe { msg_send![super(this), initWithType: uuid, value: &*value] }
    }
}

#[test]
fn characteristic_write_callback_without_service_is_ignored_without_unwinding() {
    let peripheral =
        test_peripheral_with_uuid(Uuid::from_u128(0x12345678_1234_5678_1234_567812345678));
    // CBPeripheral's destructor removes this observer. Direct test construction
    // bypasses the private remote initializer which normally registers it.
    unsafe {
        let _: () = msg_send![&*peripheral, addObserver: &*peripheral,
            forKeyPath: ns_string!("delegate"), options: 0usize,
            context: std::ptr::null_mut::<std::ffi::c_void>()];
    }
    let uuid = uuid_to_cbuuid(Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb));
    let characteristic = UnparentedTestCharacteristic::new(&uuid);
    assert!(unsafe { characteristic.service() }.is_none());
    let (sender, mut receiver) = mpsc::channel(1);
    let delegate = CentralDelegate::new(sender);
    let error = NSError::new(1, ns_string!("OpenbleCallbackTests"));
    for _ in 0..100 {
        for error in [None, Some(&*error)] {
            unsafe {
                delegate.peripheral_didWriteValueForCharacteristic_error(
                    &peripheral,
                    &characteristic,
                    error,
                );
            }
        }
    }
    assert!(
        receiver.try_recv().is_err(),
        "stale callback published an event"
    );
}

#[test]
fn descriptor_write_callback_with_missing_parents_is_ignored_without_unwinding() {
    let peripheral =
        test_peripheral_with_uuid(Uuid::from_u128(0x12345678_1234_5678_1234_567812345678));
    // CBPeripheral's destructor removes this observer. Direct test construction
    // bypasses the private remote initializer which normally registers it.
    unsafe {
        let _: () = msg_send![&*peripheral, addObserver: &*peripheral,
            forKeyPath: ns_string!("delegate"), options: 0usize,
            context: std::ptr::null_mut::<std::ffi::c_void>()];
    }
    let characteristic_uuid =
        uuid_to_cbuuid(Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb));
    let characteristic = Retained::into_super(Retained::into_super(
        UnparentedTestCharacteristic::new(&characteristic_uuid),
    ));
    let uuid = uuid_to_cbuuid(Uuid::from_u128(0x00002901_0000_1000_8000_00805f9b34fb));
    let (sender, mut receiver) = mpsc::channel(1);
    let delegate = CentralDelegate::new(sender);
    let error = NSError::new(1, ns_string!("OpenbleCallbackTests"));
    for parent in [None, Some(characteristic)] {
        let descriptor = UnparentedTestDescriptor::new(&uuid, parent);
        for _ in 0..100 {
            for error in [None, Some(&*error)] {
                unsafe {
                    delegate.peripheral_didWriteValueForDescriptor_error(
                        &peripheral,
                        &descriptor,
                        error,
                    );
                }
            }
        }
    }
    assert!(
        receiver.try_recv().is_err(),
        "stale callback published an event"
    );
}

#[tokio::test]
#[ignore = "uses an actual CoreBluetooth manager and native dispatch queue"]
async fn callback_failure_terminates_actual_apple_executor_and_drains_queue() {
    let (_commands, message_receiver) = mpsc::channel(1);
    let (events, mut event_receiver) = mpsc::channel(256);
    let mut internal = CoreBluetoothInternal::new(message_receiver, events);
    let mut sender = internal.delegate.ivars().clone();
    sender
        .send(CentralDelegateEvent::CallbackFailed {
            callback: "injected_executor_failure",
        })
        .await
        .unwrap();
    tokio::time::timeout(CALLBACK_TIMEOUT, async {
        while internal.wait_for_message().await {}
    })
    .await
    .expect("fatal callback did not stop the Apple executor");
    // The real owner detaches the delegate, closes its receiver and drains its
    // serial native queue. No test object or manager is deliberately retained.
    drop(internal);
    drop(sender);
    let mut failure = None;
    while let Some(event) = event_receiver.next().await {
        if let CoreBluetoothEvent::CallbackFailed { message } = event {
            assert!(failure.is_none(), "fatal callback reported twice");
            failure = Some(message);
        }
    }
    assert_eq!(
        failure.as_deref(),
        Some("CoreBluetooth callback panicked: injected_executor_failure")
    );
}

#[tokio::test]
async fn missing_cccd_requires_explicit_policy_and_preserves_other_errors() {
    let uuid = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
    for enabled in [true, false] {
        for nonstandard_cccd in [false, true] {
            for (domain, code) in [
                ("CBATTErrorDomain", 10),
                ("CBATTErrorDomain", 15),
                ("OtherDomain", 10),
            ] {
                let mut fixture = NotificationFixture::new(&[uuid]);
                let future = CoreBluetoothReplyFuture::default();
                fixture.internal.queue_notification_request_with_policy(
                    fixture.service_uuid,
                    uuid,
                    enabled,
                    nonstandard_cccd,
                    future.get_state_clone(),
                );
                assert_eq!(
                    fixture.set_notify_calls(),
                    vec![RecordedSetNotify {
                        characteristic_uuid: uuid,
                        enabled
                    }]
                );
                let error = NSError::new(code, &NSString::from_str(domain));
                let original = deliver_notification_state_callback(
                    &mut fixture,
                    0,
                    Some(&error),
                    "policy matrix",
                )
                .await;
                assert!(original.is_some(), "Delegate must preserve the error");
                drop(error);
                let reply = future.await;
                let tolerated = nonstandard_cccd && domain == "CBATTErrorDomain" && code == 10;
                assert_eq!(matches!(reply, CoreBluetoothReply::Ok), tolerated);
                if !tolerated {
                    let CoreBluetoothReply::NativeErr(error) = &reply else {
                        panic!("native notification failure lost its diagnostics: {reply:?}");
                    };
                    assert_eq!(error.native_code, Some(format!("{domain}:{code}")));
                    assert_eq!(Some(&error.message), original.as_ref());
                }
                assert!(
                    fixture.internal.services[&fixture.service_uuid].characteristics[&uuid]
                        .notification_requests
                        .is_empty()
                );
            }
        }
    }
}

#[tokio::test]
async fn nonstandard_cccd_rejects_non_notify_before_native_setup() {
    let uuid = Uuid::from_u128(123);
    let mut fixture =
        NotificationFixture::with_properties(&[uuid], CBCharacteristicProperties::Read);
    let future = CoreBluetoothReplyFuture::default();
    fixture.internal.queue_notification_request_with_policy(
        fixture.service_uuid,
        uuid,
        true,
        true,
        future.get_state_clone(),
    );
    assert!(matches!(future.await, CoreBluetoothReply::Err(_)));
    assert!(fixture.set_notify_calls().is_empty());
}

#[tokio::test]
async fn queued_notification_policies_do_not_leak_between_owners() {
    let uuid = Uuid::from_u128(456);
    let mut fixture = NotificationFixture::new(&[uuid]);
    let compat = CoreBluetoothReplyFuture::default();
    let strict = CoreBluetoothReplyFuture::default();
    fixture.internal.queue_notification_request_with_policy(
        fixture.service_uuid,
        uuid,
        true,
        true,
        compat.get_state_clone(),
    );
    fixture.internal.queue_notification_request(
        fixture.service_uuid,
        uuid,
        false,
        strict.get_state_clone(),
    );
    assert_eq!(fixture.set_notify_calls().len(), 1);
    let error = NSError::new(10, &NSString::from_str("CBATTErrorDomain"));
    deliver_notification_state_callback(&mut fixture, 0, Some(&error), "compatible owner").await;
    assert!(matches!(compat.await, CoreBluetoothReply::Ok));
    assert_eq!(fixture.set_notify_calls().len(), 2);
    deliver_notification_state_callback(&mut fixture, 0, Some(&error), "strict owner").await;
    assert!(matches!(strict.await, CoreBluetoothReply::NativeErr(_)));
}

#[tokio::test]
async fn gatt_delegate_errors_copy_diagnostics_before_nserror_destruction() {
    let uuid = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
    let mut fixture = NotificationFixture::with_properties(
        &[uuid],
        CBCharacteristicProperties::Read | CBCharacteristicProperties::Write,
    );
    let characteristic = Retained::into_super(Retained::into_super(
        fixture.characteristics[0].characteristic.clone(),
    ));
    let descriptor = UnparentedTestDescriptor::new(
        &uuid_to_cbuuid(Uuid::from_u128(0x00002901_0000_1000_8000_00805f9b34fb)),
        Some(characteristic.clone()),
    );
    for path in 0..5 {
        let native = NSError::new(-7, ns_string!("ControlledGattDomain"));
        let original = native.localizedDescription().to_string();
        unsafe {
            match path {
                0 => fixture
                    .delegate
                    .peripheral_didUpdateValueForCharacteristic_error(
                        &fixture.peripheral,
                        &characteristic,
                        Some(&native),
                    ),
                1 => fixture
                    .delegate
                    .peripheral_didWriteValueForCharacteristic_error(
                        &fixture.peripheral,
                        &characteristic,
                        Some(&native),
                    ),
                2 => fixture
                    .delegate
                    .peripheral_didUpdateValueForDescriptor_error(
                        &fixture.peripheral,
                        &descriptor,
                        Some(&native),
                    ),
                3 => fixture
                    .delegate
                    .peripheral_didWriteValueForDescriptor_error(
                        &fixture.peripheral,
                        &descriptor,
                        Some(&native),
                    ),
                4 => fixture.delegate.peripheral_didReadRSSI_error(
                    &fixture.peripheral,
                    &objc2_foundation::NSNumber::new_i16(-67),
                    Some(&native),
                ),
                _ => unreachable!(),
            }
        }
        drop(native);
        let event = tokio::time::timeout(CALLBACK_TIMEOUT, fixture.delegate_receiver.next())
            .await
            .unwrap()
            .unwrap();
        let error = match (path, event) {
            (0, CentralDelegateEvent::CharacteristicNotified { error, .. })
            | (1, CentralDelegateEvent::CharacteristicWritten { error, .. })
            | (2, CentralDelegateEvent::DescriptorNotified { error, .. })
            | (3, CentralDelegateEvent::DescriptorWritten { error, .. })
            | (4, CentralDelegateEvent::DidReadRssi { error, .. }) => error.unwrap(),
            other => panic!("unexpected callback: {other:?}"),
        };
        assert_eq!(error.message, original);
        assert_eq!(
            error.native_code.as_deref(),
            Some("ControlledGattDomain:-7")
        );
    }
}

#[tokio::test]
async fn failed_rssi_does_not_publish_value_or_consume_the_next_live_request() {
    let mut fixture = NotificationFixture::new(&[Uuid::from_u128(7)]);
    let (sender, mut receiver) = mpsc::channel(4);
    fixture.internal.event_sender = sender;
    for _ in 0..100 {
        let abandoned = CoreBluetoothReplyFuture::default();
        fixture
            .internal
            .read_rssi_future_state
            .push_back(abandoned.get_state_clone());
        drop(abandoned);
        let live = CoreBluetoothReplyFuture::default();
        fixture
            .internal
            .read_rssi_future_state
            .push_back(live.get_state_clone());
        let native = NSError::new(6, ns_string!("ControlledRssiDomain"));
        let error = super::super::native_error::NativeError::copy(&native);
        drop(native);
        fixture.internal.on_read_rssi(0, Some(error.clone())).await;
        assert_eq!(fixture.internal.read_rssi_future_state.len(), 1);
        assert!(
            receiver.try_recv().is_err(),
            "failed RSSI published a value"
        );
        fixture.internal.on_read_rssi(0, Some(error.clone())).await;
        let CoreBluetoothReply::NativeErr(actual) = live.await else {
            panic!("lost native RSSI error");
        };
        assert_eq!(actual.native_code, error.native_code);
        assert!(
            receiver.try_recv().is_err(),
            "failed live RSSI published a value"
        );
        fixture.internal.on_read_rssi(0, Some(error)).await;
        assert!(
            receiver.try_recv().is_err(),
            "unsolicited failed RSSI published a value"
        );
        let retry = CoreBluetoothReplyFuture::default();
        fixture
            .internal
            .read_rssi_future_state
            .push_back(retry.get_state_clone());
        fixture.internal.on_read_rssi(-67, None).await;
        assert!(matches!(retry.await, CoreBluetoothReply::ReadRssi(-67)));
        assert!(matches!(
            receiver.next().await,
            Some(PeripheralEventInternal::RssiRead(-67))
        ));
        assert!(fixture.internal.read_rssi_future_state.is_empty());
    }
}

#[tokio::test]
async fn confirmed_disconnect_acknowledges_waiters_and_fails_dependent_work() {
    let mut fixture = NotificationFixture::new(&[Uuid::from_u128(7)]);
    let cancelled = CoreBluetoothReplyFuture::default();
    fixture
        .internal
        .disconnected_future_state
        .push_back(cancelled.get_state_clone());
    drop(cancelled);
    let first = CoreBluetoothReplyFuture::default();
    let second = CoreBluetoothReplyFuture::default();
    fixture
        .internal
        .disconnected_future_state
        .push_back(first.get_state_clone());
    fixture
        .internal
        .disconnected_future_state
        .push_back(second.get_state_clone());
    let discovery = CoreBluetoothReplyFuture::default();
    fixture
        .internal
        .services_discovered_future_state
        .push_back(discovery.get_state_clone());
    fixture.internal.confirm_disconnect();
    assert!(matches!(first.await, CoreBluetoothReply::Ok));
    assert!(matches!(second.await, CoreBluetoothReply::Ok));
    assert!(
        matches!(discovery.await, CoreBluetoothReply::Err(message) if message == "Device disconnected")
    );
    assert!(fixture.internal.disconnected_future_state.is_empty());
    assert!(fixture.internal.services_discovered_future_state.is_empty());
}

#[tokio::test]
async fn failed_connect_retires_cancel_cleanup_and_allows_immediate_retry() {
    let mut fixture = NotificationFixture::new(&[Uuid::from_u128(7)]);
    for _ in 0..100 {
        let abandoned = CoreBluetoothReplyFuture::default();
        fixture
            .internal
            .connected_future_state
            .push_back(abandoned.get_state_clone());
        drop(abandoned);
        let connect = CoreBluetoothReplyFuture::default();
        fixture
            .internal
            .connected_future_state
            .push_back(connect.get_state_clone());
        let cleanup = CoreBluetoothReplyFuture::default();
        assert!(
            fixture.internal.queue_disconnect(cleanup.get_state_clone()),
            "pending connect still needs cancellation"
        );
        let discovery = CoreBluetoothReplyFuture::default();
        fixture
            .internal
            .services_discovered_future_state
            .push_back(discovery.get_state_clone());
        let notification = CoreBluetoothReplyFuture::default();
        fixture.internal.queue_notification_request(
            fixture.service_uuid,
            fixture.characteristics[0].uuid,
            true,
            notification.get_state_clone(),
        );
        let native = NSError::new(6, ns_string!("ControlledConnectionDomain"));
        let cause = super::super::native_error::NativeError::copy(&native);
        drop(native);
        assert!(
            fixture
                .internal
                .confirm_connection_failed(CoreBluetoothReply::NativeErr(cause))
        );
        assert!(matches!(cleanup.await, CoreBluetoothReply::Ok));
        for future in [connect, discovery, notification] {
            let reply = tokio::time::timeout(CALLBACK_TIMEOUT, future)
                .await
                .expect("failed connect left dependent work pending");
            assert!(
                matches!(reply, CoreBluetoothReply::NativeErr(error) if error.native_code.as_deref() == Some("ControlledConnectionDomain:6"))
            );
        }
        assert!(fixture.internal.connected_future_state.is_empty());
        assert!(fixture.internal.disconnected_future_state.is_empty());
        let redundant_cleanup = CoreBluetoothReplyFuture::default();
        assert!(
            !fixture
                .internal
                .queue_disconnect(redundant_cleanup.get_state_clone()),
            "already-disconnected cleanup awaited a nonexistent callback"
        );
        assert!(matches!(redundant_cleanup.await, CoreBluetoothReply::Ok));
        let retry = CoreBluetoothReplyFuture::default();
        fixture
            .internal
            .connected_future_state
            .push_back(retry.get_state_clone());
        fixture
            .internal
            .complete_connect(CoreBluetoothReply::Connected);
        assert!(matches!(retry.await, CoreBluetoothReply::Connected));
        *fixture.peripheral.ivars().state.lock().unwrap() = CBPeripheralState::Connected;
        let mut active_disconnect = CoreBluetoothReplyFuture::default();
        assert!(
            fixture
                .internal
                .queue_disconnect(active_disconnect.get_state_clone())
        );
        assert!(
            !fixture
                .internal
                .confirm_connection_failed(CoreBluetoothReply::Err("late old failure".into()))
        );
        assert_pending(
            &mut active_disconnect,
            "duplicate failure cannot acknowledge a live disconnect",
        )
        .await;
        fixture.internal.confirm_disconnect();
        assert!(matches!(active_disconnect.await, CoreBluetoothReply::Ok));
        *fixture.peripheral.ivars().state.lock().unwrap() = CBPeripheralState::Disconnected;
    }
}

#[tokio::test]
async fn connection_and_service_delegate_errors_keep_owned_native_causes() {
    let mut fixture = NotificationFixture::new(&[Uuid::from_u128(7)]);
    for discovery in [false, true] {
        let native = NSError::new(3, ns_string!("ControlledLifecycleDomain"));
        let message = native.localizedDescription().to_string();
        unsafe {
            if discovery {
                fixture
                    .delegate
                    .peripheral_didDiscoverServices(&fixture.peripheral, Some(&native));
            } else {
                let unused_manager = objc2_foundation::NSObject::new();
                let _: () = msg_send![&*fixture.delegate, centralManager: &*unused_manager, didFailToConnectPeripheral: &*fixture.peripheral, error: &*native];
            }
        }
        drop(native);
        let event = tokio::time::timeout(CALLBACK_TIMEOUT, fixture.delegate_receiver.next())
            .await
            .unwrap()
            .unwrap();
        let error = match event {
            CentralDelegateEvent::DiscoveredServices { error, .. } if discovery => error.unwrap(),
            CentralDelegateEvent::ConnectionFailed {
                error_description, ..
            } if !discovery => error_description.unwrap(),
            other => panic!("unexpected lifecycle callback: {other:?}"),
        };
        assert_eq!(error.message, message);
        assert_eq!(
            error.native_code.as_deref(),
            Some("ControlledLifecycleDomain:3")
        );
    }
}

#[tokio::test]
async fn discovery_failure_preserves_cache_reads_and_explicit_retry_readiness() {
    let first_uuid = Uuid::from_u128(0x00002a19_0000_1000_8000_00805f9b34fb);
    let second_uuid = Uuid::from_u128(0x00002a1a_0000_1000_8000_00805f9b34fb);
    let descriptor_uuid = Uuid::from_u128(0x00002901_0000_1000_8000_00805f9b34fb);
    let mut fixture = NotificationFixture::new(&[first_uuid, second_uuid]);
    let first_native = Retained::into_super(Retained::into_super(
        fixture.characteristics[0].characteristic.clone(),
    ));
    let second_native = Retained::into_super(Retained::into_super(
        fixture.characteristics[1].characteristic.clone(),
    ));
    let descriptor =
        UnparentedTestDescriptor::new(&uuid_to_cbuuid(descriptor_uuid), Some(first_native.clone()));
    let descriptor: Retained<CBDescriptor> = Retained::into_super(Retained::into_super(descriptor));
    fixture
        .internal
        .services
        .get_mut(&fixture.service_uuid)
        .unwrap()
        .characteristics
        .get_mut(&first_uuid)
        .unwrap()
        .descriptors
        .insert(descriptor_uuid, DescriptorInternal::new(descriptor.clone()));
    for descriptor_failure in [false, true] {
        for _ in 0..100 {
            let abandoned = CoreBluetoothReplyFuture::default();
            fixture
                .internal
                .services_discovered_future_state
                .push_back(abandoned.get_state_clone());
            drop(abandoned);
            let first = CoreBluetoothReplyFuture::default();
            let second = CoreBluetoothReplyFuture::default();
            fixture
                .internal
                .services_discovered_future_state
                .push_back(first.get_state_clone());
            fixture
                .internal
                .services_discovered_future_state
                .push_back(second.get_state_clone());
            let mut read = CoreBluetoothReplyFuture::default();
            let mut descriptor_read = CoreBluetoothReplyFuture::default();
            let service = fixture
                .internal
                .services
                .get_mut(&fixture.service_uuid)
                .unwrap();
            service.discovered = false;
            let characteristic = service.characteristics.get_mut(&first_uuid).unwrap();
            characteristic.discovered = false;
            characteristic
                .read_future_state
                .push_back(read.get_state_clone());
            characteristic
                .descriptors
                .get_mut(&descriptor_uuid)
                .unwrap()
                .read_future_state
                .push_back(descriptor_read.get_state_clone());
            let native = NSError::new(10, ns_string!("CBATTErrorDomain"));
            let cause = super::super::native_error::NativeError::copy(&native);
            drop(native);
            if descriptor_failure {
                assert!(fixture.internal.set_characteristic_descriptors(
                    fixture.service_uuid,
                    first_uuid,
                    HashMap::new(),
                    Some(cause)
                ));
            } else {
                fixture.internal.set_characteristics(
                    fixture.service_uuid,
                    HashMap::new(),
                    Some(cause),
                );
            }
            for future in [first, second] {
                let reply = tokio::time::timeout(CALLBACK_TIMEOUT, future)
                    .await
                    .unwrap();
                assert!(
                    matches!(reply, CoreBluetoothReply::NativeErr(error) if error.native_code.as_deref() == Some("CBATTErrorDomain:10")),
                    "discovery errors must never use notification compatibility"
                );
            }
            assert_pending(&mut read, "characteristic read after failed discovery").await;
            assert_pending(
                &mut descriptor_read,
                "descriptor read after failed discovery",
            )
            .await;
            assert_eq!(
                fixture.internal.services[&fixture.service_uuid]
                    .characteristics
                    .len(),
                2
            );
            assert_eq!(
                fixture.internal.services[&fixture.service_uuid].characteristics[&first_uuid]
                    .descriptors
                    .len(),
                1
            );
            assert!(fixture.internal.services_discovered_future_state.is_empty());
            let mut retry = CoreBluetoothReplyFuture::default();
            fixture
                .internal
                .services_discovered_future_state
                .push_back(retry.get_state_clone());
            let service = fixture.internal.services[&fixture.service_uuid]
                .cbservice
                .clone();
            fixture
                .internal
                .set_discovered_services(HashMap::from([(fixture.service_uuid, service)]), None);
            fixture.internal.set_characteristics(
                fixture.service_uuid,
                HashMap::from([
                    (first_uuid, first_native.clone()),
                    (second_uuid, second_native.clone()),
                ]),
                None,
            );
            assert!(fixture.internal.set_characteristic_descriptors(
                fixture.service_uuid,
                second_uuid,
                HashMap::new(),
                None
            ));
            assert_pending(&mut retry, "retry must await every characteristic").await;
            assert!(fixture.internal.set_characteristic_descriptors(
                fixture.service_uuid,
                first_uuid,
                HashMap::from([(descriptor_uuid, descriptor.clone())]),
                None
            ));
            let reply = tokio::time::timeout(CALLBACK_TIMEOUT, retry).await.unwrap();
            assert!(
                matches!(reply, CoreBluetoothReply::ServicesDiscovered(services, _) if services.iter().any(|service| service.characteristics.iter().any(|characteristic| characteristic.uuid == first_uuid && characteristic.descriptors.iter().any(|descriptor| descriptor.uuid == descriptor_uuid))))
            );
            let characteristic = fixture
                .internal
                .services
                .get_mut(&fixture.service_uuid)
                .unwrap()
                .characteristics
                .get_mut(&first_uuid)
                .unwrap();
            let state = characteristic.read_future_state.pop_front().unwrap();
            crate::corebluetooth::future::set_reply(
                &state,
                CoreBluetoothReply::ReadResult(vec![0xaa]),
            );
            let state = characteristic
                .descriptors
                .get_mut(&descriptor_uuid)
                .unwrap()
                .read_future_state
                .pop_front()
                .unwrap();
            crate::corebluetooth::future::set_reply(
                &state,
                CoreBluetoothReply::ReadResult(vec![0xbb]),
            );
            assert!(matches!(read.await, CoreBluetoothReply::ReadResult(data) if data == [0xaa]));
            assert!(
                matches!(descriptor_read.await, CoreBluetoothReply::ReadResult(data) if data == [0xbb])
            );
            assert!(fixture.internal.services_discovered_future_state.is_empty());
        }
    }
}

#[tokio::test]
async fn disconnect_callback_preserves_cause_retires_waiters_and_allows_retry() {
    for domain in [
        None,
        Some("CBErrorDomain"),
        Some("CBATTErrorDomain"),
        Some("OtherDomain"),
    ] {
        for _ in 0..100 {
            let mut fixture = NotificationFixture::new(&[Uuid::from_u128(7)]);
            *fixture.peripheral.ivars().state.lock().unwrap() = CBPeripheralState::Connected;
            let cleanup = CoreBluetoothReplyFuture::default();
            assert!(fixture.internal.queue_disconnect(cleanup.get_state_clone()));
            let abandoned = CoreBluetoothReplyFuture::default();
            fixture
                .internal
                .read_rssi_future_state
                .push_back(abandoned.get_state_clone());
            drop(abandoned);
            let discovery = CoreBluetoothReplyFuture::default();
            fixture
                .internal
                .services_discovered_future_state
                .push_back(discovery.get_state_clone());
            let rssi = CoreBluetoothReplyFuture::default();
            fixture
                .internal
                .read_rssi_future_state
                .push_back(rssi.get_state_clone());
            let notification = CoreBluetoothReplyFuture::default();
            fixture.internal.queue_notification_request_with_policy(
                fixture.service_uuid,
                fixture.characteristics[0].uuid,
                true,
                true,
                notification.get_state_clone(),
            );
            let queued = CoreBluetoothReplyFuture::default();
            fixture.internal.queue_notification_request_with_policy(
                fixture.service_uuid,
                fixture.characteristics[0].uuid,
                false,
                true,
                queued.get_state_clone(),
            );
            let submitted = fixture
                .peripheral
                .ivars()
                .set_notify_calls
                .lock()
                .unwrap()
                .len();
            let native = domain.map(|domain| NSError::new(10, &NSString::from_str(domain)));
            let message = native
                .as_ref()
                .map(|error| error.localizedDescription().to_string());
            unsafe {
                let unused_manager = objc2_foundation::NSObject::new();
                let _: () = msg_send![&*fixture.delegate, centralManager: &*unused_manager,
                    didDisconnectPeripheral: &*fixture.peripheral, error: native.as_deref()];
            }
            drop(native);
            let event = tokio::time::timeout(CALLBACK_TIMEOUT, fixture.delegate_receiver.next())
                .await
                .unwrap()
                .unwrap();
            let CentralDelegateEvent::DisconnectedDevice {
                peripheral_uuid,
                error,
            } = event
            else {
                panic!("unexpected disconnect callback");
            };
            assert_eq!(peripheral_uuid, fixture.peripheral_uuid);
            assert_eq!(error.as_ref().map(|error| error.message.clone()), message);
            fixture.internal.confirm_disconnect_with_error(error);
            assert!(matches!(cleanup.await, CoreBluetoothReply::Ok));
            for future in [discovery, rssi, notification, queued] {
                let reply = tokio::time::timeout(CALLBACK_TIMEOUT, future)
                    .await
                    .unwrap();
                match (domain, reply) {
                    (Some(domain), CoreBluetoothReply::NativeErr(error)) => {
                        assert_eq!(
                            error.native_code.as_deref(),
                            Some(format!("{domain}:10").as_str())
                        );
                        assert_eq!(Some(error.message), message);
                    }
                    (None, CoreBluetoothReply::Err(error)) => {
                        assert_eq!(error, "Device disconnected")
                    }
                    (_, other) => panic!("disconnect lost original cause: {other:?}"),
                }
            }
            assert!(fixture.internal.disconnected_future_state.is_empty());
            assert!(fixture.internal.read_rssi_future_state.is_empty());
            assert!(fixture.internal.services_discovered_future_state.is_empty());
            assert_eq!(
                fixture
                    .peripheral
                    .ivars()
                    .set_notify_calls
                    .lock()
                    .unwrap()
                    .len(),
                submitted,
                "disconnect submitted a queued notification request"
            );
            let retry = CoreBluetoothReplyFuture::default();
            fixture
                .internal
                .connected_future_state
                .push_back(retry.get_state_clone());
            fixture
                .internal
                .complete_connect(CoreBluetoothReply::Connected);
            assert!(matches!(retry.await, CoreBluetoothReply::Connected));
        }
    }
}

#[tokio::test]
async fn disconnect_retirement_precedes_blocked_event_delivery() {
    for _ in 0..100 {
        let mut fixture = NotificationFixture::new(&[Uuid::from_u128(7)]);
        let (mut sender, mut receiver) = mpsc::channel(0);
        sender
            .try_send(PeripheralEventInternal::RssiRead(-60))
            .unwrap();
        assert!(
            sender
                .try_send(PeripheralEventInternal::RssiRead(-61))
                .is_err()
        );
        fixture.internal.event_sender = sender;
        let cleanup = CoreBluetoothReplyFuture::default();
        fixture
            .internal
            .disconnected_future_state
            .push_back(cleanup.get_state_clone());
        let pending = CoreBluetoothReplyFuture::default();
        fixture
            .internal
            .services_discovered_future_state
            .push_back(pending.get_state_clone());
        let id = fixture.peripheral_uuid;
        let mut peripherals = HashMap::new();
        peripherals.insert(id, fixture.internal);
        let event = retire_disconnected(&mut peripherals, id, None).unwrap();
        assert!(peripherals.is_empty());
        assert!(matches!(
            tokio::time::timeout(CALLBACK_TIMEOUT, cleanup)
                .await
                .expect("disconnect ACK did not retire"),
            CoreBluetoothReply::Ok
        ));
        assert!(
            matches!(tokio::time::timeout(CALLBACK_TIMEOUT, pending).await.expect("dependent operation did not retire"), CoreBluetoothReply::Err(message) if message == "Device disconnected")
        );
        assert!(matches!(
            receiver.next().await,
            Some(PeripheralEventInternal::RssiRead(-60))
        ));
        assert!(
            tokio::time::timeout(CALLBACK_TIMEOUT, receiver.next())
                .await
                .expect("internal sender was retained")
                .is_none(),
            "internal sender survived retirement"
        );
        assert!(
            retire_disconnected(&mut peripherals, id, None).is_none(),
            "duplicate disconnect published twice"
        );
        let (mut events, mut event_receiver) = mpsc::channel(0);
        events
            .try_send(CoreBluetoothEvent::DidUpdateState {
                state: CBManagerState::PoweredOn,
            })
            .unwrap();
        let mut publish = Box::pin(dispatch_event(&events, event));
        assert!(
            matches!(futures::poll!(publish.as_mut()), Poll::Pending),
            "adapter consumer is intentionally blocked"
        );
        // Cleanup and removal above complete even while public publication remains pending.
        assert!(matches!(
            event_receiver.next().await,
            Some(CoreBluetoothEvent::DidUpdateState { .. })
        ));
        let (_, delivered) = tokio::time::timeout(CALLBACK_TIMEOUT, async {
            futures::join!(publish, event_receiver.next())
        })
        .await
        .unwrap();
        assert!(
            matches!(delivered, Some(CoreBluetoothEvent::DeviceDisconnected { uuid }) if uuid == id)
        );
    }
}
