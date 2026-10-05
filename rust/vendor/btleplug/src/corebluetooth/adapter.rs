use super::internal::{
    CoreBluetoothEvent, CoreBluetoothMessage, CoreBluetoothReply, CoreBluetoothReplyFuture,
    CoreBluetoothThread, run_corebluetooth_thread,
};
use super::peripheral::{Peripheral, PeripheralId};
use super::tasks::Tasks;
use crate::api::{
    BDAddr, Central, CentralEvent, CentralState, Peripheral as PeripheralTrait,
    RetrievePeripheralsOptions, ScanFilter,
};
use crate::common::adapter_manager::AdapterManager;
use crate::{Error, Result};
use async_trait::async_trait;
use futures::channel::mpsc::{self, Sender};
use futures::sink::SinkExt;
use futures::stream::{Stream, StreamExt};
use log::*;
use objc2_core_bluetooth::CBManagerState;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;

/// Implementation of [api::Central](crate::api::Central).
#[derive(Clone, Debug)]
pub struct Adapter {
    manager: Arc<AdapterManager<Peripheral>>,
    sender: Sender<CoreBluetoothMessage>,
    owner: Arc<Owner>,
}

#[derive(Debug)]
struct Owner {
    tasks: Arc<Tasks>,
    thread: Mutex<Option<CoreBluetoothThread>>,
    closing: tokio::sync::Mutex<()>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.tasks.abort();
        // The thread's Drop signals independent cancellation and joins even
        // when adapter initialization was cancelled before returning an adapter.
    }
}

fn get_central_state(state: CBManagerState) -> CentralState {
    match state {
        CBManagerState::PoweredOn => CentralState::PoweredOn,
        CBManagerState::PoweredOff => CentralState::PoweredOff,
        _ => CentralState::Unknown,
    }
}

impl Adapter {
    /// Patched upstream lifecycle API, used only by openble's adapter owner.
    pub async fn shutdown(&self) -> Result<()> {
        let _closing = self.owner.closing.lock().await;
        {
            let mut thread = self
                .owner
                .thread
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if let Some(thread) = thread.as_mut() {
                thread.stop();
            }
        }
        let tasks = self.owner.tasks.close().await;
        self.manager.clear_peripherals();
        let thread = self
            .owner
            .thread
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        let joined = match thread {
            Some(thread) => tokio::task::spawn_blocking(move || thread.join())
                .await
                .map_err(|error| {
                    Error::RuntimeError(format!("CoreBluetooth join failed: {error}"))
                })?,
            None => Ok(()),
        };
        tasks.and(joined)
    }

    pub(crate) async fn new() -> Result<Self> {
        let (sender, mut receiver) = mpsc::channel(256);
        let thread = run_corebluetooth_thread(sender)?;
        let adapter_sender = thread.sender.clone();
        let owner = Arc::new(Owner {
            tasks: Arc::new(Tasks::default()),
            thread: Mutex::new(Some(thread)),
            closing: tokio::sync::Mutex::new(()),
        });
        // Since init currently blocked until the state update, we know the
        // receiver is dropped after that. We can pick it up here and make it
        // part of our event loop to update our peripherals.
        debug!("Waiting on adapter connect");
        match receiver.next().await {
            Some(CoreBluetoothEvent::DidUpdateState { .. }) => {}
            Some(CoreBluetoothEvent::CallbackFailed { message }) => {
                return Err(Error::RuntimeError(message));
            }
            _ => {
                return Err(Error::Other(
                    "Adapter failed to connect.".to_string().into(),
                ));
            }
        }
        debug!("Adapter connected");
        let manager = Arc::new(AdapterManager::default());

        let manager_clone = manager.clone();
        let adapter_sender_clone = adapter_sender.clone();
        let tasks = owner.tasks.clone();
        owner.tasks.spawn(async move {
            let mut handles = HashMap::new();
            while let Some(msg) = receiver.next().await {
                match msg {
                    CoreBluetoothEvent::CallbackFailed { message } => {
                        manager_clone.emit(CentralEvent::AdapterError { message });
                    }
                    CoreBluetoothEvent::DeviceDiscovered {
                        uuid,
                        local_name,
                        advertisement_name,
                        event_receiver,
                    } => {
                        if manager_clone.peripheral(&uuid.into()).is_none() {
                            let peripheral = Peripheral::new(
                                uuid,
                                local_name,
                                advertisement_name,
                                Arc::downgrade(&manager_clone),
                                event_receiver,
                                adapter_sender_clone.clone(),
                                &tasks,
                            );
                            let peripheral = manager_clone.add_peripheral(peripheral);
                            handles.insert(peripheral.id(), peripheral);
                            manager_clone.emit(CentralEvent::DeviceDiscovered(uuid.into()));
                        }
                    }
                    CoreBluetoothEvent::RetrievedPeripherals {
                        peripherals,
                        future,
                    } => {
                        let mut result = Vec::with_capacity(peripherals.len());
                        for retrieved in peripherals {
                            let id = retrieved.uuid.into();
                            let peripheral = if let Some(peripheral) = handles.get(&id).cloned() {
                                peripheral.update_name(
                                    retrieved.local_name.clone(),
                                    retrieved.advertisement_name.clone(),
                                );
                                peripheral
                            } else if let Some(event_receiver) = retrieved.event_receiver {
                                let peripheral = Peripheral::new(
                                    retrieved.uuid,
                                    retrieved.local_name,
                                    retrieved.advertisement_name,
                                    Arc::downgrade(&manager_clone),
                                    event_receiver,
                                    adapter_sender_clone.clone(),
                                    &tasks,
                                );
                                handles.insert(id.clone(), peripheral.clone());
                                peripheral
                            } else {
                                continue;
                            };

                            let peripheral = if manager_clone.peripheral(&id).is_none() {
                                let peripheral = manager_clone.add_peripheral(peripheral);
                                manager_clone.emit(CentralEvent::DeviceDiscovered(id));
                                peripheral
                            } else {
                                peripheral
                            };
                            result.push(peripheral);
                        }
                        future.complete(CoreBluetoothReply::Peripherals(result));
                    }
                    CoreBluetoothEvent::DeviceUpdated {
                        uuid,
                        local_name,
                        advertisement_name,
                    } => {
                        let id = uuid.into();
                        if let Some(entry) = manager_clone.peripheral_mut(&id) {
                            entry.value().update_name(local_name, advertisement_name);
                            manager_clone.emit(CentralEvent::DeviceUpdated(id));
                        }
                    }
                    CoreBluetoothEvent::DeviceDisconnected { uuid } => {
                        handles.remove(&uuid.into());
                        manager_clone.emit(CentralEvent::DeviceDisconnected(uuid.into()));
                    }
                    CoreBluetoothEvent::PeripheralsCleared { future } => {
                        manager_clone.clear_peripherals();
                        handles.clear();
                        future.complete(CoreBluetoothReply::Ok);
                    }
                    CoreBluetoothEvent::DidUpdateState { state } => {
                        let central_state = get_central_state(state);
                        manager_clone.emit(CentralEvent::StateUpdate(central_state));
                    }
                }
            }
        });

        Ok(Adapter {
            manager,
            sender: adapter_sender,
            owner,
        })
    }
}

#[async_trait]
impl Central for Adapter {
    type Peripheral = Peripheral;

    async fn events(&self) -> Result<Pin<Box<dyn Stream<Item = CentralEvent> + Send>>> {
        Ok(self.manager.event_stream())
    }

    async fn start_scan(&self, filter: ScanFilter) -> Result<()> {
        self.sender
            .to_owned()
            .send(CoreBluetoothMessage::StartScanning { filter })
            .await?;
        Ok(())
    }

    async fn stop_scan(&self) -> Result<()> {
        self.sender
            .to_owned()
            .send(CoreBluetoothMessage::StopScanning)
            .await?;
        Ok(())
    }

    async fn peripherals(&self) -> Result<Vec<Peripheral>> {
        Ok(self.manager.peripherals())
    }

    async fn retrieve_peripherals(
        &self,
        options: RetrievePeripheralsOptions,
    ) -> Result<Vec<Peripheral>> {
        if options.identifiers.is_none() && options.services.is_none() {
            return Err(Error::NotSupported("retrieve_peripherals".to_string()));
        }
        if options.identifiers.as_ref().is_some_and(Vec::is_empty)
            && options.services.as_ref().is_none_or(Vec::is_empty)
        {
            return Ok(Vec::new());
        }
        let fut = CoreBluetoothReplyFuture::default();
        self.sender
            .to_owned()
            .send(CoreBluetoothMessage::RetrievePeripherals {
                options,
                future: fut.get_state_clone(),
            })
            .await?;
        match fut.await {
            CoreBluetoothReply::Peripherals(peripherals) => Ok(peripherals),
            CoreBluetoothReply::Err(msg) => Err(Error::RuntimeError(msg)),
            CoreBluetoothReply::Ok => Ok(Vec::new()),
            _ => Err(Error::RuntimeError(
                "Unexpected CoreBluetooth retrieval reply".to_string(),
            )),
        }
    }

    async fn peripheral(&self, id: &PeripheralId) -> Result<Peripheral> {
        if let Some(peripheral) = self.manager.peripheral(id) {
            return Ok(peripheral);
        }
        self.retrieve_peripherals(RetrievePeripheralsOptions {
            identifiers: Some(vec![id.clone()]),
            services: None,
        })
        .await?
        .into_iter()
        .find(|peripheral| peripheral.id() == *id)
        .ok_or(Error::DeviceNotFound)
    }

    async fn add_peripheral(&self, _address: &PeripheralId) -> Result<Peripheral> {
        Err(Error::NotSupported(
            "Can't add a Peripheral from a PeripheralId".to_string(),
        ))
    }

    async fn clear_peripherals(&self) -> Result<()> {
        let fut = CoreBluetoothReplyFuture::default();
        self.sender
            .to_owned()
            .send(CoreBluetoothMessage::ClearPeripherals {
                future: fut.get_state_clone(),
            })
            .await
            .map_err(|e| Error::Other(Box::new(e)))?;
        match fut.await {
            CoreBluetoothReply::Ok => Ok(()),
            _ => Err(Error::RuntimeError(
                "Unexpected CoreBluetooth clear reply".to_string(),
            )),
        }
    }

    async fn adapter_info(&self) -> Result<String> {
        // TODO: Get information about the adapter.
        Ok("CoreBluetooth".to_string())
    }

    async fn adapter_address(&self) -> Result<Option<BDAddr>> {
        // CoreBluetooth exposes opaque UUID identities, not controller addresses.
        Ok(None)
    }

    async fn adapter_state(&self) -> Result<CentralState> {
        let fut = CoreBluetoothReplyFuture::default();
        self.sender
            .to_owned()
            .send(CoreBluetoothMessage::GetAdapterState {
                future: fut.get_state_clone(),
            })
            .await?;

        match fut.await {
            CoreBluetoothReply::AdapterState(state) => {
                let central_state = get_central_state(state);
                return Ok(central_state.clone());
            }
            CoreBluetoothReply::Err(message) => Err(Error::RuntimeError(message)),
            _ => Err(Error::RuntimeError(
                "Unexpected CoreBluetooth adapter state reply".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[tokio::test]
    async fn adapter_state_reports_error_and_unexpected_replies_without_panicking() {
        for (reply, expected) in [
            (
                CoreBluetoothReply::Err("callback executor stopped".into()),
                Err("callback executor stopped"),
            ),
            (
                CoreBluetoothReply::Ok,
                Err("Unexpected CoreBluetooth adapter state reply"),
            ),
            (
                CoreBluetoothReply::AdapterState(CBManagerState::PoweredOn),
                Ok(CentralState::PoweredOn),
            ),
        ] {
            let (sender, mut receiver) = mpsc::channel(1);
            let adapter = Adapter {
                manager: Arc::new(AdapterManager::default()),
                sender,
                owner: Arc::new(Owner {
                    tasks: Arc::new(Tasks::default()),
                    thread: Mutex::new(None),
                    closing: tokio::sync::Mutex::new(()),
                }),
            };
            let (result, ()) = tokio::join!(adapter.adapter_state(), async {
                match receiver.next().await.expect("adapter state command") {
                    CoreBluetoothMessage::GetAdapterState { future } => {
                        crate::corebluetooth::future::set_reply(&future, reply);
                    }
                    _ => panic!("unexpected command"),
                }
            });
            match (result, expected) {
                (Ok(actual), Ok(expected)) => assert_eq!(actual, expected),
                (Err(Error::RuntimeError(actual)), Err(expected)) => assert_eq!(actual, expected),
                (actual, expected) => {
                    panic!("unexpected state result {actual:?}, expected {expected:?}")
                }
            }
            adapter.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn cache_eviction_retrieves_same_uuid_without_scan_and_preserves_failure() {
        let uuid = uuid::Uuid::from_u128(0x12345678_1234_5678_1234_567812345678);
        let id: PeripheralId = uuid.into();
        for outcome in 0..3 {
            let (sender, mut commands) = mpsc::channel(1);
            let adapter = Adapter {
                manager: Arc::new(AdapterManager::default()),
                sender,
                owner: Arc::new(Owner {
                    tasks: Arc::new(Tasks::default()),
                    thread: Mutex::new(None),
                    closing: tokio::sync::Mutex::new(()),
                }),
            };
            let (_events, receiver) = mpsc::channel(1);
            let peripheral = Peripheral::new(
                uuid,
                None,
                None,
                Arc::downgrade(&adapter.manager),
                receiver,
                adapter.sender.clone(),
                &adapter.owner.tasks,
            );
            adapter.manager.add_peripheral(peripheral.clone());
            assert_eq!(adapter.peripheral(&id).await.unwrap().id(), id);
            adapter
                .manager
                .emit(CentralEvent::DeviceDisconnected(id.clone()));
            assert!(adapter.manager.peripheral(&id).is_none());
            let (result, ()) = tokio::join!(adapter.peripheral(&id), async {
                let CoreBluetoothMessage::RetrievePeripherals { options, future } =
                    commands.next().await.unwrap()
                else {
                    panic!("reconnect must retrieve, never scan");
                };
                assert_eq!(options.identifiers, Some(vec![id.clone()]));
                assert_eq!(options.services, None);
                crate::corebluetooth::future::set_reply(
                    &future,
                    match outcome {
                        0 => CoreBluetoothReply::Peripherals(vec![peripheral]),
                        1 => CoreBluetoothReply::Peripherals(vec![]),
                        _ => CoreBluetoothReply::Err("Controlled retrieval failure".into()),
                    },
                );
            });
            match outcome {
                0 => assert_eq!(result.unwrap().id(), id),
                1 => assert!(matches!(result, Err(Error::DeviceNotFound))),
                _ => assert!(
                    matches!(result, Err(Error::RuntimeError(message)) if message == "Controlled retrieval failure")
                ),
            }
            adapter.shutdown().await.unwrap();
        }
    }
    struct Live(Arc<AtomicUsize>);
    impl Drop for Live {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    fn fake_adapter(live: Arc<AtomicUsize>) -> Adapter {
        let (sender, receiver) = mpsc::channel(1);
        live.fetch_add(1, Ordering::SeqCst);
        let resource = Live(live);
        let thread = CoreBluetoothThread::spawn(sender.clone(), move |stop| {
            let _resource = resource;
            let _receiver = receiver;
            futures::executor::block_on(stop).ok();
        })
        .unwrap();
        Adapter {
            manager: Arc::new(AdapterManager::default()),
            sender,
            owner: Arc::new(Owner {
                tasks: Arc::new(Tasks::default()),
                thread: Mutex::new(Some(thread)),
                closing: tokio::sync::Mutex::new(()),
            }),
        }
    }
    #[tokio::test]
    async fn hundred_adapter_shutdowns_join_thread_and_peripheral_event_tasks() {
        let live = Arc::new(AtomicUsize::new(0));
        for _ in 0..100 {
            let adapter = fake_adapter(live.clone());
            let clone = adapter.clone();
            let (events, receiver) = mpsc::channel(1);
            let peripheral = Peripheral::new(
                uuid::Uuid::from_u128(1),
                None,
                None,
                Arc::downgrade(&adapter.manager),
                receiver,
                adapter.sender.clone(),
                &adapter.owner.tasks,
            );
            adapter.manager.add_peripheral(peripheral);
            adapter.shutdown().await.unwrap();
            assert!(events.is_closed());
            assert!(adapter.manager.peripherals().is_empty());
            assert_eq!(live.load(Ordering::SeqCst), 0);
            clone.shutdown().await.unwrap();
        }
    }
    #[test]
    fn dropping_bootstrap_owner_joins_thread_without_a_returned_adapter() {
        let live = Arc::new(AtomicUsize::new(0));
        let adapter = fake_adapter(live.clone());
        drop(adapter);
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    #[ignore = "requires a real CoreBluetooth manager and Bluetooth authorization"]
    async fn real_adapter_shutdown_joins_thread_and_releases_owners() {
        for _ in 0..100 {
            let adapter = tokio::time::timeout(std::time::Duration::from_secs(10), Adapter::new())
                .await
                .expect("CoreBluetooth bootstrap deadline")
                .expect("Bluetooth authorization required");
            let owner = Arc::downgrade(&adapter.owner);
            let manager = Arc::downgrade(&adapter.manager);
            adapter.shutdown().await.unwrap();
            assert!(adapter.owner.thread.lock().unwrap().is_none());
            assert!(adapter.manager.peripherals().is_empty());
            adapter.shutdown().await.unwrap();
            drop(adapter);
            assert!(owner.upgrade().is_none());
            assert!(manager.upgrade().is_none());
        }
    }
}
