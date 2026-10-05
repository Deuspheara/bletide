/// Implements common functionality for adapters across platforms.
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
use crate::api::{CentralEvent, Peripheral};
use crate::platform::PeripheralId;
use dashmap::{DashMap, mapref::one::RefMut};
use futures::stream::{Stream, StreamExt};
use log::trace;
use std::pin::Pin;
#[cfg(any(target_os = "android", test))]
use std::sync::Weak;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

// Android's adapter is process-wide. OpenBLE opts into engine-scoped retention;
// ordinary btleplug callers keep the original discovery-cache behavior.
#[cfg(any(target_os = "android", test))]
#[derive(Debug, Default)]
struct DiscoveryOwners {
    scoped: bool,
    owners: Vec<Weak<()>>,
}
use tokio_stream::wrappers::BroadcastStream;

#[derive(Debug)]
pub struct AdapterManager<PeripheralType>
where
    PeripheralType: Peripheral,
{
    peripherals: DashMap<PeripheralId, PeripheralType>,
    #[cfg(any(target_os = "android", test))]
    discovery_owners: Mutex<DiscoveryOwners>,
    events_channel: broadcast::Sender<CentralEvent>,
    terminal_error: Arc<Mutex<Option<String>>>,
}

impl<PeripheralType: Peripheral + 'static> Default for AdapterManager<PeripheralType> {
    fn default() -> Self {
        let (broadcast_sender, _) = broadcast::channel(16);
        AdapterManager {
            peripherals: DashMap::new(),
            #[cfg(any(target_os = "android", test))]
            discovery_owners: Mutex::new(DiscoveryOwners::default()),
            events_channel: broadcast_sender,
            terminal_error: Arc::new(Mutex::new(None)),
        }
    }
}

impl<PeripheralType> AdapterManager<PeripheralType>
where
    PeripheralType: Peripheral + 'static,
{
    pub fn emit(&self, event: CentralEvent) {
        {
            let mut terminal = self
                .terminal_error
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if terminal.is_some() {
                return;
            }
            if let CentralEvent::AdapterError { message } = &event {
                *terminal = Some(message.clone());
            }
        }
        // No terminal-state lock is held while dropping a peripheral or waking
        // receivers. Each receiver also checks the cached terminal cause, so a
        // racing ordinary publication cannot hide it.

        if let CentralEvent::DeviceDisconnected(ref id) = event {
            self.peripherals.remove(id);
        }

        if let Err(lost) = self.events_channel.send(event) {
            trace!("Lost central event, while nothing subscribed: {:?}", lost);
        }
    }

    pub fn event_stream(&self) -> Pin<Box<dyn Stream<Item = CentralEvent> + Send>> {
        // Subscribe before inspecting the cache: failure during this sequence
        // is either cached or queued, never lost between the two steps.
        let receiver = self.events_channel.subscribe();
        let terminal = self.terminal_error.clone();
        let cached = terminal
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        if let Some(message) = cached {
            return Box::pin(futures::stream::once(std::future::ready(
                CentralEvent::AdapterError { message },
            )));
        }
        Box::pin(
            BroadcastStream::new(receiver).scan(false, move |ended, value| {
                if *ended {
                    return std::future::ready(None);
                }
                let cached = terminal
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .clone();
                let event = match cached {
                    Some(message) => CentralEvent::AdapterError { message },
                    None => match value {
                        Ok(event) => event,
                        Err(error) => CentralEvent::AdapterError {
                            message: format!("Central event stream lost events: {error}"),
                        },
                    },
                };
                *ended = matches!(event, CentralEvent::AdapterError { .. });
                std::future::ready(Some(event))
            }),
        )
    }

    #[cfg(any(target_os = "android", test))]
    pub fn retain_discovery_owner(&self, owner: &Arc<()>) {
        let mut state = self
            .discovery_owners
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        state.scoped = true;
        state.owners.retain(|entry| entry.strong_count() != 0);
        if !state
            .owners
            .iter()
            .any(|entry| entry.ptr_eq(&Arc::downgrade(owner)))
        {
            state.owners.push(Arc::downgrade(owner));
        }
    }

    #[cfg(any(target_os = "android", test))]
    pub fn release_discovery_owner(&self, owner: &Arc<()>) {
        let mut state = self
            .discovery_owners
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        state
            .owners
            .retain(|entry| entry.strong_count() != 0 && !entry.ptr_eq(&Arc::downgrade(owner)));
        let mut retired = Vec::new();
        if state.scoped && state.owners.is_empty() {
            // Serialize detach with insertion/new engine admission. Destruction
            // happens after releasing both the registry and DashMap locks.
            let ids: Vec<_> = self
                .peripherals
                .iter()
                .map(|entry| entry.key().clone())
                .collect();
            for id in ids {
                if let Some((_, peripheral)) = self.peripherals.remove(&id) {
                    retired.push(peripheral);
                }
            }
        }
        drop(state);
        drop(retired);
    }

    /// Inserts a peripheral if absent and returns the retained instance,
    /// preserving any existing connection state and characteristics.
    pub fn add_peripheral(&self, peripheral: PeripheralType) -> PeripheralType {
        #[cfg(any(target_os = "android", test))]
        {
            let mut state = self
                .discovery_owners
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            state.owners.retain(|entry| entry.strong_count() != 0);
            if state.scoped && state.owners.is_empty() {
                // Late scan callbacks must not repopulate an unowned cache.
                return peripheral;
            }
            let id = peripheral.id();
            if let Some(existing) = self.peripheral(&id) {
                drop(state);
                return existing;
            }
            let replaced = self.peripherals.insert(id, peripheral.clone());
            drop(state);
            drop(replaced);
            peripheral
        }
        #[cfg(not(any(target_os = "android", test)))]
        {
            let id = peripheral.id();
            self.peripherals
                .entry(id)
                .or_insert(peripheral)
                .value()
                .clone()
        }
    }

    pub fn clear_peripherals(&self) {
        self.peripherals.clear();
    }

    pub fn peripherals(&self) -> Vec<PeripheralType> {
        self.peripherals
            .iter()
            .map(|val| val.value().clone())
            .collect()
    }

    // Only used on windows and macOS/iOS, so turn off deadcode so we don't get warnings on android/linux.
    #[allow(dead_code)]
    pub fn peripheral_mut(
        &self,
        id: &PeripheralId,
    ) -> Option<RefMut<'_, PeripheralId, PeripheralType>> {
        self.peripherals.get_mut(id)
    }

    pub fn peripheral(&self, id: &PeripheralId) -> Option<PeripheralType> {
        self.peripherals.get(id).map(|val| val.value().clone())
    }
}

#[cfg(all(test, any(target_vendor = "apple", target_os = "windows")))]
mod tests {
    use super::*;
    use crate::Result;
    use crate::api::{
        BDAddr, Characteristic, Descriptor, PeripheralProperties, Service, ValueNotification,
        WriteType,
    };
    use async_trait::async_trait;
    use std::collections::BTreeSet;
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    };

    #[derive(Clone, Debug)]
    struct TestPeripheral {
        id: PeripheralId,
        state: Arc<AtomicUsize>,
        drop_probe: Option<Arc<DropProbe>>,
    }

    #[derive(Debug)]
    struct DropProbe {
        manager: Weak<AdapterManager<TestPeripheral>>,
        drops: Arc<AtomicUsize>,
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            let manager = self.manager.upgrade().unwrap();
            // A peripheral destructor may reenter adapter ownership.
            assert!(manager.discovery_owners.try_lock().is_ok());
            assert!(manager.peripherals().is_empty());
            let next = Arc::new(());
            manager.retain_discovery_owner(&next);
            manager.release_discovery_owner(&next);
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl TestPeripheral {
        fn new() -> Self {
            #[cfg(target_vendor = "apple")]
            let id = uuid::Uuid::nil().into();
            #[cfg(target_os = "windows")]
            let id = BDAddr::default().into();
            Self {
                id,
                state: Arc::new(AtomicUsize::new(0)),
                drop_probe: None,
            }
        }
    }

    #[async_trait]
    impl Peripheral for TestPeripheral {
        fn id(&self) -> PeripheralId {
            self.id.clone()
        }

        fn address(&self) -> BDAddr {
            unreachable!()
        }

        fn mtu(&self) -> u16 {
            unreachable!()
        }

        fn services(&self) -> BTreeSet<Service> {
            unreachable!()
        }

        async fn properties(&self) -> Result<Option<PeripheralProperties>> {
            unreachable!()
        }

        async fn is_connected(&self) -> Result<bool> {
            unreachable!()
        }

        async fn connect(&self) -> Result<()> {
            unreachable!()
        }

        async fn disconnect(&self) -> Result<()> {
            unreachable!()
        }

        async fn discover_services(&self) -> Result<()> {
            unreachable!()
        }

        async fn write(&self, _: &Characteristic, _: &[u8], _: WriteType) -> Result<()> {
            unreachable!()
        }

        async fn read(&self, _: &Characteristic) -> Result<Vec<u8>> {
            unreachable!()
        }

        async fn subscribe(&self, _: &Characteristic) -> Result<()> {
            unreachable!()
        }

        async fn unsubscribe(&self, _: &Characteristic) -> Result<()> {
            unreachable!()
        }

        async fn notifications(
            &self,
        ) -> Result<Pin<Box<dyn Stream<Item = ValueNotification> + Send>>> {
            unreachable!()
        }

        async fn write_descriptor(&self, _: &Descriptor, _: &[u8]) -> Result<()> {
            unreachable!()
        }

        async fn read_descriptor(&self, _: &Descriptor) -> Result<Vec<u8>> {
            unreachable!()
        }
    }

    #[test]
    fn discovery_owner_cleanup_drops_peripherals_outside_registry_locks() {
        let manager = Arc::new(AdapterManager::default());
        let owner = Arc::new(());
        manager.retain_discovery_owner(&owner);
        let drops = Arc::new(AtomicUsize::new(0));
        let mut peripheral = TestPeripheral::new();
        peripheral.drop_probe = Some(Arc::new(DropProbe {
            manager: Arc::downgrade(&manager),
            drops: drops.clone(),
        }));
        drop(manager.add_peripheral(peripheral));
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        manager.release_discovery_owner(&owner);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn discovery_owners_release_last_cache_and_reject_late_insertions() {
        let manager = AdapterManager::default();
        for _ in 0..100 {
            let first = Arc::new(());
            let second = Arc::new(());
            manager.retain_discovery_owner(&first);
            manager.retain_discovery_owner(&first); // Idempotent admission.
            manager.retain_discovery_owner(&second);
            let candidate = TestPeripheral::new();
            let state = Arc::downgrade(&candidate.state);
            drop(manager.add_peripheral(candidate));
            manager.release_discovery_owner(&first);
            assert!(state.upgrade().is_some());
            assert_eq!(manager.peripherals().len(), 1);
            manager.release_discovery_owner(&second);
            assert!(state.upgrade().is_none());
            assert!(manager.peripherals().is_empty());
            let late = TestPeripheral::new();
            let late_state = Arc::downgrade(&late.state);
            drop(manager.add_peripheral(late));
            assert!(late_state.upgrade().is_none());
            assert!(manager.peripherals().is_empty());
            let next = Arc::new(());
            manager.retain_discovery_owner(&next);
            drop(manager.add_peripheral(TestPeripheral::new()));
            manager.release_discovery_owner(&second); // Old cleanup cannot clear next engine.
            assert_eq!(manager.peripherals().len(), 1);
            manager.release_discovery_owner(&next);
            assert!(manager.peripherals().is_empty());
        }
    }

    #[test]
    fn discovery_owner_admission_racing_old_cleanup_preserves_new_engine() {
        for _ in 0..100 {
            let manager = Arc::new(AdapterManager::default());
            let old = Arc::new(());
            let next = Arc::new(());
            manager.retain_discovery_owner(&old);
            let barrier = Barrier::new(2);
            std::thread::scope(|scope| {
                scope.spawn(|| {
                    barrier.wait();
                    manager.release_discovery_owner(&old);
                });
                scope.spawn(|| {
                    barrier.wait();
                    manager.retain_discovery_owner(&next);
                    drop(manager.add_peripheral(TestPeripheral::new()));
                });
            });
            assert_eq!(manager.peripherals().len(), 1);
            manager.release_discovery_owner(&next);
            assert!(manager.peripherals().is_empty());
        }
    }

    #[test]
    fn duplicate_insertion_returns_existing_shared_state() {
        let manager = AdapterManager::default();
        let first = TestPeripheral::new();
        first.state.store(41, Ordering::SeqCst);
        let inserted = manager.add_peripheral(first.clone());
        assert!(Arc::ptr_eq(&first.state, &inserted.state));

        let duplicate = TestPeripheral::new();
        assert!(!Arc::ptr_eq(&first.state, &duplicate.state));
        let returned = manager.add_peripheral(duplicate.clone());
        assert!(Arc::ptr_eq(&first.state, &returned.state));
        assert_eq!(returned.state.fetch_add(1, Ordering::SeqCst), 41);
        assert_eq!(duplicate.state.load(Ordering::SeqCst), 0);

        let stored = manager.peripheral(&first.id()).unwrap();
        assert!(Arc::ptr_eq(&returned.state, &stored.state));
        assert_eq!(stored.state.load(Ordering::SeqCst), 42);
        assert_eq!(manager.peripherals().len(), 1);
    }

    #[test]
    fn concurrent_insertions_return_the_same_shared_state() {
        const WORKERS: usize = 16;
        let manager = AdapterManager::default();
        let barrier = Barrier::new(WORKERS);
        let returned = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..WORKERS)
                .map(|_| {
                    let manager = &manager;
                    let barrier = &barrier;
                    scope.spawn(move || {
                        let candidate = TestPeripheral::new();
                        barrier.wait();
                        let canonical = manager.add_peripheral(candidate);
                        canonical.state.fetch_add(1, Ordering::SeqCst);
                        canonical
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });

        let stored = manager.peripheral(&returned[0].id()).unwrap();
        assert_eq!(manager.peripherals().len(), 1);
        for peripheral in returned {
            assert!(Arc::ptr_eq(&stored.state, &peripheral.state));
            assert_eq!(peripheral.state.load(Ordering::SeqCst), WORKERS);
        }
    }
}

#[cfg(test)]
mod terminal_event_tests {
    use super::*;
    use crate::api::CentralState;

    #[tokio::test]
    async fn scan_failure_is_scoped_and_does_not_poison_adapter_stream() {
        let manager = AdapterManager::<crate::platform::Peripheral>::default();
        let mut early = manager.event_stream();
        manager.emit(CentralEvent::ScanError {
            generation: 41,
            error_code: 3,
        });
        manager.emit(CentralEvent::StateUpdate(CentralState::PoweredOn));
        assert!(matches!(
            early.next().await,
            Some(CentralEvent::ScanError {
                generation: 41,
                error_code: 3
            })
        ));
        assert!(matches!(
            early.next().await,
            Some(CentralEvent::StateUpdate(CentralState::PoweredOn))
        ));
        let mut late = manager.event_stream();
        manager.emit(CentralEvent::ScanError {
            generation: 42,
            error_code: 6,
        });
        assert!(matches!(
            late.next().await,
            Some(CentralEvent::ScanError {
                generation: 42,
                error_code: 6
            })
        ));
        assert!(manager.terminal_error.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn cached_fatal_cause_survives_backlog_and_is_replayed_to_late_subscribers() {
        let manager = AdapterManager::<crate::platform::Peripheral>::default();
        let mut early = manager.event_stream();
        for _ in 0..64 {
            manager.emit(CentralEvent::StateUpdate(CentralState::PoweredOn));
        }
        manager.emit(CentralEvent::AdapterError {
            message: "callback failed".into(),
        });
        for _ in 0..64 {
            manager.emit(CentralEvent::StateUpdate(CentralState::PoweredOff));
        }
        let mut late = manager.event_stream();
        for stream in [&mut early, &mut late] {
            assert!(
                matches!(stream.next().await, Some(CentralEvent::AdapterError { message }) if message == "callback failed")
            );
            assert!(stream.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn receiver_lag_is_an_explicit_terminal_error_without_poisoning_new_receivers() {
        let manager = AdapterManager::<crate::platform::Peripheral>::default();
        let mut slow = manager.event_stream();
        for _ in 0..64 {
            manager.emit(CentralEvent::StateUpdate(CentralState::PoweredOn));
        }
        assert!(
            matches!(slow.next().await, Some(CentralEvent::AdapterError { message }) if message.contains("lost events"))
        );
        assert!(slow.next().await.is_none());
        let mut next = manager.event_stream();
        manager.emit(CentralEvent::StateUpdate(CentralState::PoweredOff));
        assert!(matches!(
            next.next().await,
            Some(CentralEvent::StateUpdate(CentralState::PoweredOff))
        ));
    }
}
