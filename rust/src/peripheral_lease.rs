//! Android shares cached btleplug peripherals across engines. One physical
//! peripheral lease survives until OS disconnect has acknowledged cleanup.
use crate::codec::Error;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

const CAPACITY: usize = 1024;
#[derive(Default)]
pub(crate) struct Registry(Mutex<HashMap<String, Weak<()>>>);
pub(crate) struct Lease {
    registry: Arc<Registry>,
    id: String,
    token: Arc<()>,
    pub(crate) recover: bool,
    clean: bool,
}
impl Registry {
    pub(crate) fn acquire(self: &Arc<Self>, id: String) -> Result<Lease, Error> {
        let mut entries = self
            .0
            .lock()
            .map_err(|_| Error::new(18, "Peripheral lease registry poisoned"))?;
        if entries
            .get(&id)
            .is_some_and(|entry| entry.strong_count() != 0)
        {
            return Err(Error::new(6, "Peripheral is owned by another engine"));
        }
        let recover = entries.contains_key(&id);
        if !recover && entries.len() >= CAPACITY {
            return Err(Error::new(16, "Peripheral lease capacity exceeded"));
        }
        let token = Arc::new(());
        entries.insert(id.clone(), Arc::downgrade(&token));
        Ok(Lease {
            registry: self.clone(),
            id,
            token,
            recover,
            clean: false,
        })
    }
}
impl Lease {
    pub(crate) fn mark_clean(&mut self) -> Result<(), Error> {
        let entries = self
            .registry
            .0
            .lock()
            .map_err(|_| Error::new(18, "Peripheral lease registry poisoned"))?;
        if !entries
            .get(&self.id)
            .is_some_and(|entry| entry.ptr_eq(&Arc::downgrade(&self.token)))
        {
            return Err(Error::new(18, "Peripheral lease ownership changed"));
        }
        self.clean = true;
        Ok(())
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        if self.clean
            && let Ok(mut entries) = self.registry.0.lock()
            && entries
                .get(&self.id)
                .is_some_and(|entry| entry.ptr_eq(&Arc::downgrade(&self.token)))
        {
            entries.remove(&self.id);
        }
    }
}
// Dropping without mark_clean leaves a weak tombstone. It does not retain
// the engine or peripheral, but the next owner must disconnect before connect.
#[cfg(target_os = "android")]
pub(crate) fn process_registry() -> &'static Arc<Registry> {
    static REGISTRY: std::sync::OnceLock<Arc<Registry>> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| Arc::new(Registry::default()))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ownership_conflicts_and_clean_release_are_idempotent() {
        let registry = Arc::new(Registry::default());
        let mut first = registry.acquire("device".into()).unwrap();
        assert!(!first.recover);
        assert_eq!(registry.acquire("device".into()).err().unwrap().code, 6);
        let mut other = registry.acquire("other".into()).unwrap();
        other.mark_clean().unwrap();
        drop(other);
        first.mark_clean().unwrap();
        first.mark_clean().unwrap();
        // Even acknowledged cleanup retains the lease until the driver drops
        // its notification stream and returns from the generation worker.
        assert_eq!(registry.acquire("device".into()).err().unwrap().code, 6);
        drop(first);
        let mut next = registry.acquire("device".into()).unwrap();
        assert!(!next.recover);
        next.mark_clean().unwrap();
        drop(next);
        assert!(registry.0.lock().unwrap().is_empty());
    }
    #[test]
    fn failed_or_cancelled_cleanup_requires_recovery_without_retaining_owner() {
        let registry = Arc::new(Registry::default());
        let first = registry.acquire("device".into()).unwrap();
        let token = Arc::downgrade(&first.token);
        drop(first);
        assert!(token.upgrade().is_none());
        let mut next = registry.acquire("device".into()).unwrap();
        assert!(next.recover);
        assert_eq!(registry.acquire("device".into()).err().unwrap().code, 6);
        next.mark_clean().unwrap();
        drop(next);
        assert!(registry.0.lock().unwrap().is_empty());
    }
    #[test]
    fn hundred_recovery_cycles_leave_no_retained_leases() {
        let registry = Arc::new(Registry::default());
        for _ in 0..100 {
            drop(registry.acquire("device".into()).unwrap());
            let mut next = registry.acquire("device".into()).unwrap();
            assert!(next.recover);
            next.mark_clean().unwrap();
            drop(next);
            assert!(registry.0.lock().unwrap().is_empty());
        }
    }
    #[test]
    fn uncertain_devices_are_bounded_and_existing_device_can_still_recover() {
        let registry = Arc::new(Registry::default());
        for id in 0..CAPACITY {
            drop(registry.acquire(id.to_string()).unwrap());
        }
        assert_eq!(registry.acquire("overflow".into()).err().unwrap().code, 16);
        let mut recovery = registry.acquire("0".into()).unwrap();
        assert!(recovery.recover);
        recovery.mark_clean().unwrap();
        drop(recovery);
        let mut fresh = registry.acquire("overflow".into()).unwrap();
        assert!(!fresh.recover);
        fresh.mark_clean().unwrap();
    }
}
