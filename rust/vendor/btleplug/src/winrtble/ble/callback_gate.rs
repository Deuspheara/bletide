//! Retiring an owner waits for publication in progress and rejects late callbacks.
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub(crate) struct CallbackGate(Arc<Mutex<bool>>);

impl CallbackGate {
    pub(crate) fn new() -> Self {
        Self(Arc::new(Mutex::new(true)))
    }

    pub(crate) fn publish(&self, callback: impl FnOnce()) {
        // Keep the lock through publication. An atomic flag alone leaves a race
        // between checking the flag and publishing into a reused peripheral.
        let active = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if *active {
            callback();
        }
    }

    pub(crate) fn retire(&self) {
        *self.0.lock().unwrap_or_else(|error| error.into_inner()) = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    };

    #[test]
    fn retirement_rejects_old_callbacks_without_affecting_a_new_owner() {
        let old = CallbackGate::new();
        let callback = old.clone();
        let publications = AtomicUsize::new(0);
        callback.publish(|| {
            publications.fetch_add(1, Ordering::Relaxed);
        });
        old.retire();
        old.retire();
        callback.publish(|| panic!("retired callback ran"));
        let next = CallbackGate::new();
        next.publish(|| {
            publications.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(publications.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn device_retirement_rejects_a_still_owned_notification_handler() {
        let device = CallbackGate::new();
        let notification = CallbackGate::new();
        let publications = AtomicUsize::new(0);
        let publish = || {
            device.publish(|| {
                notification.publish(|| {
                    publications.fetch_add(1, Ordering::Relaxed);
                })
            })
        };
        publish();
        device.retire();
        // The handler is intentionally still active/owned: OS removal or a
        // retained characteristic need not succeed for retirement to stop it.
        notification.publish(|| {});
        publish();
        assert_eq!(publications.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn retirement_joins_publication_already_in_progress() {
        let gate = CallbackGate::new();
        let callback = gate.clone();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (retired_tx, retired_rx) = mpsc::channel();
        let publishing = std::thread::spawn(move || {
            callback.publish(|| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            })
        });
        entered_rx.recv().unwrap();
        // Prove the publisher owns the barrier before starting retirement.
        assert!(matches!(
            gate.0.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        let retiring = gate.clone();
        let retirement = std::thread::spawn(move || {
            retiring.retire();
            retired_tx.send(()).unwrap();
        });
        assert!(matches!(
            retired_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        release_tx.send(()).unwrap();
        publishing.join().unwrap();
        retired_rx.recv().unwrap();
        retirement.join().unwrap();
        gate.publish(|| panic!("publication after retirement"));
    }
}
