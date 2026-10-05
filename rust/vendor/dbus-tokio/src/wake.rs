//! Reactor waker hooks run outside the state mutex.
use std::{sync::Mutex, task};

#[derive(Debug)]
pub(crate) enum WakeStatus {
    /// The resource task has not yet been polled; ready is false if it has never been polled
    /// before, otherwise, it's being woken as a result of the Channel calling the waker callback
    Waiting { ready: bool },
    /// The resource task is ready to be woken again
    Polled { waker: task::Waker },
}

pub(crate) fn register_waker(wake: &Mutex<WakeStatus>, waker: &task::Waker) -> bool {
    // RawWaker clone/drop hooks are arbitrary caller code. Keep both outside
    // this mutex, including disposal of the replaced registration.
    let candidate = waker.clone();
    let mut status = wake.lock().unwrap_or_else(|error| error.into_inner());
    if matches!(&*status, WakeStatus::Polled { waker } if candidate.will_wake(waker)) {
        drop(status);
        drop(candidate);
        return false;
    }
    let previous = std::mem::replace(&mut *status, WakeStatus::Polled { waker: candidate });
    let ready = matches!(previous, WakeStatus::Waiting { ready: true });
    drop(status);
    drop(previous);
    ready
}

pub(crate) fn notify_sender(wake: &Mutex<WakeStatus>) -> Result<(), ()> {
    let previous = {
        let mut status = wake.lock().unwrap_or_else(|error| error.into_inner());
        std::mem::replace(&mut *status, WakeStatus::Waiting { ready: true })
    };
    match previous {
        WakeStatus::Polled { waker } => {
            waker.wake();
            Ok(())
        }
        WakeStatus::Waiting { .. } => Err(()),
    }
}

#[cfg(test)]
mod openble_waker_ownership_tests {
    use super::*;
    use std::sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    struct Probe {
        state: Weak<Mutex<WakeStatus>>,
        hooks: Arc<AtomicUsize>,
        reenter: bool,
        panic_wake: AtomicBool,
    }

    impl Probe {
        fn hook(&self) {
            if let Some(state) = self.state.upgrade() {
                assert!(
                    match state.try_lock() {
                        Ok(_) | Err(std::sync::TryLockError::Poisoned(_)) => true,
                        Err(std::sync::TryLockError::WouldBlock) => false,
                    },
                    "waker hook ran under the reactor mutex"
                );
            }
            self.hooks.fetch_add(1, Ordering::SeqCst);
        }
    }

    unsafe fn clone(data: *const ()) -> task::RawWaker {
        // A borrowed raw Arc is protected from decrement; the new waker owns
        // exactly one additional strong reference.
        let probe = std::mem::ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<Probe>()) });
        probe.hook();
        raw(Arc::clone(&probe))
    }

    unsafe fn wake(data: *const ()) {
        // Consuming wake retires the raw waker's single Arc reference.
        let probe = unsafe { Arc::from_raw(data.cast::<Probe>()) };
        probe.hook();
        let state = probe.state.upgrade().unwrap();
        if probe.reenter {
            assert!(notify_sender(&state).is_err());
        }
        assert!(
            !probe.panic_wake.load(Ordering::SeqCst),
            "controlled wake panic"
        );
    }

    unsafe fn wake_by_ref(data: *const ()) {
        let probe = std::mem::ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<Probe>()) });
        probe.hook();
    }

    unsafe fn drop_waker(data: *const ()) {
        let probe = unsafe { Arc::from_raw(data.cast::<Probe>()) };
        probe.hook();
    }

    static VTABLE: task::RawWakerVTable =
        task::RawWakerVTable::new(clone, wake, wake_by_ref, drop_waker);

    fn raw(probe: Arc<Probe>) -> task::RawWaker {
        task::RawWaker::new(Arc::into_raw(probe).cast(), &VTABLE)
    }

    fn waker(
        state: &Arc<Mutex<WakeStatus>>,
        reenter: bool,
        panic: bool,
    ) -> (task::Waker, Arc<AtomicUsize>) {
        let hooks = Arc::new(AtomicUsize::new(0));
        let probe = Arc::new(Probe {
            state: Arc::downgrade(state),
            hooks: hooks.clone(),
            reenter,
            panic_wake: AtomicBool::new(panic),
        });
        // Each vtable operation accounts for the reference given to raw().
        (unsafe { task::Waker::from_raw(raw(probe)) }, hooks)
    }

    #[test]
    fn clone_replacement_same_waker_and_disposal_run_outside_mutex() {
        let state = Arc::new(Mutex::new(WakeStatus::Waiting { ready: false }));
        let (first, first_hooks) = waker(&state, false, false);
        let (second, second_hooks) = waker(&state, false, false);
        for cycle in 0..100 {
            assert_eq!(register_waker(&state, &first), cycle != 0);
            assert!(!register_waker(&state, &first));
            assert!(!register_waker(&state, &second));
            assert!(notify_sender(&state).is_ok());
            assert!(register_waker(&state, &second));
            assert!(notify_sender(&state).is_ok());
        }
        drop(first);
        drop(second);
        assert!(first_hooks.load(Ordering::SeqCst) >= 300);
        assert!(second_hooks.load(Ordering::SeqCst) >= 300);
        assert!(state.try_lock().is_ok());
    }

    #[test]
    fn reentrant_sender_preserves_sticky_ready_without_locking_itself() {
        let state = Arc::new(Mutex::new(WakeStatus::Waiting { ready: false }));
        let (waker, hooks) = waker(&state, true, false);
        for _ in 0..100 {
            register_waker(&state, &waker);
            assert!(notify_sender(&state).is_ok());
            assert!(matches!(
                *state.lock().unwrap(),
                WakeStatus::Waiting { ready: true }
            ));
        }
        drop(waker);
        assert!(hooks.load(Ordering::SeqCst) >= 200);
    }

    #[test]
    fn wake_panic_does_not_poison_or_erase_the_committed_notification() {
        let state = Arc::new(Mutex::new(WakeStatus::Waiting { ready: false }));
        let (failing, _) = waker(&state, false, true);
        register_waker(&state, &failing);
        assert!(std::panic::catch_unwind(|| notify_sender(&state)).is_err());
        assert!(matches!(
            *state.lock().unwrap(),
            WakeStatus::Waiting { ready: true }
        ));
        let (retry, _) = waker(&state, false, false);
        assert!(register_waker(&state, &retry));
        assert!(notify_sender(&state).is_ok());
        drop(failing);
        drop(retry);
    }
}
