use core::pin::Pin;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// Struct used for waiting on replies from the server.
///
/// When a BtlePlugMessage is sent to the server, it may take an indeterminate
/// amount of time to get a reply. This struct holds the reply, as well as a
/// [Waker] for the related future. Once the reply_msg is filled, the waker will
/// be called to finish the future polling.
#[derive(Debug, Clone)]
pub struct BtlePlugFutureState<T> {
    reply_msg: Option<T>,
    waker: Option<Waker>,
    completed: bool,
}

// For some reason, deriving default above doesn't work, but doing an explicit
// derive here does work.
impl<T> Default for BtlePlugFutureState<T> {
    fn default() -> Self {
        BtlePlugFutureState::<T> {
            reply_msg: None,
            waker: None,
            completed: false,
        }
    }
}

/// Complete a reply exactly once. Custom wake and reply destruction happen
/// after unlocking, so they can reenter state without deadlocking or poisoning it.
pub fn set_reply<T>(shared: &BtlePlugFutureStateShared<T>, reply: T) {
    let (discarded, waker) = {
        let mut state = shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.completed {
            (Some(reply), None)
        } else {
            state.completed = true;
            state.reply_msg = Some(reply);
            (None, state.waker.take())
        }
    };
    drop(discarded);
    if let Some(waker) = waker {
        waker.wake();
    }
}

/// Shared [BtlePlugFutureState] type.
///
/// [BtlePlugFutureState] is made to be shared across futures, and we'll
/// never know if those futures are single or multithreaded. Reply publication uses [set_reply], which unlocks before waking the receiver.
pub type BtlePlugFutureStateShared<T> = Arc<Mutex<BtlePlugFutureState<T>>>;

/// [Future] implementation for [BtlePlugMessageUnion] types send to the server.
///
/// A [Future] implementation that we can always expect to return a
/// [BtlePlugMessageUnion]. Used to deal with getting server replies after
/// sending [BtlePlugMessageUnion] types via the client API.
#[derive(Debug)]
pub struct BtlePlugFuture<T> {
    /// State that holds the waker for the future, and the [BtlePlugMessageUnion] reply (once set).
    ///
    /// ## Notes
    ///
    /// This needs to be an [Arc]<[Mutex]<T>> in order to make it mutable under
    /// pinning when dealing with being a future. There is a chance we could do
    /// this as a [Pin::get_unchecked_mut] borrow, which would be way faster, but
    /// that's dicey and hasn't been proven as needed for speed yet.
    waker_state: BtlePlugFutureStateShared<T>,
}

impl<T> Default for BtlePlugFuture<T> {
    fn default() -> Self {
        BtlePlugFuture::<T> {
            waker_state: BtlePlugFutureStateShared::<T>::default(),
        }
    }
}

impl<T> BtlePlugFuture<T> {
    /// Returns a clone of the state, used for moving the state across contexts
    /// (tasks/threads/etc...).
    pub fn get_state_clone(&self) -> BtlePlugFutureStateShared<T> {
        self.waker_state.clone()
    }
}

impl<T> Drop for BtlePlugFuture<T> {
    fn drop(&mut self) {
        let retired = {
            let mut state = self
                .waker_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // Keep queued state as a callback-correlation tombstone, but stop
            // retaining the canceled task or accepting values for its receiver.
            state.completed = true;
            let retired = (state.reply_msg.take(), state.waker.take());
            // The empty completed state is valid even after a failed wake.
            // Remaining callback owners can lock it and discard their reply.
            self.waker_state.clear_poison();
            retired
        };
        // Resource/waker destructors run outside the state lock.
        drop(retired);
    }
}

impl<T> Future for BtlePlugFuture<T> {
    type Output = T;

    /// Returns when the [BtlePlugMessageUnion] reply has been set in the
    /// [BtlePlugFutureStateShared].
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        // Waker clone/drop can execute custom code. Neither belongs inside the
        // reply-state lock, including disposal of a previous poll's waker.
        let mut next_waker = Some(cx.waker().clone());
        let (result, retired_waker) = {
            let mut state = self.waker_state.lock().unwrap();
            if let Some(msg) = state.reply_msg.take() {
                (Poll::Ready(msg), state.waker.take())
            } else {
                (
                    Poll::Pending,
                    std::mem::replace(&mut state.waker, next_waker.take()),
                )
            }
        };
        drop(retired_waker);
        drop(next_waker);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::{Context, Poll, Waker};

    #[test]
    fn producer_wake_can_reenter_reply_state_without_a_lock() {
        struct ReentrantWake(std::sync::Weak<Mutex<BtlePlugFutureState<u8>>>);
        impl std::task::Wake for ReentrantWake {
            fn wake(self: Arc<Self>) {
                let state = self.0.upgrade().unwrap();
                let guard = state
                    .try_lock()
                    .expect("Producer woke with reply state locked");
                assert_eq!(guard.reply_msg, Some(7));
            }
        }
        for _ in 0..100 {
            let mut future = BtlePlugFuture::<u8>::default();
            let state = future.get_state_clone();
            let waker = Waker::from(Arc::new(ReentrantWake(Arc::downgrade(&state))));
            assert!(
                Pin::new(&mut future)
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            set_reply(&state, 7);
            assert_eq!(
                Pin::new(&mut future).poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(7)
            );
        }
    }

    #[test]
    fn late_producer_reply_is_disposed_outside_reply_state_lock() {
        struct Reply(std::sync::Weak<Mutex<BtlePlugFutureState<Reply>>>);
        impl Drop for Reply {
            fn drop(&mut self) {
                let state = self.0.upgrade().unwrap();
                assert!(
                    state.try_lock().is_ok(),
                    "Late reply disposed with state locked"
                );
            }
        }
        for _ in 0..100 {
            let future = BtlePlugFuture::<Reply>::default();
            let state = future.get_state_clone();
            drop(future);
            set_reply(&state, Reply(Arc::downgrade(&state)));
        }
    }

    #[test]
    fn replacing_pending_waker_drops_previous_owner_outside_state_lock() {
        struct ReentrantOwner {
            state: std::sync::Weak<Mutex<BtlePlugFutureState<u8>>>,
            unlocked: Arc<std::sync::atomic::AtomicBool>,
        }
        impl std::task::Wake for ReentrantOwner {
            fn wake(self: Arc<Self>) {}
        }
        impl Drop for ReentrantOwner {
            fn drop(&mut self) {
                let state = self.state.upgrade().unwrap();
                self.unlocked.store(
                    state.try_lock().is_ok(),
                    std::sync::atomic::Ordering::SeqCst,
                );
            }
        }
        for _ in 0..100 {
            let mut future = BtlePlugFuture::<u8>::default();
            let state = future.get_state_clone();
            let unlocked = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let waker = Waker::from(Arc::new(ReentrantOwner {
                state: Arc::downgrade(&state),
                unlocked: unlocked.clone(),
            }));
            assert!(
                Pin::new(&mut future)
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            drop(waker);
            assert!(
                Pin::new(&mut future)
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            assert!(
                unlocked.load(std::sync::atomic::Ordering::SeqCst),
                "Replaced waker was dropped with its state locked"
            );
            crate::corebluetooth::future::set_reply(&state, 7);
            assert_eq!(
                Pin::new(&mut future).poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(7)
            );
        }
    }

    struct WakeOwner(Arc<()>);
    impl std::task::Wake for WakeOwner {
        fn wake(self: Arc<Self>) {
            let _ = &self.0;
        }
    }

    #[test]
    fn canceled_reply_future_releases_waker_and_ignores_late_value() {
        for _ in 0..100 {
            let owner = Arc::new(());
            let mut future = BtlePlugFuture::<Arc<()>>::default();
            let state = future.get_state_clone();
            let waker = Waker::from(Arc::new(WakeOwner(owner.clone())));
            assert!(
                Pin::new(&mut future)
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            drop(waker);
            assert_eq!(Arc::strong_count(&owner), 2);
            drop(future);
            assert_eq!(
                Arc::strong_count(&owner),
                1,
                "Canceled future retained task waker"
            );
            let reply = Arc::new(());
            crate::corebluetooth::future::set_reply(&state, reply.clone());
            assert_eq!(
                Arc::strong_count(&reply),
                1,
                "Canceled state retained late reply"
            );
        }
    }

    #[test]
    fn unpolled_or_ready_future_drop_retires_reply_state() {
        for ready in [false, true] {
            let future = BtlePlugFuture::<Arc<()>>::default();
            let state = future.get_state_clone();
            let reply = Arc::new(());
            if ready {
                crate::corebluetooth::future::set_reply(&state, reply.clone());
            }
            drop(future);
            assert_eq!(
                Arc::strong_count(&reply),
                1,
                "Dropped future retained ready value"
            );
            crate::corebluetooth::future::set_reply(&state, reply.clone());
            assert_eq!(
                Arc::strong_count(&reply),
                1,
                "Retired future accepted late value"
            );
        }
    }

    #[test]
    fn dropping_future_releases_ready_value_even_after_state_poison() {
        struct PanicWake;
        impl std::task::Wake for PanicWake {
            fn wake(self: Arc<Self>) {
                panic!("controlled wake failure");
            }
        }
        let mut future = BtlePlugFuture::<Arc<()>>::default();
        let state = future.get_state_clone();
        let waker = Waker::from(Arc::new(PanicWake));
        assert!(
            Pin::new(&mut future)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let reply = Arc::new(());
        assert!(
            std::panic::catch_unwind(|| crate::corebluetooth::future::set_reply(
                &state,
                reply.clone()
            ))
            .is_err()
        );
        assert!(
            !state.is_poisoned(),
            "Wake failure must occur outside the mutex"
        );
        assert!(
            std::panic::catch_unwind(|| {
                let _guard = state.lock().unwrap();
                panic!("controlled reply-state poison");
            })
            .is_err()
        );
        assert!(state.is_poisoned());
        drop(future);
        assert_eq!(Arc::strong_count(&reply), 1);
        {
            let retired = state.lock().unwrap();
            assert!(retired.completed && retired.reply_msg.is_none() && retired.waker.is_none());
        }
        let late_reply = Arc::new(());
        crate::corebluetooth::future::set_reply(&state, late_reply.clone());
        assert_eq!(Arc::strong_count(&late_reply), 1);
    }

    #[test]
    fn late_duplicate_completion_after_poll_is_ignored() {
        let mut future = BtlePlugFuture::<u8>::default();
        let state = future.get_state_clone();
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);

        crate::corebluetooth::future::set_reply(&state, 1);
        assert_eq!(Pin::new(&mut future).poll(&mut context), Poll::Ready(1));

        // A callback arriving after the reply was consumed must not resurrect
        // the operation or replace its terminal result.
        crate::corebluetooth::future::set_reply(&state, 2);
        assert!(matches!(
            Pin::new(&mut future).poll(&mut context),
            Poll::Pending
        ));
    }
}
