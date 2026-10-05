//! Bounded synchronous signal publication. Overflow retires publication and
//! reports once; the caller retains the match until its normal cleanup.
use tokio::sync::mpsc;

const CAPACITY: usize = 256;

pub(crate) struct SignalQueue<T, F> {
    sender: mpsc::Sender<T>,
    overflow: F,
    retired: bool,
}

pub(crate) fn queue<T, F: Fn()>(overflow: F) -> (SignalQueue<T, F>, mpsc::Receiver<T>) {
    let (sender, receiver) = mpsc::channel(CAPACITY);
    (
        SignalQueue {
            sender,
            overflow,
            retired: false,
        },
        receiver,
    )
}

impl<T, F: Fn()> SignalQueue<T, F> {
    pub(crate) fn push(&mut self, value: T) {
        if self.retired {
            return;
        }
        match self.sender.try_send(value) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.retired = true;
                (self.overflow)();
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // Consumer teardown is not an overflow. Keep the D-Bus match
                // registered until MessageStream::drop removes it explicitly.
                self.retired = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct Live(Arc<AtomicUsize>);
    impl Drop for Live {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn overflow_bounds_retention_reports_once_and_retires_publication() {
        let live = Arc::new(AtomicUsize::new(0));
        let failures = Arc::new(AtomicUsize::new(0));
        let observed = failures.clone();
        let (mut sender, mut receiver) = queue(move || {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        for _ in 0..CAPACITY + 20 {
            live.fetch_add(1, Ordering::SeqCst);
            sender.push(Live(live.clone()));
        }
        assert_eq!(live.load(Ordering::SeqCst), CAPACITY);
        assert_eq!(failures.load(Ordering::SeqCst), 1);
        drop(receiver.try_recv().unwrap());
        live.fetch_add(1, Ordering::SeqCst);
        sender.push(Live(live.clone()));
        assert_eq!(live.load(Ordering::SeqCst), CAPACITY - 1);
        assert_eq!(failures.load(Ordering::SeqCst), 1);
        drop(receiver);
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn closed_consumer_is_not_an_overflow_and_drops_new_values() {
        let live = Arc::new(AtomicUsize::new(1));
        let (mut sender, receiver) = queue(|| panic!("consumer close is not an overflow"));
        drop(receiver);
        sender.push(Live(live.clone()));
        assert_eq!(live.load(Ordering::SeqCst), 0);
        live.fetch_add(1, Ordering::SeqCst);
        sender.push(Live(live.clone()));
        assert_eq!(live.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn cancelled_receive_preserves_fifo_and_last_sender_closes() {
        use futures::FutureExt;
        let (mut sender, mut receiver) = queue(|| panic!("unexpected overflow"));
        assert!(receiver.recv().now_or_never().is_none());
        sender.push(1_u8);
        sender.push(2);
        assert_eq!(receiver.recv().await, Some(1));
        assert_eq!(receiver.recv().await, Some(2));
        drop(sender);
        assert_eq!(receiver.recv().await, None);
    }
}
