//! Bounded adapter-state observation that preserves loss across fast recovery.
use futures_util::Stream;
use tokio::sync::watch;

#[derive(Clone, Copy)]
struct Snapshot {
    value: u8,
    loss_revision: u64,
    loss_state: u8,
}
pub(crate) struct AdapterState(watch::Sender<Snapshot>);
impl Default for AdapterState {
    fn default() -> Self {
        Self(
            watch::channel(Snapshot {
                value: 1,
                loss_revision: 0,
                loss_state: 1,
            })
            .0,
        )
    }
}
impl AdapterState {
    pub(crate) fn value(&self) -> u8 {
        self.0.borrow().value
    }
    pub(crate) fn update(&self, value: u8, on_loss: impl FnOnce()) {
        self.0.send_if_modified(|snapshot| {
            if snapshot.value == value {
                return false;
            }
            if value != 4 {
                // Invalidate physical ownership before publishing the loss.
                on_loss();
                snapshot.loss_revision = snapshot.loss_revision.wrapping_add(1);
                snapshot.loss_state = value;
            }
            snapshot.value = value;
            true
        });
    }
    pub(crate) fn subscribe(&self) -> (u8, impl Stream<Item = u8> + Send + use<>) {
        let receiver = self.0.subscribe();
        let initial = *receiver.borrow();
        let stream = futures_util::stream::unfold(
            (receiver, initial.loss_revision, None),
            |(mut receiver, mut seen_loss, mut recovery)| async move {
                if let Some(revision) = recovery.take() {
                    let latest = *receiver.borrow();
                    if latest.loss_revision == revision && latest.value == 4 {
                        return Some((4, (receiver, seen_loss, recovery)));
                    }
                }
                receiver.changed().await.ok()?;
                let snapshot = *receiver.borrow_and_update();
                let value = if snapshot.loss_revision != seen_loss {
                    seen_loss = snapshot.loss_revision;
                    if snapshot.value == 4 {
                        recovery = Some(seen_loss);
                    }
                    snapshot.loss_state
                } else {
                    snapshot.value
                };
                Some((value, (receiver, seen_loss, recovery)))
            },
        );
        (initial.value, stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    #[tokio::test]
    async fn loss_then_recovery_before_poll_is_observed_by_every_engine() {
        let state = AdapterState::default();
        state.update(4, || {});
        let (initial, first) = state.subscribe();
        let (_, second) = state.subscribe();
        assert_eq!(initial, 4);
        let mut first = Box::pin(first);
        let mut second = Box::pin(second);
        state.update(2, || {});
        state.update(4, || {});
        for stream in [&mut first, &mut second] {
            assert_eq!(stream.next().await, Some(2));
            assert_eq!(stream.next().await, Some(4));
        }
        assert_eq!(state.value(), 4);
    }
    #[tokio::test]
    async fn permission_loss_survives_recovery_and_stale_ready_is_not_emitted() {
        let state = AdapterState::default();
        state.update(4, || {});
        let (_, events) = state.subscribe();
        let mut events = Box::pin(events);
        state.update(3, || {});
        state.update(4, || {});
        assert_eq!(events.next().await, Some(3));
        state.update(2, || {});
        assert_eq!(events.next().await, Some(2));
        state.update(4, || {});
        assert_eq!(events.next().await, Some(4));
    }
    #[tokio::test]
    async fn duplicates_do_not_invalidate_and_new_subscriber_gets_current_state() {
        let state = AdapterState::default();
        let losses = std::sync::atomic::AtomicUsize::new(0);
        for value in [4, 4, 2, 2, 4, 4] {
            state.update(value, || {
                losses.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            });
        }
        assert_eq!(losses.load(std::sync::atomic::Ordering::SeqCst), 1);
        let (initial, events) = state.subscribe();
        assert_eq!(initial, 4);
        let mut events = Box::pin(events);
        assert!(futures_util::poll!(events.next()).is_pending());
    }
}
