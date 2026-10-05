//! Android's btleplug adapter is process-wide. This coordinator owns its scan
//! state; engines hold weakly registered lease tokens, not independent scanners.
use crate::codec::Error;
use std::{
    future::Future,
    sync::{
        Arc, Weak,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
};
use tokio::sync::Mutex;

#[derive(Default, PartialEq)]
enum Physical {
    #[default]
    Off,
    On,
    Uncertain,
}
#[derive(Default)]
struct State {
    owners: Vec<Weak<()>>,
    physical: Physical,
    revision: u64,
}
#[derive(Default)]
pub(crate) struct SharedScan(
    Mutex<State>,
    AtomicU64,
    AtomicU8,
    #[cfg(any(target_os = "android", test))] std::sync::Mutex<u64>,
);
impl SharedScan {
    pub(crate) fn invalidate(&self, reason: u8) {
        self.2.store(reason, Ordering::Release);
        self.1.fetch_add(1, Ordering::AcqRel);
    }
    #[cfg(any(target_os = "android", test))]
    pub(crate) fn fail_attempt(&self, generation: u64) {
        // Each physical failure is broadcast to several engine event streams.
        // Only its first observer invalidates shared leases; delayed observers
        // cannot invalidate a later recovery attempt.
        let mut failed = self.3.lock().unwrap_or_else(|p| p.into_inner());
        if *failed < generation {
            *failed = generation;
            self.invalidate(19);
        }
    }
    fn refresh(&self, state: &mut State) {
        let revision = self.1.load(Ordering::Acquire);
        if state.revision != revision {
            state.revision = revision;
            state.owners.clear();
            if state.physical != Physical::Off {
                state.physical = Physical::Uncertain;
            }
        }
    }
    pub(crate) async fn start(
        &self,
        owner: &Arc<()>,
        start: impl Future<Output = Result<(), Error>> + Send,
        recover: impl Future<Output = Result<(), Error>> + Send,
    ) -> Result<(), Error> {
        // OS scan transitions must serialize. Callers bound these futures and
        // drop the guard on cancellation; no engine/connection lock is held.
        let mut state = self.0.lock().await;
        self.refresh(&mut state);
        state.owners.retain(|owner| owner.strong_count() != 0);
        if state
            .owners
            .iter()
            .any(|entry| entry.ptr_eq(&Arc::downgrade(owner)))
        {
            return if state.physical == Physical::On {
                Ok(())
            } else {
                Err(Error::new(16, "Shared scanner requires cleanup"))
            };
        }
        if state.owners.is_empty() && state.physical != Physical::Off {
            // A failed engine cleanup may leave an OS scan active. Weak tokens
            // do not retain that engine; recover before granting a fresh lease.
            state.physical = Physical::Uncertain;
            recover.await?;
            state.physical = Physical::Off;
        }
        if state.physical == Physical::Uncertain {
            return Err(Error::new(16, "Shared scanner transition requires cleanup"));
        }
        state.owners.push(Arc::downgrade(owner));
        if state.physical == Physical::On {
            return Ok(());
        }
        state.physical = Physical::Uncertain;
        start.await?;
        if state.revision != self.1.load(Ordering::Acquire) {
            return Err(Error::new(
                u32::from(self.2.load(Ordering::Acquire)),
                "Adapter changed during scanner startup",
            ));
        }
        state.physical = Physical::On;
        Ok(())
    }
    pub(crate) async fn stop(
        &self,
        owner: &Arc<()>,
        stop: impl Future<Output = Result<(), Error>> + Send,
    ) -> Result<(), Error> {
        let mut state = self.0.lock().await;
        self.refresh(&mut state);
        state.owners.retain(|owner| owner.strong_count() != 0);
        if state.owners.is_empty() && state.physical != Physical::Off {
            // An invalidated/orphan transition has no live scan leases. Its
            // cleanup must run even if the requesting engine lost its lease.
            state.physical = Physical::Uncertain;
            stop.await?;
            state.physical = Physical::Off;
            return Ok(());
        }
        let token = Arc::downgrade(owner);
        let Some(index) = state.owners.iter().position(|entry| entry.ptr_eq(&token)) else {
            // Cancellation while waiting for start never acquired an OS lease.
            return Ok(());
        };
        if state.owners.len() > 1 || state.physical == Physical::Off {
            state.owners.swap_remove(index);
            return Ok(());
        }
        state.physical = Physical::Uncertain;
        stop.await?;
        state.physical = Physical::Off;
        state.owners.swap_remove(index);
        Ok(())
    }
}

#[cfg(target_os = "android")]
pub(crate) fn process_scanner() -> &'static SharedScan {
    static SCANNER: std::sync::OnceLock<SharedScan> = std::sync::OnceLock::new();
    SCANNER.get_or_init(SharedScan::default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    async fn counted(counter: &AtomicUsize) -> Result<(), Error> {
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    #[tokio::test]
    async fn duplicate_scan_failures_do_not_invalidate_retry_leases() {
        let scanner = SharedScan::default();
        let starts = AtomicUsize::new(0);
        let stops = AtomicUsize::new(0);
        for generation in 1..=100 {
            let old = Arc::new(());
            let next = Arc::new(());
            scanner
                .start(&old, counted(&starts), counted(&stops))
                .await
                .unwrap();
            scanner.fail_attempt(generation);
            scanner
                .start(&next, counted(&starts), counted(&stops))
                .await
                .unwrap();
            // Another engine consumes the old failure after recovery.
            scanner.fail_attempt(generation);
            scanner.fail_attempt(generation - 1);
            scanner.stop(&old, counted(&stops)).await.unwrap();
            assert_eq!(stops.load(Ordering::SeqCst), 2 * generation as usize - 1);
            scanner.stop(&next, counted(&stops)).await.unwrap();
            assert_eq!(stops.load(Ordering::SeqCst), 2 * generation as usize);
        }
    }

    #[tokio::test]
    async fn rapid_loss_requires_fresh_scan_and_old_stop_preserves_new_owner() {
        let scanner = SharedScan::default();
        let starts = AtomicUsize::new(0);
        let stops = AtomicUsize::new(0);
        for _ in 0..100 {
            let old = Arc::new(());
            let next = Arc::new(());
            scanner
                .start(&old, counted(&starts), counted(&stops))
                .await
                .unwrap();
            scanner.invalidate(2);
            // Recovery is already visible to a new engine, before the old
            // engine has polled its adapter-state event or released its lease.
            scanner
                .start(&next, counted(&starts), counted(&stops))
                .await
                .unwrap();
            let before = stops.load(Ordering::SeqCst);
            scanner.stop(&old, counted(&stops)).await.unwrap();
            assert_eq!(stops.load(Ordering::SeqCst), before);
            scanner.stop(&next, counted(&stops)).await.unwrap();
            assert!(scanner.0.lock().await.owners.is_empty());
        }
        assert_eq!(starts.load(Ordering::SeqCst), 200);
        assert_eq!(stops.load(Ordering::SeqCst), 200);
    }
    #[tokio::test]
    async fn loss_during_os_start_fails_and_cleanup_stops_uncertain_scan() {
        for reason in [1, 2, 3] {
            let scanner = SharedScan::default();
            let owner = Arc::new(());
            let stops = AtomicUsize::new(0);
            let result = scanner
                .start(
                    &owner,
                    async {
                        scanner.invalidate(reason);
                        Ok(())
                    },
                    counted(&stops),
                )
                .await;
            assert_eq!(result.unwrap_err().code, u32::from(reason));
            scanner.stop(&owner, counted(&stops)).await.unwrap();
            assert_eq!(stops.load(Ordering::SeqCst), 1);
            assert!(scanner.0.lock().await.owners.is_empty());
        }
    }
    #[tokio::test]
    async fn cancelled_recovery_still_cleans_unowned_physical_scan() {
        let scanner = Arc::new(SharedScan::default());
        let old = Arc::new(());
        let next = Arc::new(());
        scanner
            .start(&old, async { Ok(()) }, async { Ok(()) })
            .await
            .unwrap();
        scanner.invalidate(2);
        let (entered, recovering) = tokio::sync::oneshot::channel();
        let task = tokio::spawn({
            let scanner = scanner.clone();
            let next = next.clone();
            async move {
                scanner
                    .start(&next, async { Ok(()) }, async {
                        entered.send(()).unwrap();
                        std::future::pending().await
                    })
                    .await
            }
        });
        recovering.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let stops = AtomicUsize::new(0);
        scanner.stop(&next, counted(&stops)).await.unwrap();
        assert_eq!(stops.load(Ordering::SeqCst), 1);
        scanner
            .start(&next, async { Ok(()) }, counted(&stops))
            .await
            .unwrap();
        scanner.stop(&old, counted(&stops)).await.unwrap();
        assert_eq!(stops.load(Ordering::SeqCst), 1);
        scanner.stop(&next, counted(&stops)).await.unwrap();
        assert_eq!(stops.load(Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn hundred_owners_share_one_start_and_only_last_owner_stops() {
        let scanner = SharedScan::default();
        let starts = AtomicUsize::new(0);
        let stops = AtomicUsize::new(0);
        let owners: Vec<_> = (0..100).map(|_| Arc::new(())).collect();
        for owner in &owners {
            scanner
                .start(owner, counted(&starts), counted(&stops))
                .await
                .unwrap();
            scanner
                .start(owner, counted(&starts), counted(&stops))
                .await
                .unwrap();
        }
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        for owner in &owners[..99] {
            scanner.stop(owner, counted(&stops)).await.unwrap();
            scanner.stop(owner, counted(&stops)).await.unwrap();
            assert_eq!(stops.load(Ordering::SeqCst), 0);
        }
        scanner.stop(&owners[99], counted(&stops)).await.unwrap();
        scanner.stop(&owners[99], counted(&stops)).await.unwrap();
        assert_eq!(stops.load(Ordering::SeqCst), 1);
        let state = scanner.0.lock().await;
        assert!(state.owners.is_empty());
        assert!(state.physical == Physical::Off);
    }
    #[tokio::test]
    async fn cancelled_start_keeps_uncertainty_until_owned_cleanup() {
        let scanner = Arc::new(SharedScan::default());
        let owner = Arc::new(());
        let other = Arc::new(());
        let (entered, started) = tokio::sync::oneshot::channel();
        let task = tokio::spawn({
            let scanner = scanner.clone();
            let owner = owner.clone();
            async move {
                scanner
                    .start(
                        &owner,
                        async {
                            entered.send(()).unwrap();
                            std::future::pending().await
                        },
                        async { Ok(()) },
                    )
                    .await
            }
        });
        started.await.unwrap();
        // The second owner is cancelled while waiting for the transition lock.
        let mut waiting = Box::pin(scanner.start(&other, async { Ok(()) }, async { Ok(()) }));
        assert!(futures_util::poll!(waiting.as_mut()).is_pending());
        drop(waiting);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            scanner
                .start(&other, async { Ok(()) }, async { Ok(()) })
                .await
                .unwrap_err()
                .code,
            16
        );
        let stops = AtomicUsize::new(0);
        scanner.stop(&other, counted(&stops)).await.unwrap();
        assert_eq!(stops.load(Ordering::SeqCst), 0);
        scanner.stop(&owner, counted(&stops)).await.unwrap();
        assert_eq!(stops.load(Ordering::SeqCst), 1);
        scanner
            .start(&other, async { Ok(()) }, counted(&stops))
            .await
            .unwrap();
        scanner.stop(&other, counted(&stops)).await.unwrap();
        assert_eq!(stops.load(Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn cancelled_last_stop_requires_cleanup_before_a_new_owner() {
        let scanner = Arc::new(SharedScan::default());
        let owner = Arc::new(());
        let next = Arc::new(());
        scanner
            .start(&owner, async { Ok(()) }, async { Ok(()) })
            .await
            .unwrap();
        let (entered, stopping) = tokio::sync::oneshot::channel();
        let task = tokio::spawn({
            let scanner = scanner.clone();
            let owner = owner.clone();
            async move {
                scanner
                    .stop(&owner, async {
                        entered.send(()).unwrap();
                        std::future::pending().await
                    })
                    .await
            }
        });
        stopping.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(
            scanner
                .start(&next, async { Ok(()) }, async { Ok(()) })
                .await
                .unwrap_err()
                .code,
            16
        );
        scanner.stop(&owner, async { Ok(()) }).await.unwrap();
        scanner
            .start(&next, async { Ok(()) }, async { Ok(()) })
            .await
            .unwrap();
        scanner.stop(&next, async { Ok(()) }).await.unwrap();
        assert!(scanner.0.lock().await.owners.is_empty());
    }
    #[tokio::test]
    async fn failed_start_cleanup_and_hundred_cycles_leave_no_owners() {
        let scanner = SharedScan::default();
        let starts = AtomicUsize::new(0);
        let stops = AtomicUsize::new(0);
        for _ in 0..100 {
            let owner = Arc::new(());
            assert_eq!(
                scanner
                    .start(
                        &owner,
                        async { Err(Error::new(15, "OS start failed")) },
                        counted(&stops)
                    )
                    .await
                    .unwrap_err()
                    .code,
                15
            );
            scanner.stop(&owner, counted(&stops)).await.unwrap();
            scanner
                .start(&owner, counted(&starts), counted(&stops))
                .await
                .unwrap();
            scanner.stop(&owner, counted(&stops)).await.unwrap();
            let state = scanner.0.lock().await;
            assert!(state.owners.is_empty());
            assert!(state.physical == Physical::Off);
        }
        assert_eq!(starts.load(Ordering::SeqCst), 100);
        assert_eq!(stops.load(Ordering::SeqCst), 200);
    }
    #[tokio::test]
    async fn failed_cleanup_does_not_retain_engine_and_next_owner_recovers() {
        let scanner = SharedScan::default();
        let owner = Arc::new(());
        scanner
            .start(&owner, async { Ok(()) }, async { Ok(()) })
            .await
            .unwrap();
        assert_eq!(
            scanner
                .stop(&owner, async { Err(Error::new(15, "OS stop failed")) })
                .await
                .unwrap_err()
                .code,
            15
        );
        let weak = Arc::downgrade(&owner);
        drop(owner);
        assert!(weak.upgrade().is_none());
        let next = Arc::new(());
        let stops = AtomicUsize::new(0);
        scanner
            .start(&next, async { Ok(()) }, counted(&stops))
            .await
            .unwrap();
        assert_eq!(stops.load(Ordering::SeqCst), 1);
        scanner.stop(&next, counted(&stops)).await.unwrap();
        assert!(scanner.0.lock().await.owners.is_empty());
    }
}
