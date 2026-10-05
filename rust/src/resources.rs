//! Development-only counters follow owned resources through joined cleanup.
//! Guards retain counter storage, never engines or BLE platform objects.
#[cfg(any(test, debug_assertions, feature = "test-support"))]
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Clone, Copy)]
pub(crate) enum Kind {
    AdapterWorker = 0,
    ConnectionWorker = 1,
    NotificationStream = 2,
    Subscription = 3,
}
#[cfg(any(test, debug_assertions, feature = "test-support"))]
static TOTAL: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];

#[derive(Clone, Default)]
pub(crate) struct Resources {
    #[cfg(any(test, debug_assertions, feature = "test-support"))]
    counts: Arc<[AtomicU64; 4]>,
}
pub(crate) struct Guard {
    #[cfg(any(test, debug_assertions, feature = "test-support"))]
    counts: Arc<[AtomicU64; 4]>,
    #[cfg(any(test, debug_assertions, feature = "test-support"))]
    index: usize,
}
impl Resources {
    pub(crate) fn track(&self, kind: Kind) -> Guard {
        #[cfg(any(test, debug_assertions, feature = "test-support"))]
        {
            let index = kind as usize;
            self.counts[index].fetch_add(1, Ordering::AcqRel);
            TOTAL[index].fetch_add(1, Ordering::AcqRel);
            Guard {
                counts: self.counts.clone(),
                index,
            }
        }
        #[cfg(not(any(test, debug_assertions, feature = "test-support")))]
        {
            let _ = kind;
            Guard {}
        }
    }
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> [u64; 4] {
        std::array::from_fn(|i| self.counts[i].load(Ordering::Acquire))
    }
}
#[cfg(any(test, debug_assertions, feature = "test-support"))]
impl Drop for Guard {
    fn drop(&mut self) {
        self.counts[self.index].fetch_sub(1, Ordering::AcqRel);
        TOTAL[self.index].fetch_sub(1, Ordering::AcqRel);
    }
}
#[cfg(any(debug_assertions, feature = "test-support"))]
pub(crate) fn total(kind: u32) -> Option<u64> {
    TOTAL
        .get(kind as usize)
        .map(|count| count.load(Ordering::Acquire))
}
