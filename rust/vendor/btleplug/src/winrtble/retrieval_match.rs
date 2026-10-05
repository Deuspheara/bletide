//! Resolve the selector union before starting an optional native service lookup.
use crate::{Result, api};
use std::future::Future;
use uuid::Uuid;

/// windows-core 0.62 maps a successful null interface to Error::empty (HRESULT 0).
/// An operation failure, cancellation or failed status query is never absence.
pub(crate) fn is_missing_result(
    result_code: i32,
    async_status: Option<i32>,
    async_error: Option<i32>,
) -> bool {
    result_code == 0 && async_status == Some(1) && async_error == Some(0)
}

pub(crate) async fn matches<F: Future<Output = Result<Vec<Uuid>>>>(
    already_matched: bool,
    requested_services: Option<&[Uuid]>,
    lookup: impl FnOnce() -> F,
) -> Result<bool> {
    if already_matched {
        return Ok(true);
    }
    let Some(requested) = requested_services.filter(|services| !services.is_empty()) else {
        return Ok(false);
    };
    let services = lookup().await?;
    Ok(api::matches_service(&services, requested))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    #[test]
    fn only_successfully_completed_null_result_is_absent() {
        assert!(is_missing_result(0, Some(1), Some(0)));
        for status in [None, Some(0), Some(2), Some(3), Some(-1)] {
            assert!(!is_missing_result(0, status, Some(0)));
        }
        for code in [0x80070005_u32 as i32, 0x80004003_u32 as i32, -1, 1] {
            assert!(!is_missing_result(code, Some(1), Some(0)));
            assert!(!is_missing_result(0, Some(1), Some(code)));
        }
        assert!(!is_missing_result(0, Some(1), None));
    }

    #[tokio::test]
    async fn identifier_union_and_empty_selectors_do_not_start_service_lookup() {
        let service = Uuid::nil();
        for requested in [None, Some(&[][..]), Some(&[service][..])] {
            assert!(
                matches(true, requested, || async {
                    panic!("unnecessary service lookup")
                })
                .await
                .unwrap()
            );
        }
        for requested in [None, Some(&[][..])] {
            assert!(
                !matches(false, requested, || async {
                    panic!("empty selector started lookup")
                })
                .await
                .unwrap()
            );
        }
    }

    #[tokio::test]
    async fn required_lookup_preserves_failure_and_allows_explicit_retry() {
        let service = Uuid::nil();
        for _ in 0..100 {
            assert!(matches!(
                matches(false, Some(&[service]), || async {
                    Err(Error::PermissionDenied)
                })
                .await,
                Err(Error::PermissionDenied)
            ));
            assert!(
                matches(false, Some(&[service]), || async { Ok(vec![service]) })
                    .await
                    .unwrap()
            );
            assert!(
                !matches(false, Some(&[service]), || async {
                    Ok(vec![Uuid::from_u128(1)])
                })
                .await
                .unwrap()
            );
        }
    }

    struct Owned(Arc<AtomicUsize>);
    impl Drop for Owned {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn cancellation_drops_required_lookup_without_retaining_an_owner() {
        let service = Uuid::nil();
        let live = Arc::new(AtomicUsize::new(0));
        for _ in 0..100 {
            live.fetch_add(1, Ordering::SeqCst);
            let owner = Owned(live.clone());
            let (started, observed) = tokio::sync::oneshot::channel();
            let mut lookup = Box::pin(matches(
                false,
                Some(std::slice::from_ref(&service)),
                || async move {
                    let _owner = owner;
                    started.send(()).unwrap();
                    std::future::pending::<Result<Vec<Uuid>>>().await
                },
            ));
            tokio::select! {
                result = &mut lookup => panic!("lookup completed unexpectedly: {result:?}"),
                _ = observed => (),
            }
            drop(lookup);
            assert_eq!(live.load(Ordering::SeqCst), 0);
            assert!(
                matches(false, Some(&[service]), || async { Ok(vec![service]) })
                    .await
                    .unwrap()
            );
        }
    }
}
