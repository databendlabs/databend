// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use backoff::backoff::Backoff;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_storages_fuse::operations::set_backoff;
use rand::Rng;
use rand::thread_rng;
use tokio::sync::Notify;
use tokio::time::sleep;

const LEASE_RUNNING: u8 = 0;
const LEASE_SHUTDOWN_REQUESTED: u8 = 1;
const LEASE_RENEW_FAILED: u8 = 2;

#[async_trait]
pub(super) trait LeaseOps: Send + Sync + 'static {
    async fn extend(&self) -> Result<()>;

    async fn delete(&self) -> Result<()>;

    fn description(&self) -> &str;
}

#[derive(Debug)]
enum LeaseExit {
    Released(Result<()>),
    RenewFailed(ErrorCode),
}

#[derive(Debug)]
enum RenewResult {
    Renewed,
    Shutdown,
    Failed(ErrorCode),
}

/// Maintains a TTL-backed lease after its Meta record has been created.
///
/// Acquisition remains the holder's responsibility. This keeper only renews the lease, stops its
/// owner after a terminal renewal failure, and deletes the lease after a normal shutdown. The
/// lifecycle follows the same grant/keepalive/revoke model used by systems such as etcd, while
/// retaining Databend's existing single-RPC renewal protocol.
pub(super) struct LeaseKeeper {
    state: AtomicU8,
    notify: Notify,
}

impl Default for LeaseKeeper {
    fn default() -> Self {
        Self {
            state: AtomicU8::new(LEASE_RUNNING),
            notify: Notify::new(),
        }
    }
}

impl LeaseKeeper {
    pub(super) fn start<O, F>(self: &Arc<Self>, ttl: Duration, ops: O, on_renew_failed: F)
    where
        O: LeaseOps,
        F: FnOnce(ErrorCode) + Send + 'static,
    {
        let keeper = self.clone();
        let description = ops.description().to_string();
        GlobalIORuntime::instance().spawn(async move {
            match keeper.run(ttl, &ops).await {
                LeaseExit::Released(Ok(())) => {
                    log::debug!("released {description}");
                }
                LeaseExit::Released(Err(error)) => {
                    // Active deletion only shortens cleanup latency. TTL remains the final cleanup
                    // mechanism when bounded retries cannot reach Meta.
                    log::warn!("failed to release {description}: {error}");
                }
                LeaseExit::RenewFailed(error) => {
                    log::error!("failed to renew {description}: {error}");
                    on_renew_failed(error);
                }
            }
        });
    }

    pub(super) fn shutdown(&self) {
        if self
            .state
            .compare_exchange(
                LEASE_RUNNING,
                LEASE_SHUTDOWN_REQUESTED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.notify.notify_one();
        }
    }

    async fn run<O: LeaseOps>(&self, ttl: Duration, ops: &O) -> LeaseExit {
        let sleep_range = (ttl / 3)..=(ttl * 2 / 3);

        loop {
            if self.state.load(Ordering::Acquire) == LEASE_SHUTDOWN_REQUESTED {
                return LeaseExit::Released(self.try_delete(ttl, ops).await);
            }

            let delay = thread_rng().gen_range(sleep_range.clone());
            tokio::select! {
                _ = self.notify.notified() => {
                    return LeaseExit::Released(self.try_delete(ttl, ops).await);
                }
                _ = sleep(delay) => {}
            }

            match self.try_extend(ttl - delay, ops).await {
                RenewResult::Renewed => {}
                RenewResult::Shutdown => {
                    return LeaseExit::Released(self.try_delete(ttl, ops).await);
                }
                RenewResult::Failed(error) => return LeaseExit::RenewFailed(error),
            }
        }
    }

    async fn try_extend<O: LeaseOps>(&self, max_retry_elapsed: Duration, ops: &O) -> RenewResult {
        let mut backoff = set_backoff(
            Some(Duration::from_millis(2)),
            None,
            Some(max_retry_elapsed),
        );

        loop {
            if self.state.load(Ordering::Acquire) == LEASE_SHUTDOWN_REQUESTED {
                return RenewResult::Shutdown;
            }

            let result = ops.extend().await;
            if self.state.load(Ordering::Acquire) == LEASE_SHUTDOWN_REQUESTED {
                return RenewResult::Shutdown;
            }

            match result {
                Ok(()) => return RenewResult::Renewed,
                Err(error) if !Self::is_retryable(&error) => {
                    return self.finish_renew_failure(error);
                }
                Err(error) => {
                    let Some(delay) = backoff.next_backoff() else {
                        return self.finish_renew_failure(error);
                    };
                    log::debug!(
                        "failed to renew {}, retrying in {} ms: {error}",
                        ops.description(),
                        delay.as_millis()
                    );
                    tokio::select! {
                        _ = self.notify.notified() => return RenewResult::Shutdown,
                        _ = sleep(delay) => {}
                    }
                }
            }
        }
    }

    fn is_retryable(error: &ErrorCode) -> bool {
        matches!(
            error.code(),
            ErrorCode::META_SERVICE_ERROR | ErrorCode::TXN_RETRY_MAX_TIMES
        )
    }

    fn finish_renew_failure(&self, error: ErrorCode) -> RenewResult {
        match self.state.compare_exchange(
            LEASE_RUNNING,
            LEASE_RENEW_FAILED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) | Err(LEASE_RENEW_FAILED) => RenewResult::Failed(error),
            Err(LEASE_SHUTDOWN_REQUESTED) => RenewResult::Shutdown,
            Err(state) => unreachable!("invalid lease state: {state}"),
        }
    }

    async fn try_delete<O: LeaseOps>(&self, max_retry_elapsed: Duration, ops: &O) -> Result<()> {
        let mut backoff = set_backoff(
            Some(Duration::from_millis(2)),
            None,
            Some(max_retry_elapsed),
        );

        loop {
            match ops.delete().await {
                Ok(()) => return Ok(()),
                Err(error) if !Self::is_retryable(&error) => return Err(error),
                Err(error) => {
                    let Some(delay) = backoff.next_backoff() else {
                        return Err(error);
                    };
                    log::debug!(
                        "failed to delete {}, retrying in {} ms: {error}",
                        ops.description(),
                        delay.as_millis()
                    );
                    sleep(delay).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use async_trait::async_trait;
    use databend_common_exception::ErrorCode;
    use databend_common_exception::Result;
    use tokio::sync::Notify;

    use super::LeaseExit;
    use super::LeaseKeeper;
    use super::LeaseOps;
    use super::RenewResult;

    struct TestLeaseOps {
        extend_attempts: AtomicUsize,
        delete_attempts: AtomicUsize,
        extend_failures: usize,
        delete_failures: usize,
        extend_error: ErrorCode,
    }

    impl TestLeaseOps {
        fn new(extend_failures: usize, delete_failures: usize) -> Self {
            Self {
                extend_attempts: AtomicUsize::new(0),
                delete_attempts: AtomicUsize::new(0),
                extend_failures,
                delete_failures,
                extend_error: ErrorCode::MetaServiceError("extend failed"),
            }
        }

        fn with_extend_error(mut self, error: ErrorCode) -> Self {
            self.extend_error = error;
            self
        }
    }

    #[async_trait]
    impl LeaseOps for TestLeaseOps {
        async fn extend(&self) -> Result<()> {
            if self.extend_attempts.fetch_add(1, Ordering::SeqCst) < self.extend_failures {
                Err(self.extend_error.clone())
            } else {
                Ok(())
            }
        }

        async fn delete(&self) -> Result<()> {
            if self.delete_attempts.fetch_add(1, Ordering::SeqCst) < self.delete_failures {
                Err(ErrorCode::MetaServiceError("delete failed"))
            } else {
                Ok(())
            }
        }

        fn description(&self) -> &str {
            "test lease"
        }
    }

    #[tokio::test]
    async fn test_extend_retries_meta_service_errors() {
        let keeper = LeaseKeeper::default();
        let ops = TestLeaseOps::new(2, 0);

        let result = keeper.try_extend(Duration::from_secs(1), &ops).await;

        assert!(matches!(result, RenewResult::Renewed));
        assert_eq!(ops.extend_attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_extend_stops_after_retry_deadline() {
        let keeper = LeaseKeeper::default();
        let ops = TestLeaseOps::new(usize::MAX, 0);

        let result = keeper.try_extend(Duration::ZERO, &ops).await;

        let RenewResult::Failed(error) = result else {
            panic!("expected renewal failure");
        };
        assert_eq!(error.code(), ErrorCode::META_SERVICE_ERROR);
        assert_eq!(ops.extend_attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_extend_retries_txn_retry_max_times() {
        let keeper = LeaseKeeper::default();
        let ops = TestLeaseOps::new(2, 0)
            .with_extend_error(ErrorCode::TxnRetryMaxTimes("retries exhausted"));

        let result = keeper.try_extend(Duration::from_secs(1), &ops).await;

        assert!(matches!(result, RenewResult::Renewed));
        assert_eq!(ops.extend_attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_extend_does_not_retry_lease_expired() {
        let keeper = LeaseKeeper::default();
        let ops =
            TestLeaseOps::new(usize::MAX, 0).with_extend_error(ErrorCode::LeaseExpired("expired"));

        let result = keeper.try_extend(Duration::from_secs(1), &ops).await;

        let RenewResult::Failed(error) = result else {
            panic!("expected renewal failure");
        };
        assert_eq!(error.code(), ErrorCode::LEASE_EXPIRED);
        assert_eq!(ops.extend_attempts.load(Ordering::SeqCst), 1);
    }

    struct BlockingLeaseOps {
        started: Arc<Notify>,
        finish: Arc<Notify>,
    }

    #[async_trait]
    impl LeaseOps for BlockingLeaseOps {
        async fn extend(&self) -> Result<()> {
            self.started.notify_one();
            self.finish.notified().await;
            Err(ErrorCode::LeaseExpired("expired"))
        }

        async fn delete(&self) -> Result<()> {
            Ok(())
        }

        fn description(&self) -> &str {
            "blocking lease"
        }
    }

    #[tokio::test]
    async fn test_shutdown_wins_over_inflight_extend_failure() {
        let keeper = Arc::new(LeaseKeeper::default());
        let started = Arc::new(Notify::new());
        let finish = Arc::new(Notify::new());
        let ops = BlockingLeaseOps {
            started: started.clone(),
            finish: finish.clone(),
        };
        let task_keeper = keeper.clone();
        let task = databend_common_base::runtime::spawn(async move {
            task_keeper.try_extend(Duration::from_secs(1), &ops).await
        });

        started.notified().await;
        keeper.shutdown();
        finish.notify_one();

        assert!(matches!(task.await.unwrap(), RenewResult::Shutdown));
    }

    #[tokio::test]
    async fn test_delete_retries_meta_service_errors() {
        let keeper = LeaseKeeper::default();
        let ops = TestLeaseOps::new(0, 2);

        let result = keeper.try_delete(Duration::from_secs(1), &ops).await;

        assert!(result.is_ok());
        assert_eq!(ops.delete_attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_normal_shutdown_deletes_lease() {
        let keeper = Arc::new(LeaseKeeper::default());
        let task_keeper = keeper.clone();
        let task = databend_common_base::runtime::spawn(async move {
            let ops = TestLeaseOps::new(0, 0);
            let result = task_keeper.run(Duration::from_secs(3), &ops).await;
            (result, ops.delete_attempts.load(Ordering::SeqCst))
        });

        keeper.shutdown();

        let (result, delete_attempts) = task.await.unwrap();
        assert!(matches!(result, LeaseExit::Released(Ok(()))));
        assert_eq!(delete_attempts, 1);
    }

    #[tokio::test]
    async fn test_renew_failure_does_not_delete_lease() {
        let keeper = LeaseKeeper::default();
        let ops =
            TestLeaseOps::new(usize::MAX, 0).with_extend_error(ErrorCode::LeaseExpired("expired"));

        let result = keeper.run(Duration::ZERO, &ops).await;

        let LeaseExit::RenewFailed(error) = result else {
            panic!("expected renewal failure");
        };
        assert_eq!(error.code(), ErrorCode::LEASE_EXPIRED);
        assert_eq!(ops.extend_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(ops.delete_attempts.load(Ordering::SeqCst), 0);
    }
}
