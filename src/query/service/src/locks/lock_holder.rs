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
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use databend_common_catalog::catalog::Catalog;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_app::schema::CreateLockRevReq;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_meta_app::schema::LockMeta;
use databend_common_meta_app::schema::TableLockIdentV2;
use databend_common_meta_store::MetaStore;
use databend_common_meta_store::WatchStream;
use databend_common_metrics::lock::record_acquired_lock_nums;
use databend_common_metrics::lock::record_created_lock_nums;
use databend_common_users::UserApiProvider;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::MetaError;
use databend_meta_client::types::SeqV;
use databend_meta_client::types::protobuf::WatchRequest;
use databend_meta_client::types::protobuf::watch_request::FilterType;
use futures_util::StreamExt;
use tokio::time::timeout;

use crate::locks::lease_keeper::LeaseKeeper;
use crate::locks::lease_keeper::LeaseOps;
use crate::meta_service_error;
use crate::sessions::SessionManager;

/// How long a waiter whose watch failed waits before it lists the revisions again.
const WATCH_RETRY_DELAY: Duration = Duration::from_secs(1);

/// The meta-service reads a waiter makes on the revision before its own. A trait, so that a test
/// can refuse the watch.
#[async_trait]
trait PrevRevisionApi: Send + Sync {
    /// Watch for the deletion of `key`.
    async fn watch_deletion(
        &self,
        key: &TableLockIdentV2,
    ) -> std::result::Result<WatchStream, MetaError>;

    async fn get_lock(
        &self,
        key: &TableLockIdentV2,
    ) -> std::result::Result<Option<SeqV<LockMeta>>, MetaError>;
}

#[async_trait]
impl PrevRevisionApi for MetaStore {
    async fn watch_deletion(
        &self,
        key: &TableLockIdentV2,
    ) -> std::result::Result<WatchStream, MetaError> {
        let req = WatchRequest::new(key.to_string_key(), None).with_filter(FilterType::Delete);
        self.watch(req).await
    }

    async fn get_lock(
        &self,
        key: &TableLockIdentV2,
    ) -> std::result::Result<Option<SeqV<LockMeta>>, MetaError> {
        self.get_pb(key).await
    }
}

struct TableLockLeaseOps {
    catalog: Arc<dyn Catalog>,
    extend_req: ExtendLockRevReq,
    description: String,
}

#[async_trait]
impl LeaseOps for TableLockLeaseOps {
    async fn extend(&self) -> Result<()> {
        self.catalog
            .extend_lock_revision(self.extend_req.clone())
            .await
    }

    async fn delete(&self) -> Result<()> {
        self.catalog
            .delete_lock_revision(DeleteLockRevReq::new(
                self.extend_req.lock_key.clone(),
                self.extend_req.revision,
            ))
            .await
    }

    fn description(&self) -> &str {
        &self.description
    }
}

#[derive(Default)]
pub struct LockHolder {
    keeper: Arc<LeaseKeeper>,
}

impl Drop for LockHolder {
    fn drop(&mut self) {
        // The keeper's task outlives this holder. Stop it if acquisition is cancelled before
        // CoordinationManager takes ownership of the holder.
        self.shutdown();
    }
}

impl LockHolder {
    #[async_backtrace::framed]
    pub(crate) async fn try_acquire_lock(
        self: &Arc<Self>,
        catalog: Arc<dyn Catalog>,
        req: CreateLockRevReq,
        should_retry: bool,
        acquire_timeout: Duration,
    ) -> Result<u64> {
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        self.acquire(
            catalog,
            req,
            should_retry,
            acquire_timeout,
            meta_api.as_ref(),
        )
        .await
    }

    /// [`Self::try_acquire_lock`], with `prev_api` for the reads on the revision before this one.
    async fn acquire(
        self: &Arc<Self>,
        catalog: Arc<dyn Catalog>,
        req: CreateLockRevReq,
        should_retry: bool,
        acquire_timeout: Duration,
        prev_api: &dyn PrevRevisionApi,
    ) -> Result<u64> {
        let start = Instant::now();

        let ttl = req.ttl;

        let lock_key = req.lock_key.clone();
        let lock_type = lock_key.lock_type().to_string();
        let table_id = lock_key.get_table_id();

        let revision = self.start(catalog.clone(), req).await?;

        let list_table_lock_req = ListLockRevReq::new(lock_key.clone());

        loop {
            // List all revisions and check if the current is the minimum.
            let mut rev_list = catalog
                .list_lock_revisions(list_table_lock_req.clone())
                .await?
                .into_iter()
                .collect::<Vec<_>>();
            // list_lock_revisions are returned in big-endian order,
            // we need to sort them in ascending numeric order.
            rev_list.sort_by_key(|(revision, _)| *revision);
            let position = rev_list
                .iter()
                .position(|(rev, _)| *rev == revision)
                .ok_or_else(||
                // If the current is not found in list,  it means that the current has been expired.
                ErrorCode::LeaseExpired(format!(
                    "The acquired table lock lease with revision '{}' may have expired (elapsed: {:?})",
                    revision,
                    start.elapsed(),
                )))?;

            if position == 0 {
                // The lock is acquired by current session.
                let extend_table_lock_req =
                    ExtendLockRevReq::new(lock_key.clone(), revision, ttl, true);

                catalog.extend_lock_revision(extend_table_lock_req).await?;
                // metrics.
                record_acquired_lock_nums(lock_type.clone(), 1);
                break;
            }

            let elapsed = start.elapsed();
            // if no need retry, return error directly.
            if !should_retry || elapsed >= acquire_timeout {
                let (holder_revision, holder_lock_meta) = &rev_list[0];
                return Err(ErrorCode::TableAlreadyLocked(format!(
                    "Table is locked by query '{}' (rev: {}, holder_rev: {}, elapsed: {:?})",
                    holder_lock_meta.query_id, revision, holder_revision, elapsed
                )));
            }

            let prev_revision = rev_list[position - 1].0;
            let watch_delete_ident = lock_key.gen_v2_key(prev_revision);

            // Get the previous revision, watch the delete event.
            let mut watch_stream = match prev_api.watch_deletion(&watch_delete_ident).await {
                Ok(watch_stream) => watch_stream,
                Err(error) => {
                    // A meta node refuses a watch while it cannot serve one, such as during a
                    // leader election, and the client gives up once no node it knows serves it.
                    // Listing the revisions again finds the lock if it was released meanwhile.
                    let remaining = acquire_timeout.saturating_sub(start.elapsed());
                    let delay = WATCH_RETRY_DELAY.min(remaining);
                    log::warn!(
                        "Failed to watch the previous table lock revision (rev: {}, prev: {}): {}; listing the revisions again in {:?}",
                        revision,
                        prev_revision,
                        error,
                        delay
                    );
                    tokio::time::sleep(delay).await;
                    continue;
                }
            };

            let Some(lock_meta) = prev_api
                .get_lock(&watch_delete_ident)
                .await
                .map_err(meta_service_error)?
            else {
                log::warn!(
                    "Lock revision '{}' already does not exist, skipping",
                    prev_revision
                );
                continue;
            };

            // Add a timeout period for watch.
            if let Err(_cause) = timeout(acquire_timeout.abs_diff(elapsed), async move {
                while let Some(Ok(resp)) = watch_stream.next().await {
                    if let Some(event) = resp.event
                        && event.current.is_none()
                    {
                        break;
                    }
                }
            })
            .await
            {
                return Err(ErrorCode::TableAlreadyLocked(format!(
                    "Table is locked, timeout while waiting on previous lock held by query '{}' (rev: {}, prev: {}, elapsed: {:?})",
                    lock_meta.query_id,
                    revision,
                    prev_revision,
                    start.elapsed()
                )));
            }
        }

        log::info!(
            "Acquired table lock successfully(table_id: {}, lock_type: {}, revision: {}, elapsed: {:?})",
            table_id,
            lock_type,
            revision,
            start.elapsed()
        );
        Ok(revision)
    }

    #[async_backtrace::framed]
    async fn start(
        self: &Arc<Self>,
        catalog: Arc<dyn Catalog>,
        req: CreateLockRevReq,
    ) -> Result<u64> {
        let lock_key = req.lock_key.clone();
        let query_id = req.query_id.clone();
        let ttl = req.ttl;

        // A queued lock revision is itself a lease, so start keeping it alive before waiting for
        // ownership. Otherwise a long queue wait could expire this revision before acquisition.
        let res = catalog.create_lock_revision(req).await?;
        let revision = res.revision;
        record_created_lock_nums(lock_key.lock_type().to_string(), 1);
        log::debug!("create table lock success, revision={}", revision);

        let ops = TableLockLeaseOps {
            catalog,
            extend_req: ExtendLockRevReq::new(lock_key.clone(), revision, ttl, false),
            description: format!(
                "table lock for table {} revision {}",
                lock_key.get_table_id(),
                revision
            ),
        };
        self.keeper.start(ttl, ops, move |error| {
            if let Some(session) = SessionManager::instance().get_session_by_id(&query_id) {
                session.force_kill_query(error);
            }
            // The keeper does not delete after renewal failure. Retain the lease until TTL expiry
            // while query cancellation propagates.
        });

        Ok(revision)
    }

    pub fn shutdown(&self) {
        self.keeper.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use async_trait::async_trait;
    use databend_common_meta_app::schema::CreateLockRevReq;
    use databend_common_meta_app::schema::LockKey;
    use databend_common_meta_app::schema::LockMeta;
    use databend_common_meta_app::schema::TableLockIdentV2;
    use databend_common_meta_store::MetaStore;
    use databend_common_meta_store::WatchStream;
    use databend_meta_client::types::MetaError;
    use databend_meta_client::types::MetaNetworkError;
    use databend_meta_client::types::SeqV;
    use databend_meta_runtime::DatabendRuntime;

    use super::LockHolder;
    use super::PrevRevisionApi;
    use crate::sessions::TableContextTableAccess;
    use crate::test_kits::ConfigBuilder;
    use crate::test_kits::TestFixture;

    /// Refuses the first `refusals` watches, as a meta node that cannot serve a watch does, and
    /// passes everything else on to `meta`.
    struct RefusingWatches {
        meta: MetaStore,
        refusals: usize,
        watches: AtomicUsize,
    }

    #[async_trait]
    impl PrevRevisionApi for RefusingWatches {
        async fn watch_deletion(
            &self,
            key: &TableLockIdentV2,
        ) -> std::result::Result<WatchStream, MetaError> {
            if self.watches.fetch_add(1, Ordering::SeqCst) < self.refusals {
                let refused = tonic::Status::unavailable("this node serves no watch");
                return Err(MetaNetworkError::from(refused).into());
            }
            self.meta.watch_deletion(key).await
        }

        async fn get_lock(
            &self,
            key: &TableLockIdentV2,
        ) -> std::result::Result<Option<SeqV<LockMeta>>, MetaError> {
            self.meta.get_lock(key).await
        }
    }

    /// A waiter whose watch is refused lists the revisions again, and acquires the lock once its
    /// holder releases it, instead of failing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_refused_watch_keeps_waiting_for_the_lock() -> anyhow::Result<()> {
        // The catalog and the waiter's reads must use the same Meta; the default fixture gives
        // the catalog an embedded Meta of its own.
        let meta = MetaStore::new_local_testing::<DatabendRuntime>().await;
        let mut config = ConfigBuilder::create().build();
        config.meta.endpoints = meta.inner().get_cached_endpoints().await?;
        config.meta.auto_sync_interval = 0;
        let fixture = TestFixture::setup_with_config(&config).await?;
        fixture.create_default_database().await?;
        fixture.create_default_table().await?;
        let ctx = fixture.new_query_ctx().await?;
        let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;
        let lock_key = LockKey::Table {
            tenant: fixture.default_tenant(),
            table_id: fixture.latest_default_table().await?.get_id(),
        };
        let acquire_timeout = Duration::from_secs(30);
        let req = |query_id: &str| {
            let ttl = Duration::from_secs(30);
            CreateLockRevReq::new(
                lock_key.clone(),
                "root".into(),
                "node".into(),
                query_id.into(),
                ttl,
            )
        };

        let holder = Arc::new(LockHolder::default());
        holder
            .try_acquire_lock(catalog.clone(), req("holder"), true, acquire_timeout)
            .await?;

        let prev_api = RefusingWatches {
            meta,
            refusals: 2,
            watches: AtomicUsize::new(0),
        };
        let waiter = Arc::new(LockHolder::default());
        let release_after_refusals = async {
            // Release the lock once the waiter watches again after its refusals: the third watch
            // is served. A waiter that stopped at a refusal never watches again, so stop waiting
            // after a while, and let it fail the test instead of hanging it.
            let third_watch = async {
                while prev_api.watches.load(Ordering::SeqCst) <= prev_api.refusals {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            };
            let _ = tokio::time::timeout(Duration::from_secs(10), third_watch).await;
            holder.shutdown();
        };
        let (acquired, _) = tokio::join!(
            waiter.acquire(catalog, req("waiter"), true, acquire_timeout, &prev_api),
            release_after_refusals
        );
        waiter.shutdown();

        acquired?;
        assert_eq!(prev_api.watches.load(Ordering::SeqCst), 3);
        Ok(())
    }
}
