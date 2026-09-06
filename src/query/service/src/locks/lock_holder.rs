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
use databend_common_metrics::lock::record_acquired_lock_nums;
use databend_common_metrics::lock::record_created_lock_nums;
use databend_common_users::UserApiProvider;
use databend_meta_client::kvapi::StructKey;
use databend_meta_client::types::protobuf::WatchRequest;
use databend_meta_client::types::protobuf::watch_request::FilterType;
use futures_util::StreamExt;
use tokio::time::timeout;

use crate::locks::lease_keeper::LeaseKeeper;
use crate::locks::lease_keeper::LeaseOps;
use crate::meta_service_error;
use crate::sessions::SessionManager;

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

impl LockHolder {
    #[async_backtrace::framed]
    pub(crate) async fn try_acquire_lock(
        self: &Arc<Self>,
        catalog: Arc<dyn Catalog>,
        req: CreateLockRevReq,
        should_retry: bool,
        acquire_timeout: Duration,
    ) -> Result<u64> {
        let start = Instant::now();

        let ttl = req.ttl;

        let lock_key = req.lock_key.clone();
        let lock_type = lock_key.lock_type().to_string();
        let table_id = lock_key.get_table_id();

        let revision = self.start(catalog.clone(), req).await?;

        let meta_api = UserApiProvider::instance().get_meta_store_client();
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
            let req = WatchRequest::new(watch_delete_ident.to_string_key(), None)
                .with_filter(FilterType::Delete);
            let mut watch_stream = meta_api.watch(req).await.map_err(meta_service_error)?;

            let Some(lock_meta) = meta_api
                .get_pb(&watch_delete_ident)
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
