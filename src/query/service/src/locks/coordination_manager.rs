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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::lock::Lock;
use databend_common_catalog::table_context::TableContextAuthorization;
use databend_common_catalog::table_context::TableContextCluster;
use databend_common_catalog::table_context::TableContextQueryIdentity;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_catalog::table_context::TableContextTableAccess;
use databend_common_exception::Result;
use databend_common_meta_api::SegmentClaimApi;
use databend_common_meta_app::schema::CreateLockRevReq;
use databend_common_meta_app::schema::CreateSegmentClaimReq;
use databend_common_meta_app::schema::ExtendSegmentClaimReq;
use databend_common_meta_app::schema::ListSegmentClaimsReq;
use databend_common_meta_app::schema::LockKey;
use databend_common_meta_app::schema::TableInfo;
use databend_common_metrics::lock::metrics_inc_shutdown_lock_holder_nums;
use databend_common_metrics::lock::metrics_inc_start_lock_holder_nums;
use databend_common_metrics::storage::metrics_dec_maintenance_active_tasks;
use databend_common_metrics::storage::metrics_inc_maintenance_active_tasks;
use databend_common_pipeline::core::LockGuard;
use databend_common_pipeline::core::UnlockApi;
use databend_common_users::UserApiProvider;
use parking_lot::RwLock;
use tokio::sync::mpsc;

use crate::locks::lock_holder::LockHolder;
use crate::locks::segment_claim::SegmentClaimHolder;
use crate::locks::table_lock::TableLock;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

struct ChannelUnlocker {
    tx: mpsc::UnboundedSender<u64>,
}

impl UnlockApi for ChannelUnlocker {
    fn unlock(&self, id: u64) {
        let _ = self.tx.send(id);
    }
}

/// Owns the query node's active coordination holders.
///
/// Table locks and segment claims intentionally retain separate Meta protocols,
/// identifier namespaces, holder implementations, and release paths.
pub struct CoordinationManager {
    active_locks: Arc<RwLock<HashMap<u64, Arc<LockHolder>>>>,
    active_segment_claims: Arc<RwLock<HashMap<u64, Arc<SegmentClaimHolder>>>>,
    table_lock_unlocker: Arc<dyn UnlockApi>,
    segment_claim_unlocker: Arc<dyn UnlockApi>,
}

impl CoordinationManager {
    pub fn init() -> Result<()> {
        let (table_lock_tx, mut table_lock_rx) = mpsc::unbounded_channel();
        let (claim_tx, mut claim_rx) = mpsc::unbounded_channel();
        let active_locks = Arc::new(RwLock::new(HashMap::new()));
        let active_segment_claims = Arc::new(RwLock::new(HashMap::new()));

        let manager = Arc::new(Self {
            active_locks: active_locks.clone(),
            active_segment_claims: active_segment_claims.clone(),
            table_lock_unlocker: Arc::new(ChannelUnlocker { tx: table_lock_tx }),
            segment_claim_unlocker: Arc::new(ChannelUnlocker { tx: claim_tx }),
        });

        GlobalIORuntime::instance().spawn(async move {
            while let Some(revision) = table_lock_rx.recv().await {
                if let Some(holder) = active_locks.write().remove(&revision) {
                    metrics_inc_shutdown_lock_holder_nums();
                    holder.shutdown();
                }
            }
        });
        GlobalIORuntime::instance().spawn(async move {
            while let Some(claim_id) = claim_rx.recv().await {
                if let Some(holder) = active_segment_claims.write().remove(&claim_id) {
                    metrics_inc_shutdown_lock_holder_nums();
                    metrics_dec_maintenance_active_tasks();
                    holder.shutdown();
                }
            }
        });

        GlobalInstance::set(manager);
        Ok(())
    }

    pub fn instance() -> Arc<Self> {
        GlobalInstance::get()
    }

    pub fn create_table_lock(table_info: TableInfo) -> Arc<dyn Lock> {
        TableLock::create(Self::instance(), table_info)
    }

    pub async fn claimed_segments(
        &self,
        ctx: &dyn TableContext,
        table_id: u64,
    ) -> Result<HashSet<String>> {
        let claims = UserApiProvider::instance()
            .get_meta_store_client()
            .list_segment_claims(ListSegmentClaimsReq {
                tenant: ctx.get_tenant(),
                table_id,
            })
            .await?;
        Ok(claims
            .into_iter()
            .flat_map(|(_, meta)| meta.segment_locations)
            .collect())
    }

    pub async fn try_segment_claim(
        &self,
        ctx: Arc<QueryContext>,
        table_id: u64,
        segment_locations: Vec<String>,
    ) -> Result<Option<Arc<LockGuard>>> {
        let tenant = ctx.get_tenant();
        // A zero TTL cannot be renewed safely and would make the random renewal range empty.
        let ttl = Duration::from_secs(ctx.get_settings().get_table_lock_expire_secs()?.max(3));
        let reply = UserApiProvider::instance()
            .get_meta_store_client()
            .create_segment_claim(CreateSegmentClaimReq {
                tenant: tenant.clone(),
                table_id,
                ttl,
                user: ctx.get_current_user()?.name,
                node: ctx.get_cluster().local_id.clone(),
                query_id: ctx.get_id(),
                segment_locations,
            })
            .await?;
        let Some(claim_id) = reply.claim_id else {
            return Ok(None);
        };

        let holder = Arc::new(SegmentClaimHolder::default());
        holder.start(
            ExtendSegmentClaimReq {
                tenant,
                table_id,
                claim_id,
                ttl,
            },
            ctx,
        );
        let previous = self.active_segment_claims.write().insert(claim_id, holder);
        assert!(previous.is_none());
        metrics_inc_start_lock_holder_nums();
        metrics_inc_maintenance_active_tasks();

        Ok(Some(Arc::new(LockGuard::new(
            self.segment_claim_unlocker.clone(),
            claim_id,
        ))))
    }

    /// The requested lock returns a global incremental revision, listing all existing revisions,
    /// and if the current revision is the smallest, the lock is acquired successfully.
    /// Otherwise, listen to the deletion event of the previous revision in a loop until get lock success.
    ///
    /// NOTICE: the lock holder is not 100% reliable.
    /// E.g., there is a very small probability of failure in extending or deleting the lock.
    #[async_backtrace::framed]
    pub async fn try_table_lock(
        self: &Arc<Self>,
        ctx: Arc<dyn TableContext>,
        lock_key: LockKey,
        catalog_name: &str,
        should_retry: bool,
    ) -> Result<Option<Arc<LockGuard>>> {
        let acquire_timeout = Duration::from_secs(ctx.get_settings().get_acquire_lock_timeout()?);
        let ttl = Duration::from_secs(ctx.get_settings().get_table_lock_expire_secs()?);
        let req = CreateLockRevReq::new(
            lock_key,
            ctx.get_current_user()?.name,
            ctx.get_cluster().local_id.clone(),
            ctx.get_id(),
            ttl,
        );
        let catalog = ctx.get_catalog(catalog_name).await?;
        let holder = Arc::new(LockHolder::default());

        match holder
            .try_acquire_lock(catalog, req, should_retry, acquire_timeout)
            .await
        {
            Ok(revision) => {
                let previous = self.active_locks.write().insert(revision, holder);
                assert!(previous.is_none());
                metrics_inc_start_lock_holder_nums();
                Ok(Some(Arc::new(LockGuard::new(
                    self.table_lock_unlocker.clone(),
                    revision,
                ))))
            }
            Err(error) => {
                holder.shutdown();
                Err(error)
            }
        }
    }
}
