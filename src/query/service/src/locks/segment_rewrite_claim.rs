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

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::table_context::TableContextAuthorization;
use databend_common_catalog::table_context::TableContextCluster;
use databend_common_catalog::table_context::TableContextQueryIdentity;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_catalog::table_context::TableContextTableAccess;
use databend_common_exception::Result;
use databend_common_meta_api::SegmentRewriteClaimApi;
use databend_common_meta_app::schema::CreateSegmentRewriteClaimReq;
use databend_common_meta_app::schema::DeleteSegmentRewriteClaimReq;
use databend_common_meta_app::schema::ExtendSegmentRewriteClaimReq;
use databend_common_meta_app::schema::ListSegmentRewriteClaimsReq;
use databend_common_meta_app::schema::SegmentRewriteTarget;
use databend_common_metrics::lock::metrics_inc_start_lock_holder_nums;
use databend_common_metrics::storage::metrics_inc_maintenance_active_tasks;
use databend_common_pipeline::core::LockGuard;
use databend_common_users::UserApiProvider;
use rand::Rng;
use rand::thread_rng;
use tokio::sync::Notify;
use tokio::time::sleep;

use crate::locks::CoordinationManager;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Default)]
pub(super) struct SegmentRewriteClaimHolder {
    shutdown: AtomicBool,
    notify: Notify,
}

impl SegmentRewriteClaimHolder {
    fn start(
        self: &Arc<Self>,
        extend_req: ExtendSegmentRewriteClaimReq,
        delete_req: DeleteSegmentRewriteClaimReq,
        ctx: Arc<QueryContext>,
    ) {
        let ttl = extend_req.ttl;
        let sleep_range = (ttl / 3)..=(ttl * 2 / 3);
        let holder = self.clone();
        GlobalIORuntime::instance().spawn(async move {
            while !holder.shutdown.load(Ordering::SeqCst) {
                let delay = thread_rng().gen_range(sleep_range.clone());
                tokio::select! {
                    _ = holder.notify.notified() => break,
                    _ = sleep(delay) => {
                        if let Err(error) = UserApiProvider::instance()
                            .get_meta_store_client()
                            .extend_segment_rewrite_claim(extend_req.clone())
                            .await
                        {
                            log::error!("failed to renew segment rewrite claim: {error}");
                            ctx.kill(error.into());
                            // Do not delete a claim after renewal fails: retain exclusion until
                            // its TTL expires while query cancellation propagates.
                            return Ok::<_, databend_common_exception::ErrorCode>(());
                        }
                    }
                }
            }

            if let Err(error) = UserApiProvider::instance()
                .get_meta_store_client()
                .delete_segment_rewrite_claim(delete_req)
                .await
            {
                log::warn!("failed to release segment rewrite claim: {error}");
            }
            Ok::<_, databend_common_exception::ErrorCode>(())
        });
    }

    pub(super) fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.notify.notify_one();
    }
}

impl CoordinationManager {
    pub async fn claimed_segments(
        &self,
        ctx: &dyn TableContext,
        table_id: u64,
    ) -> Result<HashSet<SegmentRewriteTarget>> {
        let claims = UserApiProvider::instance()
            .get_meta_store_client()
            .list_segment_rewrite_claims(ListSegmentRewriteClaimsReq {
                tenant: ctx.get_tenant(),
                table_id,
            })
            .await?;
        Ok(claims
            .into_iter()
            .flat_map(|(_, segments)| segments)
            .collect())
    }

    pub async fn try_segment_rewrite_claim(
        self: &Arc<Self>,
        ctx: Arc<QueryContext>,
        table_id: u64,
        segments: Vec<SegmentRewriteTarget>,
    ) -> Result<Option<Arc<LockGuard>>> {
        let tenant = ctx.get_tenant();
        let query_id = ctx.get_id();
        // A zero TTL cannot be renewed safely and would make the random renewal range empty.
        let ttl = Duration::from_secs(ctx.get_settings().get_table_lock_expire_secs()?.max(3));
        let reply = UserApiProvider::instance()
            .get_meta_store_client()
            .create_segment_rewrite_claim(CreateSegmentRewriteClaimReq {
                tenant: tenant.clone(),
                table_id,
                ttl,
                user: ctx.get_current_user()?.name,
                node: ctx.get_cluster().local_id.clone(),
                query_id: query_id.clone(),
                segments,
            })
            .await?;
        let Some(revision) = reply.revision else {
            return Ok(None);
        };

        let holder = Arc::new(SegmentRewriteClaimHolder::default());
        holder.start(
            ExtendSegmentRewriteClaimReq {
                tenant: tenant.clone(),
                table_id,
                revision,
                ttl,
            },
            DeleteSegmentRewriteClaimReq {
                tenant,
                table_id,
                revision,
            },
            ctx,
        );
        let previous = self
            .active_segment_rewrite_claims
            .write()
            .insert(revision, holder);
        assert!(previous.is_none());
        metrics_inc_start_lock_holder_nums();
        metrics_inc_maintenance_active_tasks();

        Ok(Some(Arc::new(LockGuard::new(
            self.segment_rewrite_claim_unlocker.clone(),
            revision,
        ))))
    }
}
