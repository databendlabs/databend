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
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use backoff::backoff::Backoff;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::table_context::TableContextAuthorization;
use databend_common_catalog::table_context::TableContextCluster;
use databend_common_catalog::table_context::TableContextQueryIdentity;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_catalog::table_context::TableContextTableAccess;
use databend_common_exception::Result;
use databend_common_meta_api::SegmentClaimApi;
use databend_common_meta_api::kv_app_error::KVAppError;
use databend_common_meta_app::schema::CreateSegmentClaimReq;
use databend_common_meta_app::schema::DeleteSegmentClaimReq;
use databend_common_meta_app::schema::ExtendSegmentClaimReq;
use databend_common_meta_app::schema::ListSegmentClaimsReq;
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
pub(super) struct SegmentClaimHolder {
    shutdown: AtomicBool,
    notify: Notify,
}

impl SegmentClaimHolder {
    async fn try_renew<F, Fut, E, R>(
        &self,
        max_retry_elapsed: Duration,
        mut renew: F,
        is_retryable: R,
    ) -> std::result::Result<bool, E>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<(), E>>,
        R: Fn(&E) -> bool,
    {
        let mut backoff = databend_common_storages_fuse::operations::set_backoff(
            Some(Duration::from_millis(2)),
            None,
            Some(max_retry_elapsed),
        );
        loop {
            if self.shutdown.load(Ordering::SeqCst) {
                return Ok(false);
            }

            match renew().await {
                Ok(()) => return Ok(true),
                Err(error) => {
                    if !is_retryable(&error) {
                        return Err(error);
                    }
                    let Some(delay) = backoff.next_backoff() else {
                        return Err(error);
                    };
                    log::debug!(
                        "failed to renew segment claim, retrying in {} ms",
                        delay.as_millis()
                    );
                    tokio::select! {
                        _ = self.notify.notified() => return Ok(false),
                        _ = sleep(delay) => {}
                    }
                }
            }
        }
    }

    fn start(
        self: &Arc<Self>,
        extend_req: ExtendSegmentClaimReq,
        delete_req: DeleteSegmentClaimReq,
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
                        let renewed = holder
                            .try_renew(
                                ttl - delay,
                                || {
                                    let req = extend_req.clone();
                                    async move {
                                        UserApiProvider::instance()
                                            .get_meta_store_client()
                                            .extend_segment_claim(req)
                                            .await
                                    }
                                },
                                |error| matches!(error, KVAppError::MetaError(_)),
                            )
                            .await;
                        match renewed {
                            Ok(true) => {}
                            Ok(false) => break,
                            Err(error) => {
                                log::error!("failed to renew segment claim after retries: {error}");
                                ctx.kill(error.into());
                                // Do not delete a claim after renewal fails: retain exclusion until
                                // its TTL expires while query cancellation propagates.
                                return Ok::<_, databend_common_exception::ErrorCode>(());
                            }
                        }
                    }
                }
            }

            if let Err(error) = UserApiProvider::instance()
                .get_meta_store_client()
                .delete_segment_claim(delete_req)
                .await
            {
                log::warn!("failed to release segment claim: {error}");
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
        self: &Arc<Self>,
        ctx: Arc<QueryContext>,
        table_id: u64,
        segment_locations: Vec<String>,
    ) -> Result<Option<Arc<LockGuard>>> {
        let tenant = ctx.get_tenant();
        let query_id = ctx.get_id();
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
                query_id: query_id.clone(),
                segment_locations,
            })
            .await?;
        let Some(claim_id) = reply.claim_id else {
            return Ok(None);
        };

        let holder = Arc::new(SegmentClaimHolder::default());
        holder.start(
            ExtendSegmentClaimReq {
                tenant: tenant.clone(),
                table_id,
                claim_id,
                ttl,
            },
            DeleteSegmentClaimReq {
                tenant,
                table_id,
                claim_id,
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use super::SegmentClaimHolder;

    #[tokio::test]
    async fn test_renew_retries_transient_errors() {
        let holder = SegmentClaimHolder::default();
        let attempts = AtomicUsize::new(0);

        let renewed = holder
            .try_renew(
                Duration::from_secs(1),
                || {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if attempt < 2 {
                            Err("transient")
                        } else {
                            Ok(())
                        }
                    }
                },
                |_| true,
            )
            .await;

        assert_eq!(renewed, Ok(true));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_renew_stops_after_retry_deadline() {
        let holder = SegmentClaimHolder::default();
        let attempts = AtomicUsize::new(0);

        let renewed = holder
            .try_renew(
                Duration::ZERO,
                || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async { Err::<(), _>("transient") }
                },
                |_| true,
            )
            .await;

        assert_eq!(renewed, Err("transient"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_renew_does_not_retry_terminal_errors() {
        let holder = SegmentClaimHolder::default();
        let attempts = AtomicUsize::new(0);

        let renewed = holder
            .try_renew(
                Duration::from_secs(1),
                || {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    async { Err::<(), _>("expired") }
                },
                |_| false,
            )
            .await;

        assert_eq!(renewed, Err("expired"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_renew_stops_on_shutdown() {
        let holder = Arc::new(SegmentClaimHolder::default());
        let attempts = Arc::new(AtomicUsize::new(0));
        let renew_holder = holder.clone();
        let renew_attempts = attempts.clone();
        let task = databend_common_base::runtime::spawn(async move {
            renew_holder
                .try_renew(
                    Duration::from_secs(1),
                    || {
                        renew_attempts.fetch_add(1, Ordering::SeqCst);
                        async { Err::<(), _>("transient") }
                    },
                    |_| true,
                )
                .await
        });

        while attempts.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        holder.shutdown();

        assert_eq!(task.await.unwrap(), Ok(false));
    }
}
