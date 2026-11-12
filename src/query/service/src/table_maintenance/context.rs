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
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use databend_common_base::runtime::IoStats;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ParentMemStat;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayload;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_base::runtime::drop_guard;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_version::BUILD_INFO;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::sessions::QueryContext;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextPartitionStats;
use crate::sessions::TableContextQueryIdentity;
use crate::sessions::TableContextQueryInfo;
use crate::table_maintenance::MaintenanceIdentity;
use crate::table_maintenance::MaintenanceResources;

/// Where one maintenance unit runs: an internal session derived from the triggering
/// session, a query context of its own, and tracking (query id, memory, IO) of its own.
/// Work run under [`MaintenanceContext::tracking`] is never accounted to the query that
/// triggered it.
pub(crate) struct MaintenanceContext {
    pub(crate) ctx: Arc<QueryContext>,
    pub(crate) tracking: Arc<TrackingPayload>,
}

impl MaintenanceContext {
    /// Resolve a fresh execution environment only when a queued action actually starts.
    pub(crate) async fn from_identity(identity: &MaintenanceIdentity) -> Result<Self> {
        let reservation = MaintenanceResources::reserve()?;
        let query_id = Uuid::new_v4().to_string();
        let mut payload = maintenance_tracking(query_id.clone());
        payload.mem_stat = Some(MemStat::create_child(
            Some(query_id.clone()),
            0,
            ParentMemStat::Normal(reservation.memory()),
        ));
        // Do not inherit an arbitrary worker's foreground workload resource.
        payload.workload_group_resource = None;
        let tracking = Arc::new(payload);
        let ctx = tracking
            .clone()
            .tracking(async {
                let session = identity.create_session().await?;
                reservation.constrain(&session.get_settings())?;
                session.create_query_context(&BUILD_INFO).await
            })
            .await?;
        ctx.update_init_query_id(query_id);
        ctx.set_maintenance_reservation(reservation)?;
        // Until maintenance has its own disk/remote-spill budget, use a zero-spill
        // envelope. Oversized rounds fail/defer instead of consuming foreground disk.
        ctx.set_enable_sort_spill(false);
        Ok(Self { ctx, tracking })
    }

    pub(crate) async fn create(parent: &Arc<QueryContext>) -> Result<Self> {
        let query_id = Uuid::new_v4().to_string();
        let tracking = Arc::new(maintenance_tracking(query_id.clone()));
        let session = parent
            .get_current_session()
            .create_internal_session()
            .await?;
        // A new context takes the memory tracking and workload group of the current scope,
        // so it must be created under the maintenance tracking, not the triggering query's.
        let ctx = {
            let _guard = ThreadTracker::tracking_inner(tracking.clone());
            session.create_query_context_with_cluster(
                parent.get_cluster(),
                parent.get_version(),
                None,
            )?
        };
        ctx.update_init_query_id(query_id);
        Ok(Self { ctx, tracking })
    }
}

struct MaintenanceUnitGuard<'a> {
    ctx: &'a Arc<QueryContext>,
    cause: Option<ErrorCode>,
}

impl Drop for MaintenanceUnitGuard<'_> {
    fn drop(&mut self) {
        if let Some(cause) = self.cause.take() {
            drop_guard(|| {
                self.ctx.kill(cause.clone());
                // Planning can initialize remote environments before installing an executor.
                // This is idempotent with the pipelines' normal finished callbacks.
                self.ctx
                    .get_exchange_manager()
                    .on_finished_query(&self.ctx.get_id(), Some(cause));
            });
        }
    }
}

/// Interrupt planning as well as execution, but give executors a chance to run their
/// finished callbacks (including distributed exchange/claim cleanup) before dropping work.
/// A hung unit cannot monopolize a mailbox indefinitely, even without further notifications.
pub(crate) async fn run_maintenance_unit<T>(
    ctx: &Arc<QueryContext>,
    cancellation: CancellationToken,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    run_maintenance_unit_with_timeout(ctx, cancellation, Duration::from_secs(5 * 60), work).await
}

async fn run_maintenance_unit_with_timeout<T>(
    ctx: &Arc<QueryContext>,
    cancellation: CancellationToken,
    unit_timeout: Duration,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    let mut guard = MaintenanceUnitGuard {
        ctx,
        cause: Some(ErrorCode::AbortedQuery("Maintenance unit interrupted")),
    };
    if cancellation.is_cancelled() {
        return Err(ErrorCode::AbortedQuery(
            "Node is stopping table maintenance",
        ));
    }
    tokio::pin!(work);
    let cause = tokio::select! {
        biased;
        _ = cancellation.cancelled() => ErrorCode::AbortedQuery("Node is stopping table maintenance"),
        result = tokio::time::timeout(unit_timeout, &mut work) => {
            match result {
                Ok(result) => {
                    guard.cause = result.as_ref().err().cloned();
                    return result;
                }
                Err(_) => ErrorCode::AbortedQuery("Table maintenance unit exceeded its time limit"),
            }
        }
    };
    ctx.kill(cause.clone());
    if tokio::time::timeout(Duration::from_secs(3), &mut work)
        .await
        .is_err()
    {
        log::warn!(query_id = ctx.get_id().as_str();
            "Maintenance cancellation cleanup exceeded its grace period");
    }
    guard.cause = Some(cause.clone());
    Err(cause)
}

/// Derive the tracking of a maintenance unit from the current scope. Process-level fields
/// such as the warehouse and workload group are kept; everything scoped to the current
/// query is replaced, so memory, IO and profiles start empty.
fn maintenance_tracking(query_id: String) -> TrackingPayload {
    let mut payload = ThreadTracker::new_tracking_payload();
    payload.mem_stat = Some(MemStat::create(query_id.clone()));
    payload.io_stats = Some(Arc::new(IoStats::default()));
    payload.query_id = Some(query_id);
    payload.profile = None;
    payload.metrics = None;
    payload.time_series_profile = None;
    payload.local_time_series_profile = None;
    payload.processor_interrupt = None;
    payload.perf_enabled = false;
    payload.perf_query_token = 0;
    payload.process_rows = AtomicUsize::new(0);
    payload
}

#[cfg(test)]
mod tests {
    use tokio::sync::Notify;

    use super::*;
    use crate::sessions::TableContextQueryState;
    use crate::test_kits::TestFixture;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_cancellation_interrupts_work_and_waits_for_cleanup() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let parent = fixture.new_query_ctx().await?;
        let identity = MaintenanceIdentity::capture(&parent).await?;
        let maintenance = MaintenanceContext::from_identity(&identity).await?;
        let cancellation = CancellationToken::new();
        let started = Notify::new();
        let mut cleaned_up = false;
        let work = async {
            started.notify_one();
            loop {
                if maintenance.ctx.check_aborting().is_err() {
                    cleaned_up = true;
                    return Err::<(), _>(ErrorCode::AbortedQuery("test observed cancellation"));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        let cancel = async {
            started.notified().await;
            cancellation.cancel();
        };
        let (result, _) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(
                run_maintenance_unit(&maintenance.ctx, cancellation.clone(), work),
                cancel
            )
        })
        .await
        .expect("cancellation must finish the unit");
        assert!(result.is_err());
        assert!(
            cleaned_up,
            "work must observe abort before the unit is dropped"
        );
        assert!(maintenance.ctx.check_aborting().is_err());
        drop(maintenance);
        assert!(MaintenanceResources::reserve().is_ok());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_timeout_and_panic_interrupt_contexts() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let work = async {
            while ctx.check_aborting().is_ok() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err::<(), _>(ErrorCode::AbortedQuery("timeout observed"))
        };
        let result = run_maintenance_unit_with_timeout(
            &ctx,
            CancellationToken::new(),
            Duration::from_millis(10),
            work,
        )
        .await;
        assert!(result.is_err());
        assert!(ctx.check_aborting().is_err());

        let ctx = fixture.new_query_ctx().await?;
        let result = databend_common_base::runtime::CatchUnwindFuture::create(
            run_maintenance_unit::<()>(&ctx, CancellationToken::new(), async {
                panic!("injected maintenance panic before executor installation");
            }),
        )
        .await;
        assert!(result.is_err());
        assert!(
            ctx.check_aborting().is_err(),
            "unwind must cancel initialized query environments"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_closed_scheduler_never_starts_work() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let ctx = fixture.new_query_ctx().await?;
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let result = run_maintenance_unit(&ctx, cancellation, async {
            assert!(
                ctx.check_aborting().is_ok(),
                "cancelled maintenance must not be polled"
            );
            Ok(())
        })
        .await;
        assert!(result.is_err());
        Ok(())
    }
}
