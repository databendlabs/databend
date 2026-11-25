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

use databend_common_base::runtime::CatchUnwindFuture;
use databend_common_base::runtime::IoStats;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ParentMemStat;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayload;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_base::runtime::drop_guard;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_version::BUILD_INFO;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::resources::MaintenanceAdmission;
use super::resources::MaintenanceResources;
use crate::clusters::ClusterDiscovery;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextPartitionStats;
use crate::sessions::TableContextQueryIdentity;
use crate::sessions::TableContextQueryInfo;
use crate::table_maintenance::MaintenanceIdentity;
use crate::table_maintenance::MaintenanceReservation;
#[cfg(test)]
use crate::table_maintenance::TableMaintenanceService;

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
    #[cfg(test)]
    pub(crate) async fn from_identity(identity: &MaintenanceIdentity) -> Result<Self> {
        Self::from_identity_with_reservation(
            identity,
            TableMaintenanceService::instance().reserve()?,
        )
        .await
    }

    pub(crate) async fn from_identity_with_reservation(
        identity: &MaintenanceIdentity,
        reservation: Arc<MaintenanceReservation>,
    ) -> Result<Self> {
        let query_id = Uuid::new_v4().to_string();
        let tracking = Arc::new(TrackingPayload {
            query_id: Some(query_id.clone()),
            warehouse_id: Some(identity.execution_warehouse.clone()),
            mem_stat: Some(MemStat::create_child(
                Some(query_id.clone()),
                0,
                ParentMemStat::Normal(reservation.memory()),
            )),
            io_stats: Some(Arc::new(IoStats::default())),
            ..Default::default()
        });
        let ctx = tracking
            .clone()
            .tracking(async {
                let session = identity.create_session().await?;
                reservation.constrain(&session.get_settings())?;
                let cluster = ClusterDiscovery::instance()
                    .discover_for_maintenance(&GlobalConfig::instance())
                    .await?;
                identity.check_execution_warehouse(&cluster)?;
                session.create_query_context_with_cluster(cluster, &BUILD_INFO, None)
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
        let tracking = Arc::new(inline_maintenance_tracking(query_id.clone()));
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

#[derive(Debug)]
pub(super) enum Execution<T> {
    Executed(T),
    Busy,
    Closed,
    Panicked(ErrorCode),
}

/// Each unit acquires a grant, revalidates its identity and runs with fresh
/// tracking and bounded cancellation. Queued tasks own none of these resources.
pub(super) async fn execute_unit<T, F: Future<Output = Result<T>>>(
    identity: &MaintenanceIdentity,
    cancellation: CancellationToken,
    resources: &Arc<MaintenanceResources>,
    demand: usize,
    work: impl FnOnce(Arc<QueryContext>) -> F,
) -> Result<Execution<T>> {
    if cancellation.is_cancelled() {
        return Err(ErrorCode::AbortedQuery("Maintenance scheduler closed"));
    }
    let reservation = match resources.try_reserve(demand) {
        MaintenanceAdmission::Granted(reservation) => reservation,
        MaintenanceAdmission::Busy => return Ok(Execution::Busy),
        MaintenanceAdmission::Closed => return Ok(Execution::Closed),
    };
    let MaintenanceContext { ctx, tracking } = tokio::select! {
        biased;
        _ = cancellation.cancelled() => return Err(ErrorCode::AbortedQuery("Maintenance scheduler closed")),
        context = tokio::time::timeout(
            Duration::from_secs(60),
            MaintenanceContext::from_identity_with_reservation(identity, reservation),
        ) => context.map_err(|_| ErrorCode::AbortedQuery("Maintenance context creation timed out"))??,
    };
    let result = CatchUnwindFuture::create(async {
        run_maintenance_unit(&ctx, cancellation, tracking.tracking(work(ctx.clone()))).await
    })
    .await;
    match result {
        Ok(result) => result.map(Execution::Executed),
        Err(error) => Ok(Execution::Panicked(
            error.with_context("automatic maintenance execution panicked"),
        )),
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
/// This bounds cooperative cancellation, not OS-thread termination. Scheduler gates
/// cover logical units; forced cancellation does not join detached pipeline workers.
async fn run_maintenance_unit<T>(
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
        // Publication is protected only within this grace, never indefinitely. A
        // forced stop can lose a successful meta response, so callers must recheck
        // coverage rather than infer rollback or delete generated objects.
        ctx.force_maintenance_cancellation(cause.clone());
        log::warn!(query_id = ctx.get_id().as_str();
            "Maintenance cancellation cleanup exceeded its grace period");
    }
    guard.cause = Some(cause.clone());
    Err(cause)
}

/// Inline maintenance retains the caller's workload group and log capture but
/// records memory, IO and profiles under its own query ID.
fn inline_maintenance_tracking(query_id: String) -> TrackingPayload {
    let caller = ThreadTracker::new_tracking_payload();
    TrackingPayload {
        mem_stat: Some(MemStat::create(query_id.clone())),
        io_stats: Some(Arc::new(IoStats::default())),
        query_id: Some(query_id),
        warehouse_id: caller.warehouse_id,
        workload_group_resource: caller.workload_group_resource,
        capture_log_settings: caller.capture_log_settings,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sessions::TableContext;
    use crate::sessions::TableContextQueryState;
    use crate::test_kits::TestFixture;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_publication_drains_before_cooperative_cancellation() -> Result<()> {
        let fixture = TestFixture::setup().await?;
        let parent = fixture.new_query_ctx().await?;
        let identity = MaintenanceIdentity::capture(&parent).await?;
        let maintenance = MaintenanceContext::from_identity(&identity).await?;
        let first = maintenance.ctx.begin_maintenance_publication()?.unwrap();
        let second = maintenance.ctx.begin_maintenance_publication()?.unwrap();
        maintenance
            .ctx
            .kill(ErrorCode::AbortedQuery("stop maintenance"));
        assert!(maintenance.ctx.check_aborting().is_ok());
        let error = maintenance
            .ctx
            .begin_maintenance_publication()
            .err()
            .expect("new publication must be rejected");
        assert_eq!(error.code(), ErrorCode::ABORTED_QUERY);
        drop(first);
        assert!(
            maintenance.ctx.check_aborting().is_ok(),
            "another publication is still draining"
        );
        drop(second);
        assert_eq!(
            maintenance.ctx.check_aborting().unwrap_err().code(),
            ErrorCode::ABORTED_QUERY
        );
        drop(maintenance);

        let forced = MaintenanceContext::from_identity(&identity).await?;
        let _publication = forced.ctx.begin_maintenance_publication()?.unwrap();
        forced
            .ctx
            .force_maintenance_cancellation(ErrorCode::AbortedQuery("cleanup grace expired"));
        assert_eq!(
            forced.ctx.check_aborting().unwrap_err().code(),
            ErrorCode::ABORTED_QUERY
        );
        Ok(())
    }
}
