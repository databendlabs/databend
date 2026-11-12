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

use databend_common_base::runtime::IoStats;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ParentMemStat;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayload;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_exception::Result;
use databend_common_version::BUILD_INFO;
use uuid::Uuid;

use crate::sessions::QueryContext;
use crate::sessions::TableContextCluster;
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
        ctx.set_maintenance_reservation(reservation);
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
