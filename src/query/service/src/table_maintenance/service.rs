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

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::LimitMemGuard;
use databend_common_base::runtime::catch_unwind;
use databend_common_config::QueryConfig;
use databend_common_exception::Result;
use databend_common_metrics::table_maintenance::record_rejected;
use tokio::time::Instant;

use super::policy::MaintenancePolicy;
use super::resources::MaintenanceReservation;
use super::resources::MaintenanceResources;
use super::scheduler::SchedulerCore;
use super::task::MaintenanceTask;
use crate::interpreters::hook::refresh_hook::RefreshCoverage;

/// Node-local owner of automatic maintenance. Remote fragments share its resources,
/// but do not enter the local notification queue. Business progress stays in tasks.
pub(crate) struct TableMaintenanceService {
    scheduler: Arc<SchedulerCore<MaintenanceTask>>,
    resources: Arc<MaintenanceResources>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MaintenanceShutdown {
    Drained,
    TimedOut,
}

impl TableMaintenanceService {
    pub(crate) fn init(config: &QueryConfig) -> Result<()> {
        // Detect capacity once, not in a maintenance-specific monitoring loop. The
        // allocator's global tracker supplies live usage and any subsequently enforced
        // node limit on each admission (the binary installs that limit after init).
        let mut system = sysinfo::System::new();
        system.refresh_memory();
        let capacity = [
            system.total_memory(),
            system
                .cgroup_limits()
                .map_or(0, |limits| limits.total_memory),
            config.common.max_server_memory_usage,
        ]
        .into_iter()
        .filter(|limit| *limit != 0)
        .min()
        .unwrap_or(0)
        .min(i64::MAX as u64);
        let policy = MaintenancePolicy::create(config, capacity);
        let concurrency = policy.max_concurrency;
        let resources = MaintenanceResources::create(policy);
        let config = &config.common;
        let coverage = RefreshCoverage::create(config.table_maintenance_max_pending_tables);
        let runner_resources = resources.clone();
        // Capturing the service itself would keep the scheduler alive after shutdown.
        let scheduler = SchedulerCore::create(
            concurrency,
            config.table_maintenance_max_pending_tables,
            Arc::new(move |task: MaintenanceTask, cancellation, demand| {
                let resources = runner_resources.clone();
                let coverage = coverage.clone();
                Box::pin(async move {
                    task.execute(cancellation, &resources, &coverage, demand)
                        .await
                })
            }),
            Arc::new(|future, name| {
                GlobalIORuntime::instance().spawn_named(future, name);
            }),
        );
        // If registration or allocation unwinds after the dispatcher starts, close it
        // before losing its owner. Its unpolled dispatch guard handles rejected spawns.
        let mut startup = StartupGuard {
            scheduler: Some(scheduler.clone()),
            resources: resources.clone(),
        };
        GlobalInstance::set(Arc::new(Self {
            scheduler,
            resources,
        }));
        startup.scheduler = None;
        Ok(())
    }

    pub(crate) fn instance() -> Arc<Self> {
        GlobalInstance::get()
    }

    pub(crate) fn try_instance() -> Option<Arc<Self>> {
        GlobalInstance::try_get()
    }

    #[cfg(test)]
    pub(crate) fn resources(&self) -> &Arc<MaintenanceResources> {
        &self.resources
    }

    #[cfg(test)]
    pub(crate) fn reserve(&self) -> Result<Arc<MaintenanceReservation>> {
        self.resources.reserve()
    }

    pub(crate) fn reserve_remote(
        &self,
        settings: &databend_common_settings::Settings,
    ) -> Result<Arc<MaintenanceReservation>> {
        self.resources.reserve_remote(settings)
    }

    /// Never wait for capacity or propagate maintenance failure to a committed write.
    pub(crate) fn enqueue(&self, task: MaintenanceTask) {
        let table_id = task.target.table_id;
        let action = task.kind().name();
        let result = catch_unwind(|| {
            if let Err(rejected) = self.scheduler.try_enqueue(task) {
                record_rejected(rejected.reason);
                log::debug!(table_id, action, reason = rejected.reason;
                    "Discard best-effort maintenance notification");
                drop(rejected.task);
            }
        });
        if let Err(error) = result {
            let _guard = LimitMemGuard::enter_unlimited();
            record_rejected("enqueue_panic");
            log::warn!(table_id, action, error :? = error;
                "Best-effort maintenance admission panicked");
        }
    }

    /// After return, enqueue, reservation and late context registration are closed.
    /// Cancellation does not imply executor termination or a known publication result.
    pub(crate) fn close(&self) {
        self.scheduler.close();
        self.resources.close();
    }

    pub(crate) async fn shutdown(&self, deadline: Instant) -> MaintenanceShutdown {
        self.close();
        match tokio::time::timeout_at(deadline, self.drain()).await {
            Ok(()) => MaintenanceShutdown::Drained,
            Err(_) => MaintenanceShutdown::TimedOut,
        }
    }

    /// Drain logical units and reservation owners, not detached pipeline workers.
    pub(crate) async fn drain(&self) {
        self.close();
        tokio::join!(self.scheduler.shutdown(), self.resources.drain());
    }

    // Direct unit tests still need resource admission after stopping dispatch.
    pub(crate) async fn stop_scheduler_for_test(&self) {
        self.scheduler.shutdown().await;
    }
}

struct StartupGuard {
    scheduler: Option<Arc<SchedulerCore<MaintenanceTask>>>,
    resources: Arc<MaintenanceResources>,
}

impl Drop for StartupGuard {
    fn drop(&mut self) {
        if let Some(scheduler) = self.scheduler.take() {
            scheduler.close();
            self.resources.close();
        }
    }
}
