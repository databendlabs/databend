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
use std::time::Instant as StdInstant;

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::LimitMemGuard;
use databend_common_base::runtime::catch_unwind;
use databend_common_catalog::session_type::SessionType;
use databend_common_config::QueryConfig;
use databend_common_exception::Result;
use databend_common_metrics::table_maintenance::record_rejected;
use parking_lot::Mutex;
use tokio::time::Instant;

use super::resources::MaintenancePolicy;
use super::resources::MaintenanceReservation;
use super::resources::MaintenanceResources;
use super::scheduler::SchedulerCore;
use super::task::MaintenanceTask;
use super::task::WarehouseIdleProbes;

/// Node-local owner of automatic maintenance. Remote fragments share its resources,
/// but do not enter the local notification queue. Business progress stays in tasks.
pub(crate) struct TableMaintenanceService {
    scheduler: Arc<SchedulerCore<MaintenanceTask>>,
    resources: Arc<MaintenanceResources>,
    foreground: Arc<ForegroundActivity>,
}

pub(super) struct ForegroundActivity {
    state: Mutex<(usize, StdInstant)>,
}

pub(crate) struct ForegroundRequest(Arc<ForegroundActivity>);

impl Drop for ForegroundRequest {
    fn drop(&mut self) {
        let mut state = self.0.state.lock();
        state.0 -= 1;
        state.1 = StdInstant::now();
    }
}

impl ForegroundActivity {
    fn enter(self: &Arc<Self>) -> ForegroundRequest {
        self.state.lock().0 += 1;
        ForegroundRequest(self.clone())
    }

    pub(super) fn is_idle(&self) -> bool {
        let (active, last_finished) = *self.state.lock();
        active == 0 && last_finished.elapsed() >= Duration::from_secs(60)
    }
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
        let runner_resources = resources.clone();
        let foreground = Arc::new(ForegroundActivity {
            state: Mutex::new((0, StdInstant::now())),
        });
        let runner_foreground = foreground.clone();
        let idle_probes = Arc::new(WarehouseIdleProbes::default());
        // Capturing the service itself would keep the scheduler alive after shutdown.
        let scheduler = SchedulerCore::create(
            concurrency,
            // Cap the number of tables without rejecting merges into existing mailboxes.
            1024,
            Arc::new(move |task: MaintenanceTask, cancellation, demand| {
                let resources = runner_resources.clone();
                let foreground = runner_foreground.clone();
                let idle_probes = idle_probes.clone();
                Box::pin(async move {
                    task.execute(cancellation, &resources, demand, &foreground, &idle_probes)
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
            foreground,
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

    pub(crate) fn foreground_is_idle() -> bool {
        Self::try_instance().is_some_and(|service| service.foreground.is_idle())
    }

    /// A user context lives through planning, queueing and execution. An RAII
    /// count also covers cancellations and errors before the interpreter starts.
    pub(crate) fn track_foreground(session_type: &SessionType) -> Option<ForegroundRequest> {
        if !session_type.is_user_session() || matches!(session_type, SessionType::FlightRPC) {
            return None;
        }
        Self::try_instance().map(|service| service.foreground.enter())
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
    fn close(&self) {
        self.scheduler.close();
        self.resources.close();
    }

    pub(crate) async fn shutdown(&self, deadline: Instant) -> MaintenanceShutdown {
        self.close();
        match tokio::time::timeout_at(deadline, self.wait_for_shutdown()).await {
            Ok(()) => MaintenanceShutdown::Drained,
            Err(_) => MaintenanceShutdown::TimedOut,
        }
    }

    /// Wait for logical units and reservation owners, not detached pipeline workers.
    async fn wait_for_shutdown(&self) {
        tokio::join!(self.scheduler.wait_for_shutdown(), self.resources.drain());
    }

    #[cfg(debug_assertions)]
    pub(crate) async fn shutdown_for_test(&self) {
        self.close();
        self.wait_for_shutdown().await;
    }

    // Direct unit tests still need resource admission after stopping dispatch.
    pub(crate) async fn stop_scheduler_for_test(&self) {
        self.scheduler.close();
        self.scheduler.wait_for_shutdown().await;
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
