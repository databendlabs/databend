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

use databend_common_base::runtime::CatchUnwindFuture;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::table_maintenance::record_stopped;
use databend_common_metrics::table_maintenance::record_unit;
use tokio_util::sync::CancellationToken;

use super::MaintenanceIdentity;
use super::MaintenanceResources;
use super::executor::Execution;
use super::executor::execute_unit;
use super::scheduler::RunResult;
use super::scheduler::SchedulerTask;
use super::scheduler::TableKey;
use super::scheduler::TaskKey;
use super::scheduler::TaskKind;
use crate::interpreters::hook::TableHookTarget;
use crate::interpreters::hook::analyze_hook;
use crate::interpreters::hook::recluster_hook::ReclusterTask;
use crate::interpreters::hook::refresh_hook;
use crate::interpreters::hook::refresh_hook::RefreshCoverage;
use crate::sessions::QueryContext;
use crate::sessions::TableContextQueryIdentity;
use crate::sessions::TableContextSettings;

/// Common envelope; each variant owns only its own operation's state.
/// No query context, execution permit or transaction survives a unit.
pub(crate) struct MaintenanceTask {
    identity: Arc<MaintenanceIdentity>,
    pub(super) target: TableHookTarget,
    data: TaskData,
    runtime: TaskRuntime,
    source_query_id: String,
}

enum TaskData {
    Refresh,
    Analyze,
    Recluster(ReclusterTask),
}

pub(crate) enum MaintenanceOutcome {
    Completed,
    Progress(Duration),
    ScanProgress,
    Deferred(Duration),
    ResourceUnavailable,
}

const MAX_TASK_FAILURES: u32 = 8;
const MAX_TASK_BLOCKED: Duration = Duration::from_secs(10 * 60);

struct TaskRuntime {
    ready_at: Instant,
    failures: u32,
    blocked_since: Option<Instant>,
}

#[derive(Debug, PartialEq, Eq)]
enum TaskDecision {
    Completed,
    Continue,
    Stopped(&'static str),
}

impl TaskRuntime {
    fn new(ready_at: Instant) -> Self {
        Self {
            ready_at,
            failures: 0,
            blocked_since: None,
        }
    }

    fn apply_outcome(&mut self, result: Result<MaintenanceOutcome>) -> TaskDecision {
        match result {
            Ok(MaintenanceOutcome::Completed) => {
                self.failures = 0;
                self.blocked_since = None;
                TaskDecision::Completed
            }
            Ok(MaintenanceOutcome::Progress(delay)) => {
                self.failures = 0;
                self.blocked_since = None;
                self.ready_at = Instant::now() + delay;
                TaskDecision::Continue
            }
            Ok(outcome) => {
                if self.blocked_too_long() {
                    return TaskDecision::Stopped("no_progress");
                }
                match outcome {
                    MaintenanceOutcome::Deferred(delay) => self.ready_at = Instant::now() + delay,
                    MaintenanceOutcome::ResourceUnavailable => {
                        // Coordinators release their grants before retrying. Jitter avoids
                        // peers repeatedly reclaiming all local capacity in lockstep.
                        self.ready_at = Instant::now()
                            + Duration::from_millis(1000 + rand::random::<u64>() % 4000)
                    }
                    _ => {}
                }
                TaskDecision::Continue
            }
            Err(error) if invalid_execution(&error) => {
                TaskDecision::Stopped("invalid_target_or_identity")
            }
            Err(_) => {
                self.failures = self.failures.saturating_add(1);
                if self.failures >= MAX_TASK_FAILURES {
                    return TaskDecision::Stopped("retry_exhausted");
                }
                if self.blocked_too_long() {
                    return TaskDecision::Stopped("no_progress");
                }
                self.ready_at = Instant::now()
                    + Duration::from_secs(1u64 << self.failures.min(8))
                    + Duration::from_millis(rand::random::<u64>() % 1000);
                TaskDecision::Continue
            }
        }
    }

    fn blocked_too_long(&mut self) -> bool {
        self.blocked_since
            .get_or_insert_with(Instant::now)
            .elapsed()
            >= MAX_TASK_BLOCKED
    }
}

/// Full-envelope remote admission is fail-fast, not an allocation OOM. Keep the
/// no-progress bound, but do not exhaust the execution-failure budget on contention.
fn resource_outcome(result: Result<MaintenanceOutcome>) -> Result<MaintenanceOutcome> {
    match result {
        Err(error) if error.code() == ErrorCode::NO_RESOURCES_AVAILABLE => {
            Ok(MaintenanceOutcome::ResourceUnavailable)
        }
        result => result,
    }
}

fn invalid_execution(error: &ErrorCode) -> bool {
    matches!(
        error.code(),
        ErrorCode::UNKNOWN_USER
            | ErrorCode::UNKNOWN_ROLE
            | ErrorCode::INVALID_ROLE
            | ErrorCode::INVALID_WAREHOUSE
            | ErrorCode::PERMISSION_DENIED
            | ErrorCode::AUTHENTICATE_FAILURE
            | ErrorCode::UNCLUSTERED_TABLE
            | ErrorCode::UNKNOWN_TABLE
            | ErrorCode::UNKNOWN_DATABASE
    )
}

impl MaintenanceTask {
    pub fn kind(&self) -> TaskKind {
        match &self.data {
            TaskData::Refresh => TaskKind::Refresh,
            TaskData::Analyze => TaskKind::Analyze,
            TaskData::Recluster(_) => TaskKind::Recluster,
        }
    }

    pub async fn from_write(
        ctx: &Arc<QueryContext>,
        target: TableHookTarget,
        analyze: bool,
        recluster_target: Option<TableHookTarget>,
    ) -> Result<Vec<Self>> {
        let now = Instant::now();
        let identity = Arc::new(MaintenanceIdentity::capture(ctx).await?);
        let source_query_id = ctx.get_id();
        let make = |data, target, ready_at| Self {
            identity: identity.clone(),
            target,
            data,
            runtime: TaskRuntime::new(ready_at),
            source_query_id: source_query_id.clone(),
        };
        let mut tasks =
            Vec::with_capacity(1 + usize::from(analyze) + usize::from(recluster_target.is_some()));
        let analyze_target = analyze.then(|| target.clone());
        tasks.push(make(TaskData::Refresh, target, now));
        if let Some(target) = analyze_target {
            tasks.push(make(TaskData::Analyze, target, now));
        }
        if let Some(target) = recluster_target {
            // Use the session's cached window; enqueue must not read meta.
            match ctx.get_settings().get_recluster_accumulation_secs() {
                Ok(seconds) => tasks.push(make(
                    TaskData::Recluster(ReclusterTask::new()),
                    target,
                    now + Duration::from_secs(seconds),
                )),
                Err(error) => {
                    // An invalid window must not suppress independent Refresh/Analyze.
                    log::warn!(table_id = target.table_id, error :? = error;
                        "Skip automatic Recluster because its accumulation window could not be read");
                }
            }
        }
        Ok(tasks)
    }

    pub async fn execute(
        mut self,
        cancellation: CancellationToken,
        resources: &Arc<MaintenanceResources>,
        coverage: &RefreshCoverage,
        demand: usize,
    ) -> RunResult<Self> {
        let started = Instant::now();
        let identity = self.identity.clone();
        let result = CatchUnwindFuture::create(execute_unit(
            &identity,
            cancellation.clone(),
            resources,
            demand,
            |ctx| self.run(ctx, coverage),
        ))
        .await;
        let result = match result {
            Ok(Ok(Execution::Closed)) => {
                record_stopped(self.kind().name(), "shutdown");
                return RunResult::Stopped;
            }
            Ok(Ok(Execution::Panicked(error))) => {
                self.reset_progress();
                Err(error)
            }
            Ok(result) => result.map(|outcome| match outcome {
                Execution::Executed(outcome) => outcome,
                Execution::Busy => MaintenanceOutcome::ResourceUnavailable,
                Execution::Closed | Execution::Panicked(_) => unreachable!(),
            }),
            Err(error) => {
                self.reset_progress();
                Err(ErrorCode::Internal(format!(
                    "Maintenance task panicked: {error}"
                )))
            }
        };
        if result
            .as_ref()
            .is_err_and(|error| error.code() == ErrorCode::NO_RESOURCES_AVAILABLE)
        {
            // Remote initialization may have consumed candidates without committing.
            // Release that state and replan on a fresh snapshot after deferral.
            self.reset_progress();
        }
        let result = resource_outcome(result);
        let action = self.kind().name();
        if cancellation.is_cancelled() {
            record_unit(action, "cancelled", started.elapsed());
            return RunResult::Stopped;
        }
        let outcome = match &result {
            Ok(MaintenanceOutcome::Completed) => "completed",
            Ok(MaintenanceOutcome::Progress(_)) => "progress",
            Ok(MaintenanceOutcome::ScanProgress) => "scan",
            Ok(MaintenanceOutcome::Deferred(_)) => "deferred",
            Ok(MaintenanceOutcome::ResourceUnavailable) => "resource_deferred",
            Err(_) => "error",
        };
        record_unit(action, outcome, started.elapsed());
        if let Err(error) = &result {
            log::warn!(table_id = self.target.table_id, action, error :? = error; "Maintenance task failed");
            if error.code() == ErrorCode::ABORTED_QUERY {
                self.reset_progress();
            }
        }
        match self.runtime.apply_outcome(result) {
            TaskDecision::Continue => RunResult::Continue(self),
            TaskDecision::Completed => RunResult::Completed,
            TaskDecision::Stopped(reason) => {
                record_stopped(action, reason);
                log::warn!(table_id = self.target.table_id, action, reason; "Stop bounded best-effort maintenance attempt");
                RunResult::Stopped
            }
        }
    }

    async fn run(
        &mut self,
        ctx: Arc<QueryContext>,
        coverage: &RefreshCoverage,
    ) -> Result<MaintenanceOutcome> {
        let grant_bytes = ctx.get_settings().get_max_query_memory_usage()?;
        log::info!(source_query_id = self.source_query_id.as_str(), query_id = ctx.get_id().as_str(),
            table_id = self.target.table_id, action = self.kind().name(), grant_bytes; "Start maintenance unit");
        match &mut self.data {
            TaskData::Refresh => refresh_hook::run(ctx, &self.target, coverage).await,
            TaskData::Analyze => analyze_hook::run(ctx, &self.target).await,
            TaskData::Recluster(task) => task.run(ctx, &self.target).await,
        }
    }

    fn reset_progress(&mut self) {
        if let TaskData::Recluster(task) = &mut self.data {
            task.reset_progress();
        }
    }
}

impl SchedulerTask for MaintenanceTask {
    type ExecutionKey = Arc<MaintenanceIdentity>;

    fn key(&self) -> TaskKey {
        TaskKey {
            table: TableKey {
                tenant: self.identity.tenant.tenant_name().to_string(),
                catalog: self.target.catalog.clone(),
                table_id: self.target.table_id,
            },
            kind: self.kind(),
        }
    }

    fn execution_key(&self) -> Self::ExecutionKey {
        self.identity.clone()
    }
    fn ready_at(&self) -> Instant {
        self.runtime.ready_at
    }

    fn merge(&mut self, other: Self) {
        // Preserve the first deadline and retry budget. Only Recluster needs a rescan.
        match (&mut self.data, other.data) {
            (TaskData::Refresh, TaskData::Refresh) | (TaskData::Analyze, TaskData::Analyze) => {}
            (TaskData::Recluster(task), TaskData::Recluster(_)) => task.merge(),
            _ => unreachable!("scheduler only merges the same TaskKey"),
        }
    }
}

#[cfg(test)]
mod tests;
