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
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::runtime::CatchUnwindFuture;
use databend_common_catalog::table_context::RefreshAnchor;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::table_maintenance::record_stopped;
use databend_common_metrics::table_maintenance::record_unit;
use databend_storages_common_session::ReclusterWriteHint;
use parking_lot::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use tokio::time::timeout_at;
use tokio_util::sync::CancellationToken;

use super::MaintenanceIdentity;
use super::MaintenanceResources;
use super::context::Execution;
use super::context::execute_unit;
use super::scheduler::RunResult;
use super::scheduler::SchedulerTask;
use super::scheduler::TableKey;
use super::scheduler::TaskKey;
use super::scheduler::TaskKind;
use super::service::ForegroundActivity;
use crate::clusters::ClusterDiscovery;
use crate::interpreters::hook::TableHookTarget;
use crate::interpreters::hook::recluster_hook::ReclusterProgress;
use crate::interpreters::hook::refresh_hook;
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
    Refresh(RefreshAnchor),
    Recluster(ReclusterBatch),
}

const RECLUSTER_WRITE_THRESHOLD: u64 = 1 << 30;
const RECLUSTER_IDLE_RECHECK: Duration = Duration::from_secs(5);
const RECLUSTER_IDLE_PROBE_TIMEOUT: Duration = Duration::from_secs(1);

struct ReclusterBatch {
    cluster_key_id: u32,
    known_bytes: u64,
    window_end: Instant,
    /// The window/volume/idle checks govern startup only, not FINAL continuations.
    started: bool,
    progress: Option<ReclusterProgress>,
}

impl ReclusterBatch {
    fn volume_eligible(&self) -> bool {
        self.known_bytes >= RECLUSTER_WRITE_THRESHOLD
    }
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
    Exhausted(&'static str),
    InvalidExecution,
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
            Ok(MaintenanceOutcome::Completed) => TaskDecision::Completed,
            Ok(MaintenanceOutcome::Progress(delay)) => {
                self.failures = 0;
                self.blocked_since = None;
                self.ready_at = Instant::now() + delay;
                TaskDecision::Continue
            }
            Ok(outcome) => {
                if self.blocked_too_long() {
                    return TaskDecision::Exhausted("no_progress");
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
            Err(error) if invalid_execution(&error) => TaskDecision::InvalidExecution,
            Err(_) => {
                self.failures = self.failures.saturating_add(1);
                if self.failures >= MAX_TASK_FAILURES {
                    return TaskDecision::Exhausted("retry_exhausted");
                }
                if self.blocked_too_long() {
                    return TaskDecision::Exhausted("no_progress");
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

#[derive(Clone, Copy)]
struct WarehouseIdleSample {
    // The start of a successful probe, not the time this caller read the sample.
    checked_at: Option<Instant>,
    valid_until: Instant,
}

type WarehouseProbe = AsyncMutex<Option<WarehouseIdleSample>>;

/// Shared by the node's maintenance runners, not by foreground queries or remote nodes.
#[derive(Default)]
pub(super) struct WarehouseIdleProbes(Mutex<HashMap<String, Arc<WarehouseProbe>>>);

impl WarehouseIdleProbes {
    async fn check(
        &self,
        warehouse: &str,
        expires_at: Instant,
        cancellation: &CancellationToken,
    ) -> Option<Instant> {
        self.check_with(warehouse, expires_at, cancellation, async {
            matches!(
                ClusterDiscovery::instance()
                    .are_warehouse_peers_idle(warehouse)
                    .await,
                Ok(true)
            )
        })
        .await
    }

    async fn check_with(
        &self,
        warehouse: &str,
        expires_at: Instant,
        cancellation: &CancellationToken,
        probe: impl Future<Output = bool>,
    ) -> Option<Instant> {
        let now = Instant::now();
        let deadline = (now + RECLUSTER_IDLE_PROBE_TIMEOUT).min(expires_at);
        let slot = {
            let mut probes = self.0.lock();
            // Keep active entries so concurrent callers cannot create a second probe.
            probes.retain(|_, slot| {
                Arc::get_mut(slot)
                    .is_none_or(|slot| slot.get_mut().is_some_and(|s| now < s.valid_until))
            });
            match probes.get(warehouse) {
                Some(slot) => slot.clone(),
                None => probes.entry(warehouse.to_owned()).or_default().clone(),
            }
        };
        let checked_at = tokio::select! {
            biased;
            _ = cancellation.cancelled() => None,
            checked_at = async {
                // Hold only this warehouse's lock during I/O. The caller's budget
                // covers both waiting for a shared result and making a new probe.
                let (mut sample, waited) = match slot.try_lock() {
                    Ok(sample) => (sample, false),
                    Err(_) => (timeout_at(deadline.into(), slot.lock()).await.ok()?, true),
                };
                let started_at = Instant::now();
                if started_at >= deadline {
                    return None;
                }
                if let Some(sample) = *sample
                    && started_at < sample.valid_until
                {
                    return sample.checked_at;
                }
                let result = timeout_at(deadline.into(), probe).await;
                let finished_at = Instant::now();
                let timed_out = result.is_err() || finished_at >= deadline;
                // A waiter that takes over has less than a full probe budget.
                // Its timeout defers only this batch, not the entire warehouse.
                if finished_at >= expires_at || (waited && timed_out) {
                    return None;
                }
                let checked_at = (matches!(result, Ok(true)) && !timed_out).then_some(started_at);
                // Reuse a failed round across sequential dispatches too. A positive
                // result never extends the existing one-second freshness window.
                let valid_until = if checked_at.is_some() {
                    started_at + RECLUSTER_IDLE_PROBE_TIMEOUT
                } else {
                    finished_at + RECLUSTER_IDLE_RECHECK
                };
                *sample = Some(WarehouseIdleSample { checked_at, valid_until });
                checked_at
            } => checked_at,
        };
        checked_at.filter(|_| Instant::now() < expires_at)
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
            TaskData::Refresh(_) => TaskKind::Refresh,
            TaskData::Recluster(_) => TaskKind::Recluster,
        }
    }

    pub async fn from_write(
        ctx: &Arc<QueryContext>,
        target: TableHookTarget,
        recluster_write: Option<ReclusterWriteHint>,
        refresh: Option<RefreshAnchor>,
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
        let mut tasks = Vec::with_capacity(
            usize::from(refresh.is_some()) + usize::from(recluster_write.is_some()),
        );
        if let Some(anchor) = refresh {
            tasks.push(make(TaskData::Refresh(anchor), target.clone(), now));
        }
        if let Some(write) = recluster_write {
            // Use the session's cached window; enqueue must not read meta.
            match ctx.get_settings().get_recluster_accumulation_secs() {
                Ok(seconds) => {
                    let window_end = write.committed_at + Duration::from_secs(seconds);
                    let batch = ReclusterBatch {
                        cluster_key_id: write.cluster_key_id,
                        known_bytes: write.known_bytes,
                        window_end,
                        started: false,
                        progress: None,
                    };
                    tasks.push(make(TaskData::Recluster(batch), target, window_end));
                }
                Err(error) => {
                    // An invalid window must not suppress independent Refresh.
                    log::warn!(table_id = target.table_id, error :? = error;
                        "Skip automatic Recluster because its accumulation window could not be read");
                }
            }
        }
        Ok(tasks)
    }

    pub(super) async fn execute(
        mut self,
        cancellation: CancellationToken,
        resources: &Arc<MaintenanceResources>,
        demand: usize,
        foreground: &ForegroundActivity,
        idle_probes: &WarehouseIdleProbes,
    ) -> RunResult<Self> {
        let identity = self.identity.clone();
        let warehouse = &identity.execution_warehouse;
        let mut idle_check = None;
        if let TaskData::Recluster(batch) = &self.data
            && !batch.started
        {
            let now = Instant::now();
            if now < batch.window_end {
                self.runtime.ready_at = batch.window_end;
                return RunResult::Continue(self);
            }
            if !batch.volume_eligible() {
                let expires_at = batch.window_end + MAX_TASK_BLOCKED;
                // A busy or unreachable peer must not make this batch hold a grant.
                if now < expires_at
                    && foreground.is_idle()
                    && let Some(checked_at) = idle_probes
                        .check(warehouse, expires_at, &cancellation)
                        .await
                    && foreground.is_idle()
                {
                    idle_check = Some((checked_at, expires_at));
                } else {
                    if cancellation.is_cancelled() {
                        return RunResult::Stopped;
                    }
                    return self.wait_for_idle(expires_at);
                }
            }
        }

        let idle_cancellation = &cancellation;
        let task = &mut self;
        let result = CatchUnwindFuture::create(execute_unit(
            &identity,
            cancellation.clone(),
            resources,
            demand,
            |ctx| async move {
                if let Some((checked_at, expires_at)) = idle_check {
                    if Instant::now() >= expires_at || !foreground.is_idle() {
                        return Ok(None);
                    }
                    // A slow context setup can stale a successful preflight. Only
                    // then repeat the bounded probe, after the initial idle check passed.
                    if checked_at.elapsed() >= RECLUSTER_IDLE_PROBE_TIMEOUT
                        && (idle_probes
                            .check(warehouse, expires_at, idle_cancellation)
                            .await
                            .is_none()
                            || !foreground.is_idle())
                    {
                        return Ok(None);
                    }
                }
                task.run(ctx).await.map(Some)
            },
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
            Ok(Ok(Execution::Busy)) => {
                if let TaskData::Recluster(batch) = &self.data
                    && !cancellation.is_cancelled()
                {
                    let retained_bytes = batch
                        .progress
                        .as_ref()
                        .map_or(0, ReclusterProgress::retained_full_pass_bytes);
                    if resources.hard_limit_blocks_continuation(retained_bytes) {
                        let reason = "retained_full_pass_memory";
                        record_stopped(self.kind().name(), reason);
                        log::warn!(table_id = self.target.table_id, retained_bytes, reason;
                            "Stop background vector recluster: its full pass leaves no room for a new grant");
                        return self.complete();
                    }
                }
                Ok(MaintenanceOutcome::ResourceUnavailable)
            }
            Ok(Ok(Execution::Executed(Some(outcome)))) => Ok(outcome),
            Ok(Ok(Execution::Executed(None))) => {
                if cancellation.is_cancelled() {
                    return RunResult::Stopped;
                }
                let (_, expires_at) = idle_check.expect("idle admission was required");
                return self.wait_for_idle(expires_at);
            }
            Ok(Err(error)) => Err(error),
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
            && let TaskData::Recluster(batch) = &mut self.data
            && let Some(progress) = &mut batch.progress
        {
            // Failed resource checks may have consumed candidates without committing.
            // Replan after deferral without changing the scope of a vector full pass.
            progress.reset_candidates();
        }
        let result = resource_outcome(result);
        let action = self.kind().name();
        if cancellation.is_cancelled() {
            record_unit(action, "cancelled");
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
        record_unit(action, outcome);
        if let Err(error) = &result {
            log::warn!(table_id = self.target.table_id, action, error :? = error; "Maintenance task failed");
            if error.code() == ErrorCode::ABORTED_QUERY {
                self.reset_progress();
            }
        }
        match self.runtime.apply_outcome(result) {
            TaskDecision::Continue => RunResult::Continue(self),
            TaskDecision::Completed => self.complete(),
            TaskDecision::Exhausted(reason) => {
                record_stopped(action, reason);
                log::warn!(table_id = self.target.table_id, action, reason; "Stop bounded best-effort maintenance attempt");
                if matches!(&self.data, TaskData::Recluster(batch) if batch.started) {
                    self.complete()
                } else {
                    RunResult::RetryPendingAt(Instant::now() + Duration::from_secs(1))
                }
            }
            TaskDecision::InvalidExecution => {
                record_stopped(action, "invalid_target_or_identity");
                match &self.data {
                    // A newer key may have been queued under another execution identity.
                    TaskData::Recluster(_) => self.complete(),
                    TaskData::Refresh(_) => RunResult::Stopped,
                }
            }
        }
    }

    fn wait_for_idle(mut self, expires_at: Instant) -> RunResult<Self> {
        let now = Instant::now();
        if now >= expires_at {
            record_stopped(TaskKind::Recluster.name(), "admission_expired");
            // Nothing ran: same-key notifications can still start a fresh batch.
            return RunResult::Completed;
        }
        self.runtime.ready_at = (now + RECLUSTER_IDLE_RECHECK).min(expires_at);
        RunResult::Continue(self)
    }

    fn complete(&self) -> RunResult<Self> {
        match &self.data {
            TaskData::Recluster(batch) => RunResult::CompletedUnlessNewer(batch.cluster_key_id),
            TaskData::Refresh(_) => RunResult::Completed,
        }
    }

    async fn run(&mut self, ctx: Arc<QueryContext>) -> Result<MaintenanceOutcome> {
        let grant_bytes = ctx.get_settings().get_max_query_memory_usage()?;
        log::info!(source_query_id = self.source_query_id.as_str(), query_id = ctx.get_id().as_str(),
            table_id = self.target.table_id, action = self.kind().name(), grant_bytes; "Start maintenance unit");
        match &mut self.data {
            TaskData::Refresh(anchor) => refresh_hook::run(ctx, &self.target, anchor).await,
            TaskData::Recluster(batch) => {
                batch.started = true;
                ReclusterProgress::run(&mut batch.progress, ctx, &self.target, batch.cluster_key_id)
                    .await
            }
        }
    }

    fn reset_progress(&mut self) {
        if let TaskData::Recluster(batch) = &mut self.data {
            batch.progress = None;
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

    fn can_merge(
        kind: TaskKind,
        existing: &Self::ExecutionKey,
        incoming: &Self::ExecutionKey,
    ) -> bool {
        match kind {
            TaskKind::Refresh => existing == incoming,
            // Only write volume crosses users; each batch keeps one execution identity.
            TaskKind::Recluster => existing.execution_warehouse == incoming.execution_warehouse,
        }
    }

    fn ready_at(&self) -> Instant {
        self.runtime.ready_at
    }

    fn definition_version(&self) -> u32 {
        match &self.data {
            TaskData::Recluster(batch) => batch.cluster_key_id,
            TaskData::Refresh(_) => 0,
        }
    }

    fn merge(&mut self, mut other: Self) {
        match (&mut self.data, &mut other.data) {
            (TaskData::Refresh(anchor), TaskData::Refresh(incoming)) => {
                if incoming.base_table_seq < anchor.base_table_seq {
                    std::mem::swap(anchor, incoming);
                }
            }
            (TaskData::Recluster(batch), TaskData::Recluster(incoming)) => {
                // Order ordinary key changes; FLASHBACK to a smaller ID is not tracked.
                match incoming.cluster_key_id.cmp(&batch.cluster_key_id) {
                    std::cmp::Ordering::Greater => *self = other,
                    std::cmp::Ordering::Equal if !batch.started => {
                        batch.known_bytes = batch.known_bytes.saturating_add(incoming.known_bytes);
                    }
                    // Once FINAL starts, ignore same-key notifications until it ends, even
                    // if the pass does not cover the blocks written while it is running.
                    std::cmp::Ordering::Equal | std::cmp::Ordering::Less => {}
                }
            }
            _ => unreachable!("scheduler only merges the same TaskKey"),
        }
    }
}

#[cfg(test)]
mod tests;
