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

use std::fmt;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::runtime::CpuProfiler;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::MemoryProfiler;
use databend_common_base::runtime::ProfileScope;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::info;
use parking_lot::Mutex;
use serde::Deserialize;
use serde::Serialize;

use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::sessions::SessionManager;

pub static PERF: &str = "/actions/perf";

/// How often a running profiling session checks its time limit and, for a
/// query, whether the query is still running on this node.
const WATCH_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(tag = "type", content = "query_id", rename_all = "snake_case")]
pub enum PerfTarget {
    /// The threads running a query on this node.
    Query(String),
    /// Every thread of this node.
    Process,
}

impl fmt::Display for PerfTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PerfTarget::Query(query_id) => write!(f, "query {query_id}"),
            PerfTarget::Process => write!(f, "the whole process"),
        }
    }
}

/// What a profiling session samples.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PerfKind {
    /// Call stacks on CPU, see `CpuProfiler`.
    Cpu,
    /// Call stacks allocating memory, see `MemoryProfiler`.
    Memory,
}

impl fmt::Display for PerfKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PerfKind::Cpu => write!(f, "CPU"),
            PerfKind::Memory => write!(f, "memory"),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum PerfRequest {
    Start {
        kind: PerfKind,
        target: PerfTarget,
        /// Samples per second, only for `PerfKind::Cpu`.
        frequency: i32,
        max_seconds: u64,
    },
    Stop {
        kind: PerfKind,
        target: PerfTarget,
    },
    Status {
        kind: PerfKind,
        target: PerfTarget,
    },
    Dump {
        kind: PerfKind,
        target: PerfTarget,
    },
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum PerfState {
    /// The query is not running on this node and no profile of it is kept.
    NotRunning,
    /// The target is not being profiled and no profile of it is kept.
    Idle,
    Profiling,
    Stopped,
    /// The request could not be served on this node, see `message`.
    Failed,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PerfNodeStatus {
    pub state: PerfState,
    pub elapsed_secs: u64,
    pub message: Option<String>,
    /// Folded stacks of the samples collected so far, only set for `Dump`.
    pub folded: Option<String>,
}

impl PerfNodeStatus {
    fn with_state(state: PerfState) -> Self {
        PerfNodeStatus {
            state,
            elapsed_secs: 0,
            message: None,
            folded: None,
        }
    }

    pub fn failed(message: String) -> Self {
        PerfNodeStatus {
            message: Some(message),
            ..Self::with_state(PerfState::Failed)
        }
    }
}

/// An on-demand profiling session of this node. The profilers are
/// process-wide, so a node profiles one target at a time per `PerfKind`; the
/// last session is kept after it stops so its samples can still be fetched.
struct PerfSession {
    id: u64,
    kind: PerfKind,
    target: PerfTarget,
    /// The threads sampled, the query's `perf_query_token` on this node.
    scope: ProfileScope,
    started_at: Instant,
    deadline: Instant,
    state: PerfSessionState,
}

enum Profiler {
    Cpu(CpuProfiler),
    Memory(MemoryProfiler),
}

impl Profiler {
    fn start(kind: PerfKind, scope: ProfileScope, frequency: i32) -> Result<Profiler> {
        match kind {
            PerfKind::Cpu => CpuProfiler::start(scope, frequency).map(Profiler::Cpu),
            PerfKind::Memory => MemoryProfiler::start(scope).map(Profiler::Memory),
        }
    }

    fn dump_folded(&self) -> Result<String> {
        match self {
            Profiler::Cpu(profiler) => profiler.dump_folded(),
            Profiler::Memory(profiler) => profiler.dump_folded(),
        }
    }
}

enum PerfSessionState {
    Profiling(Profiler),
    Stopped {
        elapsed: Duration,
        reason: String,
        folded: Result<String>,
    },
}

impl PerfSession {
    fn status(&self, with_folded: bool) -> PerfNodeStatus {
        match &self.state {
            PerfSessionState::Profiling(profiler) => {
                let (folded, message) = match with_folded {
                    false => (None, None),
                    true => match profiler.dump_folded() {
                        Ok(folded) => (Some(folded), None),
                        Err(cause) => (None, Some(cause.message())),
                    },
                };
                PerfNodeStatus {
                    state: PerfState::Profiling,
                    elapsed_secs: self.started_at.elapsed().as_secs(),
                    message,
                    folded,
                }
            }
            PerfSessionState::Stopped {
                elapsed,
                reason,
                folded,
            } => {
                let (folded, message) = match (with_folded, folded) {
                    (true, Ok(folded)) => (Some(folded.clone()), Some(reason.clone())),
                    (true, Err(cause)) => (None, Some(cause.message())),
                    (false, _) => (None, Some(reason.clone())),
                };
                PerfNodeStatus {
                    state: PerfState::Stopped,
                    elapsed_secs: elapsed.as_secs(),
                    message,
                    folded,
                }
            }
        }
    }

    /// Stop sampling and keep the final samples.
    fn stop(&mut self, reason: String) {
        if let PerfSessionState::Profiling(profiler) = &self.state {
            info!(
                "Stop {} profiling of {} on this node: {}",
                self.kind, self.target, reason
            );
            let folded = profiler.dump_folded();
            self.state = PerfSessionState::Stopped {
                elapsed: self.started_at.elapsed(),
                reason,
                folded,
            };
        }
    }
}

static CPU_SESSION: Mutex<Option<PerfSession>> = Mutex::new(None);
static MEMORY_SESSION: Mutex<Option<PerfSession>> = Mutex::new(None);

fn session_of(kind: PerfKind) -> &'static Mutex<Option<PerfSession>> {
    match kind {
        PerfKind::Cpu => &CPU_SESSION,
        PerfKind::Memory => &MEMORY_SESSION,
    }
}

pub async fn perf(request: PerfRequest) -> Result<PerfNodeStatus> {
    // Dumping the samples symbolizes every sampled stack, keep it off the
    // async workers.
    databend_common_base::runtime::spawn_blocking(move || match request {
        PerfRequest::Start {
            kind,
            target,
            frequency,
            max_seconds,
        } => start(kind, target, frequency, Duration::from_secs(max_seconds)),
        PerfRequest::Stop { kind, target } => Ok(stop(kind, &target)),
        PerfRequest::Status { kind, target } => Ok(status(kind, &target, false)),
        PerfRequest::Dump { kind, target } => Ok(status(kind, &target, true)),
    })
    .await
    .map_err(|cause| ErrorCode::Internal(format!("perf task failed: {cause}")))?
}

fn start(
    kind: PerfKind,
    target: PerfTarget,
    frequency: i32,
    max_duration: Duration,
) -> Result<PerfNodeStatus> {
    let mut session = session_of(kind).lock();

    let profiling = session
        .as_ref()
        .filter(|current| matches!(current.state, PerfSessionState::Profiling(_)));
    if let Some(current) = profiling {
        if current.target == target {
            return Ok(current.status(false));
        }
    }

    let scope = match &target {
        PerfTarget::Query(query_id) => match find_perf_query_token(query_id) {
            Some(token) => ProfileScope::Query(token),
            None => return Ok(PerfNodeStatus::with_state(PerfState::NotRunning)),
        },
        PerfTarget::Process => ProfileScope::Process,
    };

    if let Some(current) = profiling {
        return Ok(PerfNodeStatus::failed(format!(
            "Profiling {} of {} on this node, stop it first",
            kind, current.target
        )));
    }

    let profiler = match Profiler::start(kind, scope, frequency) {
        Ok(profiler) => profiler,
        // EXPLAIN PERF or /debug/pprof/profile owns the CPU profiler.
        Err(cause) => return Ok(PerfNodeStatus::failed(cause.message())),
    };

    match kind {
        PerfKind::Cpu => info!(
            "Start CPU profiling of {} on this node, frequency: {}, max duration: {:?}",
            target, frequency, max_duration
        ),
        PerfKind::Memory => info!(
            "Start memory profiling of {} on this node, max duration: {:?}",
            target, max_duration
        ),
    }
    let id = session.as_ref().map_or(0, |s| s.id + 1);
    let now = Instant::now();
    let new_session = PerfSession {
        id,
        kind,
        target,
        scope,
        started_at: now,
        deadline: now + max_duration,
        state: PerfSessionState::Profiling(profiler),
    };
    let status = new_session.status(false);
    *session = Some(new_session);

    GlobalIORuntime::instance().spawn(watch_session(kind, id));
    Ok(status)
}

fn stop(kind: PerfKind, target: &PerfTarget) -> PerfNodeStatus {
    let mut session = session_of(kind).lock();
    match session.as_mut() {
        Some(current) if &current.target == target => {
            current.stop("stopped by request".to_string());
            current.status(false)
        }
        _ => idle_status(target),
    }
}

fn status(kind: PerfKind, target: &PerfTarget, with_folded: bool) -> PerfNodeStatus {
    let session = session_of(kind).lock();
    match session.as_ref() {
        Some(current) if &current.target == target => current.status(with_folded),
        _ => idle_status(target),
    }
}

/// Status of a target that has no session on this node.
fn idle_status(target: &PerfTarget) -> PerfNodeStatus {
    match target {
        PerfTarget::Query(query_id) if find_perf_query_token(query_id).is_none() => {
            PerfNodeStatus::with_state(PerfState::NotRunning)
        }
        _ => PerfNodeStatus::with_state(PerfState::Idle),
    }
}

/// Stops the session `id` once it reaches its deadline or its query is no
/// longer running on this node, so a forgotten session never keeps the
/// profiler running.
async fn watch_session(kind: PerfKind, id: u64) {
    loop {
        tokio::time::sleep(WATCH_INTERVAL).await;
        let watching =
            databend_common_base::runtime::spawn_blocking(move || check_session(kind, id));
        if !matches!(watching.await, Ok(true)) {
            return;
        }
    }
}

/// Returns whether session `id` of `kind` is still profiling after the check.
fn check_session(kind: PerfKind, id: u64) -> bool {
    let mut session = session_of(kind).lock();
    let Some(current) = session.as_mut() else {
        return false;
    };
    if current.id != id || !matches!(current.state, PerfSessionState::Profiling(_)) {
        return false;
    }

    if let (PerfTarget::Query(query_id), ProfileScope::Query(token)) =
        (&current.target, current.scope)
    {
        // A new query with the same id gets a new token.
        if find_perf_query_token(query_id) != Some(token) {
            current.stop("query finished".to_string());
            return false;
        }
    }
    if Instant::now() >= current.deadline {
        current.stop("reached max duration".to_string());
        return false;
    }
    true
}

/// The perf token of `query_id` if the query is running on this node, either
/// as the coordinator or as a remote fragment executor.
fn find_perf_query_token(query_id: &str) -> Option<u64> {
    if let Some(token) = SessionManager::instance().get_query_perf_token(query_id) {
        return Some(token);
    }
    DataExchangeManager::instance()
        .get_query_ctx(query_id)
        .ok()
        .map(|ctx| ctx.get_perf_query_token())
}
