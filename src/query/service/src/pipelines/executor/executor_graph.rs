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
use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::future::Future;
use std::sync::Arc;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use databend_common_base::base::WatchNotify;
use databend_common_base::runtime::ExecutorStats;
use databend_common_base::runtime::ExecutorStatsSnapshot;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ParentMemStat;
use databend_common_base::runtime::PerfEvent;
use databend_common_base::runtime::PerfValue;
use databend_common_base::runtime::QueryTimeSeriesProfileBuilder;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TimeSeriesProfiles;
use databend_common_base::runtime::TrackingPayload;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_base::runtime::error_info::NodeErrorType;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ResultExt;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::EventCause;
use databend_common_pipeline::core::ExecutorWaker;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::PlanProfile;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProxyWakeCallback;
use databend_common_pipeline::core::port::connect;
use databend_common_pipeline::core::port_trigger::DirectedEdge;
use databend_common_pipeline::core::port_trigger::UpdateList;
use databend_common_pipeline::core::port_trigger::UpdateTrigger;
use databend_common_pipeline::core::profile::PlanScope;
use databend_common_storages_system::QueryExecutionStatsQueue;
use fastrace::prelude::*;
use log::debug;
use log::trace;
use log::warn;
use parking_lot::Condvar;
use parking_lot::Mutex;
use petgraph::Direction;
use petgraph::dot::Config;
use petgraph::dot::Dot;
use petgraph::prelude::EdgeIndex;
use petgraph::prelude::NodeIndex;
use petgraph::prelude::StableGraph;
use tokio::sync::Notify;

use crate::pipelines::executor::ExecutorTask;
use crate::pipelines::executor::ExecutorWorkerContext;
use crate::pipelines::executor::ProcessorAsyncTask;
use crate::pipelines::executor::QueriesExecutorTasksQueue;
use crate::pipelines::executor::QueriesPipelineExecutor;
use crate::pipelines::executor::QueryExecutorTasksQueue;
use crate::pipelines::executor::QueryPipelineExecutor;
use crate::pipelines::executor::WorkersCondvar;
use crate::pipelines::executor::memory_limit_diagnostics::log_memory_limit_diagnostics;
use crate::pipelines::executor::memory_limit_diagnostics::out_of_limit_error;
use crate::pipelines::executor::processor_async_task::ExecutorTasksQueue;
use crate::servers::flight::v1::packets::NodePerfCounters;

enum State {
    Idle,
    Processing,
    Finished,
}

#[derive(Debug, Clone)]
struct EdgeInfo {
    input_index: usize,
    output_index: usize,
}

#[derive(Debug, Clone)]
pub struct PlanNodeMemoryUsage {
    pub identity: String,
    pub current_bytes: usize,
    pub peak_bytes: usize,
}

fn plan_node_memory_identity(profile: &Profile) -> String {
    let mut identity = profile
        .plan_name
        .as_deref()
        .unwrap_or("unknown")
        .to_string();

    if let Some(plan_id) = profile.plan_id {
        identity.push_str(&format!(" [#{plan_id}]"));
    }

    if !profile.title.is_empty() {
        identity.push(' ');
        identity.push_str(profile.title.as_str());
    }

    identity
}

/// Cancellation request for a node's in-flight `async_process`, see
/// `Processor::cancel_async_on_outputs_finished`.
///
/// The request is sticky: output ports never leave the finished state, so once every output has
/// finished no later `async_process` of the node is useful either.
#[derive(Default)]
struct AsyncCancel {
    requested: AtomicBool,
    notify: Notify,
}

impl AsyncCancel {
    fn request(&self) {
        self.requested.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }

    async fn cancelled(&self) {
        loop {
            // Created before checking the flag: `notify_waiters` wakes every `Notified` that
            // already exists, even if it has not been polled yet.
            let notified = self.notify.notified();
            if self.requested.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

/// A node's scheduling state and its processor, guarded by one lock.
///
/// While the node is `Processing`, the processor is owned by a `ProcessorWrapper` and `processor`
/// is `None`.
struct Slot {
    state: State,
    processor: Option<Box<dyn Processor>>,
}

impl Slot {
    fn processor(&mut self) -> Result<&mut dyn Processor> {
        match self.processor.as_deref_mut() {
            Some(processor) => Ok(processor),
            None => Err(ErrorCode::Internal(
                "Processor is not in the executor graph while scheduling it",
            )),
        }
    }
}

pub(crate) struct Node {
    slot: std::sync::Mutex<Slot>,
    cancel_async_on_outputs_finished: bool,
    async_cancel: AsyncCancel,

    pub(crate) tracking_payload: TrackingPayload,
    updated_list: Arc<UpdateList>,
    inputs_port: Vec<Arc<InputPort>>,
    outputs_port: Vec<Arc<OutputPort>>,
}

impl Node {
    pub fn create(
        pid: usize,
        scope: Option<Arc<PlanScope>>,
        processor: Box<dyn Processor>,
        inputs_port: &[Arc<InputPort>],
        outputs_port: &[Arc<OutputPort>],
        time_series_profile: Option<Arc<TimeSeriesProfiles>>,
        plan_mem_stat: Option<Arc<MemStat>>,
        processor_interrupt: Arc<AtomicBool>,
    ) -> Arc<Node> {
        let p_name = processor.name();
        let cancel_async_on_outputs_finished = processor.cancel_async_on_outputs_finished();
        let tracking_payload = {
            let mut tracking_payload = ThreadTracker::new_tracking_payload();

            // Node tracking profile
            tracking_payload.profile = Some(Arc::new(Profile::create(
                pid,
                p_name,
                scope.as_ref().map(|x| x.id),
                scope.as_ref().map(|x| x.name.clone()),
                scope.as_ref().and_then(|x| x.parent_id),
                scope
                    .as_ref()
                    .map(|x| x.title.clone())
                    .unwrap_or(Arc::new(String::new())),
                scope
                    .as_ref()
                    .map(|x| x.labels.clone())
                    .unwrap_or(Arc::new(vec![])),
                scope.as_ref().map(|x| x.metrics_registry.clone()),
            )));

            // Node tracking metrics
            tracking_payload.metrics = scope.as_ref().map(|x| x.metrics_registry.clone());

            tracking_payload.local_time_series_profile = time_series_profile;

            if let Some(plan_mem_stat) = plan_mem_stat {
                tracking_payload.mem_stat = Some(plan_mem_stat);
            }
            tracking_payload.processor_interrupt = Some(processor_interrupt);

            tracking_payload
        };

        Arc::new(Node {
            slot: std::sync::Mutex::new(Slot {
                state: State::Idle,
                processor: Some(processor),
            }),
            cancel_async_on_outputs_finished,
            async_cancel: AsyncCancel::default(),
            updated_list: UpdateList::create(),
            inputs_port: inputs_port.to_vec(),
            outputs_port: outputs_port.to_vec(),
            tracking_payload,
        })
    }

    fn name(&self) -> &str {
        self.tracking_payload
            .profile
            .as_ref()
            .map_or("", |profile| profile.p_name.as_str())
    }

    fn lock_slot(&self) -> MutexGuard<'_, Slot> {
        self.slot.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn outputs_finished(&self) -> bool {
        !self.outputs_port.is_empty() && self.outputs_port.iter().all(|x| x.is_finished())
    }

    pub fn record_error(&self, error: NodeErrorType) {
        if let Some(profile) = &self.tracking_payload.profile {
            let mut errors_info = profile.errors.lock();
            errors_info.push(error);
        }
    }

    pub unsafe fn trigger(&self, queue: &mut VecDeque<DirectedEdge>) {
        unsafe { self.updated_list.trigger(queue) }
    }

    pub unsafe fn create_trigger(&self, index: EdgeIndex) -> *mut UpdateTrigger {
        unsafe {
            self.updated_list
                .create_trigger(index)
                .expect("Failed to create trigger")
        }
    }
}

const POINTS_MASK: u64 = 0xFFFFFFFF00000000;
const EPOCH_MASK: u64 = 0x00000000FFFFFFFF;

// DEFAULT_POINTS is equal to Priority::MEDIUM
const DEFAULT_POINTS: u64 = 3;

struct ExecutingGraph {
    finished_nodes: AtomicUsize,
    graph: StableGraph<Arc<Node>, EdgeInfo>,
    /// points store two values
    ///
    /// - the high 32 bit store the number of points that can be consumed
    /// - the low 32 bit store this points belong to which epoch
    points: AtomicU64,
    max_points: AtomicU64,
    query_id: Arc<String>,
    should_finish: Arc<AtomicBool>,
    finished_notify: Arc<WatchNotify>,
    finish_condvar_notify: Option<Arc<(Mutex<bool>, Condvar)>>,
    finished_error: Mutex<Option<ErrorCode>>,
    executor_stats: ExecutorStats,
    waker: Arc<ExecutorWaker>,
    /// Perf event groups selected for this query (only set during EXPLAIN PERF).
    perf_event_groups: Vec<Vec<PerfEvent>>,
}

type StateLockGuard = ExecutingGraph;

impl ExecutingGraph {
    pub fn create(
        mut pipeline: Pipeline,
        init_epoch: u32,
        query_id: Arc<String>,
        finish_condvar_notify: Option<Arc<(Mutex<bool>, Condvar)>>,
        perf_event_groups: Vec<Vec<PerfEvent>>,
    ) -> Result<ExecutingGraph> {
        let waker = pipeline.get_waker();
        let perf_enabled = !perf_event_groups.is_empty();
        let mut graph = StableGraph::new();
        let mut time_series_profile_builder =
            QueryTimeSeriesProfileBuilder::new(query_id.to_string());
        let mut plan_memory_stats = HashMap::new();
        let should_finish = Arc::new(AtomicBool::new(false));
        Self::init_graph(
            &mut pipeline,
            &mut graph,
            &mut time_series_profile_builder,
            &mut plan_memory_stats,
            perf_enabled,
            &should_finish,
        );
        let executor_stats = ExecutorStats::new();
        Ok(ExecutingGraph {
            graph,
            finished_nodes: AtomicUsize::new(0),
            points: AtomicU64::new((DEFAULT_POINTS << 32) | init_epoch as u64),
            max_points: AtomicU64::new(DEFAULT_POINTS),
            query_id,
            should_finish,
            finished_notify: Arc::new(WatchNotify::new()),
            finish_condvar_notify,
            finished_error: Mutex::new(None),
            executor_stats,
            waker,
            perf_event_groups,
        })
    }

    pub fn from_pipelines(
        mut pipelines: Vec<Pipeline>,
        init_epoch: u32,
        query_id: Arc<String>,
        finish_condvar_notify: Option<Arc<(Mutex<bool>, Condvar)>>,
        perf_event_groups: Vec<Vec<PerfEvent>>,
    ) -> Result<ExecutingGraph> {
        // Create a shared waker at the graph level
        let graph_waker = ExecutorWaker::create();

        // Proxy each pipeline's waker to the graph_waker
        for pipeline in &pipelines {
            let proxy_target = ProxyWakeCallback::create(graph_waker.clone());

            pipeline.get_waker().bind(proxy_target);
        }

        let perf_enabled = !perf_event_groups.is_empty();
        let mut graph = StableGraph::new();
        let mut time_series_profile_builder =
            QueryTimeSeriesProfileBuilder::new(query_id.to_string());
        let mut plan_memory_stats = HashMap::new();
        let should_finish = Arc::new(AtomicBool::new(false));
        for pipeline in &mut pipelines {
            Self::init_graph(
                pipeline,
                &mut graph,
                &mut time_series_profile_builder,
                &mut plan_memory_stats,
                perf_enabled,
                &should_finish,
            );
        }
        let executor_stats = ExecutorStats::new();
        Ok(ExecutingGraph {
            finished_nodes: AtomicUsize::new(0),
            graph,
            points: AtomicU64::new((DEFAULT_POINTS << 32) | init_epoch as u64),
            max_points: AtomicU64::new(DEFAULT_POINTS),
            query_id,
            should_finish,
            finished_notify: Arc::new(WatchNotify::new()),
            finish_condvar_notify,
            finished_error: Mutex::new(None),
            executor_stats,
            waker: graph_waker,
            perf_event_groups,
        })
    }

    fn init_graph(
        pipeline: &mut Pipeline,
        graph: &mut StableGraph<Arc<Node>, EdgeInfo>,
        time_series_profile_builder: &mut QueryTimeSeriesProfileBuilder,
        plan_memory_stats: &mut HashMap<u32, Arc<MemStat>>,
        perf_enabled: bool,
        interrupt: &Arc<AtomicBool>,
    ) {
        let offset = graph.node_count();
        let (nodes, edges) = pipeline.take_graph();
        for node in nodes {
            let pid = graph.node_count();
            let mut time_series_profile = None;

            if let Some(scope) = node.scope.as_ref() {
                let plan_id = scope.id;
                time_series_profile =
                    Some(time_series_profile_builder.register_time_series_profile(plan_id));
            }

            let plan_mem_stat = node.scope.as_ref().and_then(|scope| {
                let query_mem_stat = ThreadTracker::mem_stat()?;
                let plan_id = scope.id;
                Some(
                    plan_memory_stats
                        .entry(plan_id)
                        .or_insert_with(|| {
                            MemStat::create_child(
                                None,
                                0,
                                ParentMemStat::Normal(query_mem_stat.clone()),
                            )
                        })
                        .clone(),
                )
            });

            let node_index = NodeIndex::new(pid);
            let mut processor = node.proc.into_inner();
            processor.set_id(node_index);

            let graph_node_index = graph.add_node(Node::create(
                pid,
                node.scope,
                processor,
                &node.inputs,
                &node.outputs,
                time_series_profile,
                plan_mem_stat,
                interrupt.clone(),
            ));
            debug_assert_eq!(graph_node_index, node_index);
        }

        // FIXME:
        let query_time_series = Arc::new(time_series_profile_builder.build());
        let node_indices: Vec<_> = graph.node_indices().collect();
        for node_index in node_indices {
            // we are sure that the node is only have one reference in the graph
            let mut_node = Arc::get_mut(&mut graph[node_index]);
            debug_assert!(
                mut_node.is_some(),
                "ExecutorGraph's node should only have one reference"
            );
            if let Some(mut_node) = mut_node {
                mut_node.tracking_payload.time_series_profile = Some(query_time_series.clone());
                mut_node.tracking_payload.perf_enabled = perf_enabled;
            }
        }

        for (source, target, edge_weight) in edges {
            {
                let source = NodeIndex::new(offset + source.index());
                let target = NodeIndex::new(offset + target.index());

                let edge_index = graph.add_edge(source, target, EdgeInfo {
                    input_index: edge_weight.input_index,
                    output_index: edge_weight.output_index,
                });

                unsafe {
                    let (target_node, target_port) = (target, edge_weight.input_index);
                    let input_trigger = graph[target_node].create_trigger(edge_index);
                    graph[target_node].inputs_port[target_port].set_trigger(input_trigger);

                    let (source_node, source_port) = (source, edge_weight.output_index);
                    let output_trigger = graph[source_node].create_trigger(edge_index);
                    graph[source_node].outputs_port[source_port].set_trigger(output_trigger);

                    let source_plan_id = graph[source_node]
                        .tracking_payload
                        .profile
                        .as_ref()
                        .and_then(|x| x.plan_id);
                    let target_plan_id = graph[target_node]
                        .tracking_payload
                        .profile
                        .as_ref()
                        .and_then(|x| x.plan_id);

                    if source_plan_id.is_some() && source_plan_id != target_plan_id {
                        graph[source_node].outputs_port[source_port].record_profile();
                    }

                    connect(
                        &graph[target_node].inputs_port[target_port],
                        &graph[source_node].outputs_port[source_port],
                    );
                }
            }
        }
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn init_schedule_queue(
        locker: &StateLockGuard,
        capacity: usize,
        graph: &Arc<RunningGraph>,
    ) -> Result<ScheduleQueue> {
        unsafe {
            let mut schedule_queue = ScheduleQueue::with_capacity(capacity);
            for sink_index in locker.graph.externals(Direction::Outgoing) {
                ExecutingGraph::schedule_queue(
                    locker,
                    sink_index,
                    None,
                    &mut schedule_queue,
                    graph,
                )?;
            }

            Ok(schedule_queue)
        }
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn schedule_queue(
        locker: &StateLockGuard,
        index: NodeIndex,
        mut executed: Option<Box<dyn Processor>>,
        schedule_queue: &mut ScheduleQueue,
        graph: &Arc<RunningGraph>,
    ) -> Result<()> {
        let mut need_schedule_nodes = VecDeque::new();
        let mut need_schedule_edges = VecDeque::new();

        need_schedule_nodes.push_back(index);

        while !need_schedule_nodes.is_empty() || !need_schedule_edges.is_empty() {
            // To avoid lock too many times, we will try to cache lock.
            let mut state_guard_cache = None;
            let mut event_cause = EventCause::Other;

            if need_schedule_nodes.is_empty() {
                let edge = need_schedule_edges.pop_front().unwrap();
                let target_index = DirectedEdge::get_target(&edge, &locker.graph)?;

                event_cause = match edge {
                    DirectedEdge::Source(index) => {
                        EventCause::Input(locker.graph.edge_weight(index).unwrap().input_index)
                    }
                    DirectedEdge::Target(index) => {
                        EventCause::Output(locker.graph.edge_weight(index).unwrap().output_index)
                    }
                };

                let node = &locker.graph[target_index];
                let slot = node.lock_slot();

                if matches!(slot.state, State::Idle) {
                    state_guard_cache = Some(slot);
                    need_schedule_nodes.push_back(target_index);
                } else if node.cancel_async_on_outputs_finished
                    && matches!(slot.state, State::Processing)
                    && matches!(event_cause, EventCause::Output(_))
                    && node.outputs_finished()
                {
                    node.async_cancel.request();
                }
            }

            if let Some(schedule_index) = need_schedule_nodes.pop_front() {
                let node = &locker.graph[schedule_index];
                let slot = state_guard_cache.get_or_insert_with(|| node.lock_slot());
                match executed.take() {
                    // The node that just finished executing hands its processor back.
                    Some(processor) => slot.processor = Some(processor),
                    // A wake-up for a running or finished node. A running node calls `event()`
                    // when it completes, so there is nothing to do.
                    None if !matches!(slot.state, State::Idle) => continue,
                    None => {}
                }

                let (event, process_rows) = {
                    let mut payload = node.tracking_payload.clone();
                    payload.process_rows = AtomicUsize::new(0);
                    let guard = ThreadTracker::tracking(payload);

                    let slot = state_guard_cache.as_mut().unwrap();
                    let event = slot.processor()?.event_with_cause(event_cause)?;
                    let process_rows = ThreadTracker::process_rows();
                    match guard.flush() {
                        Ok(_) => Ok((event, process_rows)),
                        Err(out_of_limit) => Err(out_of_limit_error(out_of_limit)),
                    }
                }?;

                trace!(
                    "node id: {:?}, name: {:?}, event: {:?}",
                    schedule_index,
                    node.name(),
                    event
                );
                let processor_state = match event {
                    Event::Finished => {
                        if !matches!(
                            state_guard_cache.as_deref().map(|x| &x.state),
                            Some(State::Finished)
                        ) {
                            locker.finished_nodes.fetch_add(1, Ordering::SeqCst);
                        }

                        State::Finished
                    }
                    Event::NeedData | Event::NeedConsume => State::Idle,
                    Event::Sync => {
                        let slot = state_guard_cache.as_mut().unwrap();
                        schedule_queue.push_sync(ProcessorWrapper::create(
                            schedule_index,
                            slot.processor.take(),
                            graph,
                            process_rows,
                        ));
                        State::Processing
                    }
                    Event::Async => {
                        // The request is sticky, so this `async_process` would be cancelled at
                        // once and the node would come straight back here, spinning forever.
                        if node.async_cancel.is_requested() {
                            return Err(ErrorCode::Internal(format!(
                                "Processor {} (node {}) returned Event::Async after all its outputs finished, \
                                 which cancel_async_on_outputs_finished does not allow",
                                node.name(),
                                schedule_index.index()
                            )));
                        }

                        let slot = state_guard_cache.as_mut().unwrap();
                        schedule_queue.push_async(ProcessorWrapper::create(
                            schedule_index,
                            slot.processor.take(),
                            graph,
                            process_rows,
                        ));
                        State::Processing
                    }
                };

                // SAFETY: the caller of `schedule_queue` serializes access to the graph.
                unsafe { node.trigger(&mut need_schedule_edges) };
                state_guard_cache.unwrap().state = processor_state;
            }
        }

        Ok(())
    }

    /// Checks if a task can be performed in the current epoch, consuming a point if possible.
    pub fn can_perform_task(&self, global_epoch: u32) -> bool {
        let max_points = self.max_points.load(Ordering::SeqCst);
        let mut expected_value = 0;
        let mut desired_value = 0;

        loop {
            match self.points.compare_exchange_weak(
                expected_value,
                desired_value,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return (desired_value & EPOCH_MASK) as u32 == global_epoch;
                }
                Err(new_expected) => {
                    let remain_points = (new_expected & POINTS_MASK) >> 32;
                    let epoch = new_expected & EPOCH_MASK;

                    expected_value = new_expected;

                    if epoch > global_epoch as u64 {
                        desired_value = new_expected;
                    } else if epoch < global_epoch as u64 {
                        desired_value = (max_points - 1) << 32 | global_epoch as u64;
                    } else if remain_points >= 1 {
                        desired_value = (remain_points - 1) << 32 | epoch;
                    } else {
                        desired_value = max_points << 32 | (epoch + 1);
                    }
                }
            }
        }
    }
}

/// A scheduled processor. The wrapper owns the processor while it runs, so only the worker holding
/// it can drive the processor.
///
/// Rescheduling the node hands the processor back to the graph (`RunningGraph::schedule_queue`).
/// A wrapper that is dropped instead, e.g. after an error or when the query shuts down, also
/// returns it, so processors are always dropped together with the graph.
pub struct ProcessorWrapper {
    pub node: NodeIndex,
    /// Always `Some` while the wrapper is alive. It is an `Option` only so that the processor can
    /// be moved out when the wrapper is consumed: by `RunningGraph::schedule_queue` on the normal
    /// path, or by `Drop` otherwise.
    processor: Option<Box<dyn Processor>>,
    graph: Arc<RunningGraph>,
    pub process_rows: usize,
}

impl ProcessorWrapper {
    fn create(
        node: NodeIndex,
        processor: Option<Box<dyn Processor>>,
        graph: &Arc<RunningGraph>,
        process_rows: usize,
    ) -> ProcessorWrapper {
        debug_assert!(processor.is_some());
        ProcessorWrapper {
            node,
            processor,
            graph: graph.clone(),
            process_rows,
        }
    }

    pub fn graph(&self) -> &Arc<RunningGraph> {
        &self.graph
    }

    pub fn processor(&mut self) -> &mut dyn Processor {
        self.processor
            .as_deref_mut()
            .expect("ProcessorWrapper owns its processor until it is dropped")
    }

    /// Runs `async_process` inside a span named after the processor.
    pub async fn async_process(&mut self) -> Result<()> {
        let node = self.node;
        let span =
            Span::enter_with_local_parent(format!("{}::async_process", self.graph.node_name(node)))
                .with_property(|| ("graph-node-id", node.index().to_string()));

        match self.processor().async_process().await {
            Ok(_) => Ok(()),
            Err(err) => {
                span.with_property(|| ("error", "true")).add_properties(|| {
                    [
                        ("error.type", err.code().to_string()),
                        ("error.message", err.display_text()),
                    ]
                });
                log::info!(error = err.to_string(); "Error in process");
                Err(err)
            }
        }
    }
}

/// Hands the processor back to its node's slot when a wrapper is dropped without being
/// rescheduled.
///
/// On the normal path `RunningGraph::schedule_queue` has already taken the processor, so this is
/// a no-op. It only does work when a scheduled task is abandoned:
/// - `process()` or `async_process()` returned an error and the worker gives up the task;
/// - the query finishes or is aborted while tasks are still queued, and the queues are dropped;
/// - a spawned `ProcessorAsyncTask` is dropped before completing (e.g. runtime shutdown, or its
///   future panicked and is dropped later).
///
/// Without this, the processor would be dropped right there, on whatever worker or runtime thread
/// abandoned the task, while the rest of the query may still be running. Before processors were
/// owned by the scheduled task they always lived until the graph was dropped, and processors
/// rely on that: dropping one early releases what it holds (channel senders, spill files, shared
/// state), which peers can observe as an unexpected disconnect and may report as an error before
/// the real cause is recorded. Returning the processor keeps that lifetime unchanged.
///
/// The node's state is left as `Processing`, so the node is never scheduled again and the
/// processor is only dropped together with the graph. Taking the slot lock here cannot deadlock:
/// wrappers are created while the scheduler holds that lock, but they are only moved into the
/// schedule queue there, never dropped.
impl Drop for ProcessorWrapper {
    fn drop(&mut self) {
        if let Some(processor) = self.processor.take() {
            self.graph.0.graph[self.node].lock_slot().processor = Some(processor);
        }
    }
}

/// Why a node is scheduled again.
pub enum Reschedule {
    /// The node finished executing and hands its processor back.
    Executed(ProcessorWrapper),
    /// The node was woken through its `ExecutorWaker`. Ignored unless the node is idle.
    Woken {
        node: NodeIndex,
        graph: Arc<RunningGraph>,
    },
}

impl Reschedule {
    pub fn node(&self) -> NodeIndex {
        match self {
            Reschedule::Executed(executed) => executed.node,
            Reschedule::Woken { node, .. } => *node,
        }
    }

    pub fn graph(&self) -> &Arc<RunningGraph> {
        match self {
            Reschedule::Executed(executed) => &executed.graph,
            Reschedule::Woken { graph, .. } => graph,
        }
    }
}

pub struct ScheduleQueue {
    pub sync_queue: VecDeque<ProcessorWrapper>,
    pub async_queue: VecDeque<ProcessorWrapper>,
}

impl ScheduleQueue {
    pub fn with_capacity(capacity: usize) -> ScheduleQueue {
        ScheduleQueue {
            sync_queue: VecDeque::with_capacity(capacity),
            async_queue: VecDeque::with_capacity(capacity),
        }
    }

    #[inline]
    pub fn push_sync(&mut self, processor: ProcessorWrapper) {
        self.sync_queue.push_back(processor);
    }

    #[inline]
    pub fn push_async(&mut self, processor: ProcessorWrapper) {
        self.async_queue.push_back(processor);
    }

    pub fn schedule(
        mut self,
        global: &Arc<QueryExecutorTasksQueue>,
        context: &mut ExecutorWorkerContext,
        executor: &Arc<QueryPipelineExecutor>,
    ) {
        debug_assert!(!context.has_task());

        while let Some(processor) = self.async_queue.pop_front() {
            let query_id = processor.graph().get_query_id().clone();
            Self::schedule_async_task(
                processor,
                query_id,
                executor,
                context.get_worker_id(),
                context.get_workers_condvar().clone(),
                global.clone(),
            )
        }

        if !self.sync_queue.is_empty() {
            self.schedule_sync(global, context);
        }

        if !self.sync_queue.is_empty() {
            self.schedule_tail(global, context);
        }
    }

    pub fn schedule_async_task(
        proc: ProcessorWrapper,
        query_id: Arc<String>,
        executor: &Arc<QueryPipelineExecutor>,
        wakeup_worker_id: usize,
        workers_condvar: Arc<WorkersCondvar>,
        global_queue: Arc<QueryExecutorTasksQueue>,
    ) {
        workers_condvar.inc_active_async_worker();
        let tracking_payload = proc.graph().get_node_tracking_payload(proc.node).clone();
        let _guard = ThreadTracker::tracking(tracking_payload.clone());
        let processor_task = ProcessorAsyncTask::create(
            query_id,
            wakeup_worker_id,
            proc,
            Arc::new(ExecutorTasksQueue::QueryExecutorTasksQueue(global_queue)),
            workers_condvar,
        );
        executor
            .async_runtime
            .spawn(tracking_payload.tracking(processor_task).in_span(
                Span::enter_with_local_parent(std::any::type_name::<ProcessorAsyncTask>()),
            ));
    }

    fn schedule_sync(&mut self, _: &QueryExecutorTasksQueue, ctx: &mut ExecutorWorkerContext) {
        if let Some(processor) = self.sync_queue.pop_front() {
            ctx.set_task(ExecutorTask::Sync(processor));
        }
    }

    pub fn schedule_tail(
        mut self,
        global: &QueryExecutorTasksQueue,
        ctx: &mut ExecutorWorkerContext,
    ) {
        let mut tasks = VecDeque::with_capacity(self.sync_queue.len());

        while let Some(processor) = self.sync_queue.pop_front() {
            tasks.push_back(ExecutorTask::Sync(processor));
        }

        global.push_tasks(ctx, tasks)
    }

    pub fn schedule_with_condition(
        mut self,
        global: &Arc<QueriesExecutorTasksQueue>,
        context: &mut ExecutorWorkerContext,
        executor: &Arc<QueriesPipelineExecutor>,
    ) {
        debug_assert!(!context.has_task());

        while let Some(processor) = self.async_queue.pop_front() {
            if processor
                .graph()
                .can_perform_task(executor.epoch.load(Ordering::SeqCst))
            {
                let query_id = processor.graph().get_query_id().clone();
                Self::schedule_async_task_with_condition(
                    processor,
                    query_id,
                    executor,
                    context.get_worker_id(),
                    context.get_workers_condvar().clone(),
                    global.clone(),
                )
            } else {
                let mut tasks = VecDeque::with_capacity(1);
                tasks.push_back(ExecutorTask::Async(processor));
                global.push_tasks(context.get_worker_id(), None, tasks);
            }
        }

        let mut tasks_to_global = VecDeque::with_capacity(self.sync_queue.len());

        if let Some(processor) = self.sync_queue.pop_front() {
            if processor
                .graph()
                .can_perform_task(executor.epoch.load(Ordering::SeqCst))
            {
                context.set_task(ExecutorTask::Sync(processor));
            } else {
                tasks_to_global.push_back(ExecutorTask::Sync(processor));
            }
        }

        // Add remaining tasks from sync queue to global queue
        while let Some(processor) = self.sync_queue.pop_front() {
            tasks_to_global.push_back(ExecutorTask::Sync(processor));
        }
        if !tasks_to_global.is_empty() {
            global.push_tasks(context.get_worker_id(), None, tasks_to_global);
        }
    }

    pub fn schedule_async_task_with_condition(
        proc: ProcessorWrapper,
        query_id: Arc<String>,
        executor: &Arc<QueriesPipelineExecutor>,
        wakeup_worker_id: usize,
        workers_condvar: Arc<WorkersCondvar>,
        global_queue: Arc<QueriesExecutorTasksQueue>,
    ) {
        workers_condvar.inc_active_async_worker();
        let tracking_payload = proc.graph().get_node_tracking_payload(proc.node).clone();
        let _guard = ThreadTracker::tracking(tracking_payload.clone());
        let processor_task = ProcessorAsyncTask::create(
            query_id,
            wakeup_worker_id,
            proc,
            Arc::new(ExecutorTasksQueue::QueriesExecutorTasksQueue(global_queue)),
            workers_condvar,
        );
        executor
            .async_runtime
            .spawn(tracking_payload.tracking(processor_task).in_span(
                Span::enter_with_local_parent(std::any::type_name::<ProcessorAsyncTask>()),
            ));
    }
}

pub struct RunningGraph(ExecutingGraph);

impl RunningGraph {
    pub fn create(
        pipeline: Pipeline,
        init_epoch: u32,
        query_id: Arc<String>,
        finish_condvar_notify: Option<Arc<(Mutex<bool>, Condvar)>>,
        perf_event_groups: Vec<Vec<PerfEvent>>,
    ) -> Result<Arc<RunningGraph>> {
        let graph_state = ExecutingGraph::create(
            pipeline,
            init_epoch,
            query_id,
            finish_condvar_notify,
            perf_event_groups,
        )?;
        debug!("Create running graph:{:?}", graph_state);
        Ok(Arc::new(RunningGraph(graph_state)))
    }

    pub fn from_pipelines(
        pipelines: Vec<Pipeline>,
        init_epoch: u32,
        query_id: Arc<String>,
        finish_condvar_notify: Option<Arc<(Mutex<bool>, Condvar)>>,
        perf_event_groups: Vec<Vec<PerfEvent>>,
    ) -> Result<Arc<RunningGraph>> {
        let graph_state = ExecutingGraph::from_pipelines(
            pipelines,
            init_epoch,
            query_id,
            finish_condvar_notify,
            perf_event_groups,
        )?;
        debug!("Create running graph:{:?}", graph_state);
        Ok(Arc::new(RunningGraph(graph_state)))
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn init_schedule_queue(self: Arc<Self>, capacity: usize) -> Result<ScheduleQueue> {
        unsafe { ExecutingGraph::init_schedule_queue(&self.0, capacity, &self) }
    }

    /// # Safety
    ///
    /// Method is thread unsafe and require thread safe call
    pub unsafe fn schedule_queue(
        self: &Arc<Self>,
        reschedule: Reschedule,
    ) -> Result<ScheduleQueue> {
        debug_assert!(Arc::ptr_eq(self, reschedule.graph()));
        let (node, processor) = match reschedule {
            Reschedule::Executed(mut executed) => (executed.node, executed.processor.take()),
            Reschedule::Woken { node, .. } => (node, None),
        };

        let mut schedule_queue = ScheduleQueue::with_capacity(0);
        unsafe {
            ExecutingGraph::schedule_queue(&self.0, node, processor, &mut schedule_queue, self)?;
        }
        Ok(schedule_queue)
    }

    pub(crate) fn get_node_tracking_payload(&self, pid: NodeIndex) -> &TrackingPayload {
        &self.0.graph[pid].tracking_payload
    }

    pub(crate) fn node_name(&self, pid: NodeIndex) -> &str {
        self.0.graph[pid].name()
    }

    /// Resolves when the node's in-flight `async_process` should be dropped, see
    /// `Processor::cancel_async_on_outputs_finished`. `None` if the node never cancels.
    pub(crate) fn node_async_cancelled(
        &self,
        pid: NodeIndex,
    ) -> Option<impl Future<Output = ()> + Send + '_> {
        let node = &self.0.graph[pid];
        node.cancel_async_on_outputs_finished
            .then(|| node.async_cancel.cancelled())
    }

    pub fn perf_event_groups(&self) -> &[Vec<PerfEvent>] {
        &self.0.perf_event_groups
    }

    pub fn get_proc_profiles(&self) -> Vec<Arc<Profile>> {
        self.0
            .graph
            .node_weights()
            .map(|x| {
                let new_profile = x.tracking_payload.profile.as_deref().cloned();
                Arc::new(new_profile.unwrap())
            })
            .collect::<Vec<_>>()
    }

    pub fn fetch_profiling(&self, node_id: Option<String>) -> HashMap<u32, PlanProfile> {
        let mut plans_profile: HashMap<u32, PlanProfile> = HashMap::<u32, PlanProfile>::new();

        for x in self.0.graph.node_weights() {
            let profile = x.tracking_payload.profile.as_deref().unwrap();

            if let Some(plan_id) = &profile.plan_id {
                match plans_profile.entry(*plan_id) {
                    Entry::Occupied(mut v) => {
                        let plan_profile = v.get_mut();
                        for index in 0..std::mem::variant_count::<ProfileStatisticsName>() {
                            plan_profile.statistics[index] +=
                                profile.statistics[index].fetch_min(0, Ordering::SeqCst);
                        }
                    }
                    Entry::Vacant(v) => {
                        let plan_profile = v.insert(PlanProfile::create(profile));

                        for index in 0..std::mem::variant_count::<ProfileStatisticsName>() {
                            plan_profile.statistics[index] +=
                                profile.statistics[index].fetch_min(0, Ordering::SeqCst);
                        }

                        let node_id = node_id.as_ref();
                        let metrics_registry = profile.metrics_registry.as_ref();
                        if let Some((id, metrics_registry)) = node_id.zip(metrics_registry) {
                            let Ok(metrics) = metrics_registry.dump_sample() else {
                                warn!("Dump {:?} plan metrics error", plan_profile.name);
                                continue;
                            };

                            plan_profile.add_metrics(id.clone(), metrics);
                        }
                    }
                };
            }
        }

        plans_profile
    }

    pub fn fetch_perf_counters(&self) -> NodePerfCounters {
        let mut by_plan: HashMap<u32, (String, HashMap<PerfEvent, PerfValue>)> = HashMap::new();

        for node in self.0.graph.node_weights() {
            let profile = node.tracking_payload.profile.as_deref().unwrap();
            let plan_id = match profile.plan_id {
                Some(id) => id,
                None => continue,
            };
            let plan_name = profile.plan_name.clone().unwrap_or_default();
            let counters = profile.perf_counters.lock();
            if counters.is_empty() {
                continue;
            }
            let entry = by_plan
                .entry(plan_id)
                .or_insert_with(|| (plan_name, HashMap::new()));
            for (event, pv) in counters.iter() {
                let e = entry.1.entry(*event).or_default();
                e.count += pv.count;
                e.multiplexed = e.multiplexed || pv.multiplexed;
            }
        }

        let mut counters: Vec<_> = by_plan
            .into_iter()
            .map(|(id, (name, c))| (format!("{} [#{}]", name, id), c))
            .collect();
        counters.sort_by_key(|(name, _)| name.clone());
        NodePerfCounters { counters }
    }

    pub fn interrupt(&self) {
        self.0.should_finish.store(true, Ordering::SeqCst);
    }

    pub fn assert_finished_graph(&self) -> Result<()> {
        let finished_nodes = self.0.finished_nodes.load(Ordering::SeqCst);

        match finished_nodes >= self.0.graph.node_count() {
            true => Ok(()),
            false => Err(ErrorCode::Internal(format!(
                "Pipeline graph is not finished, details: {}",
                self.format_graph_nodes(true)
            ))),
        }
    }

    /// Checks if all nodes in the graph are finished.
    pub fn is_all_nodes_finished(&self) -> bool {
        self.0.finished_nodes.load(Ordering::SeqCst) >= self.0.graph.node_count()
    }

    /// Flag the graph should finish and no more tasks should be scheduled.
    pub fn should_finish<C>(&self, cause: Result<(), C>) -> Result<()> {
        let cause = cause.with_context(|| "should finish");

        if self.0.should_finish.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.0.should_finish.store(true, Ordering::SeqCst);
        if let Err(cause) = &cause {
            log_memory_limit_diagnostics(cause, "memory limit exceeded");
        }
        self.0.finished_notify.notify_waiters();
        self.interrupt();
        let mut finished_error = self.0.finished_error.lock();
        if finished_error.is_none() {
            *finished_error = cause.err();
            drop(finished_error);
        }

        if let Some(notify) = self.0.finish_condvar_notify.clone() {
            let (lock, cvar) = &*notify;
            let mut started = lock.lock();
            *started = true;
            cvar.notify_one();
        }
        Ok(())
    }

    /// Checks if the graph should finish and no more tasks should be scheduled.
    pub fn is_should_finish(&self) -> bool {
        self.0.should_finish.load(Ordering::SeqCst)
    }

    /// Checks if a task can be performed in the current epoch, consuming a point if possible.
    pub fn can_perform_task(&self, global_epoch: u32) -> bool {
        self.0.can_perform_task(global_epoch)
    }

    pub fn get_query_id(&self) -> Arc<String> {
        self.0.query_id.clone()
    }

    pub fn get_waker(&self) -> Arc<ExecutorWaker> {
        self.0.waker.clone()
    }

    pub fn get_error(&self) -> Option<ErrorCode> {
        let finished_error = self.0.finished_error.lock();
        finished_error.clone()
    }

    pub fn record_node_error(&self, node_index: NodeIndex, error: NodeErrorType) {
        self.0.graph[node_index].record_error(error);
    }

    pub fn get_points(&self) -> u64 {
        self.0.points.load(Ordering::SeqCst)
    }

    pub fn get_finished_notify(&self) -> Arc<WatchNotify> {
        self.0.finished_notify.clone()
    }

    pub fn format_graph_nodes(&self, pretty: bool) -> String {
        pub struct NodeDisplay {
            id: usize,
            name: String,
            state: String,
            details_status: Option<String>,
            inputs_status: Vec<(&'static str, &'static str, &'static str)>,
            outputs_status: Vec<(&'static str, &'static str, &'static str)>,
        }

        impl Debug for NodeDisplay {
            fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
                match &self.details_status {
                    None => f
                        .debug_struct("Node")
                        .field("name", &self.name)
                        .field("id", &self.id)
                        .field("state", &self.state)
                        .field("inputs_status", &self.inputs_status)
                        .field("outputs_status", &self.outputs_status)
                        .finish(),
                    Some(details_status) => f
                        .debug_struct("Node")
                        .field("name", &self.name)
                        .field("id", &self.id)
                        .field("state", &self.state)
                        .field("inputs_status", &self.inputs_status)
                        .field("outputs_status", &self.outputs_status)
                        .field("details", details_status)
                        .finish(),
                }
            }
        }

        let mut nodes_display = Vec::with_capacity(self.0.graph.node_count());

        for node_index in self.0.graph.node_indices() {
            let node = &self.0.graph[node_index];
            let slot = node.lock_slot();
            // A running processor is owned by its worker, so only idle or finished ones report.
            let details_status = slot.processor.as_ref().and_then(|x| x.details_status());

            let inputs_status = node
                .inputs_port
                .iter()
                .map(|x| {
                    let finished = match x.is_finished() {
                        true => "Finished",
                        false => "Unfinished",
                    };

                    let has_data = match x.has_data() {
                        true => "HasData",
                        false => "Nodata",
                    };

                    let need_data = match x.is_need_data() {
                        true => "NeedData",
                        false => "UnNeeded",
                    };

                    (finished, has_data, need_data)
                })
                .collect::<Vec<_>>();

            let outputs_status = node
                .outputs_port
                .iter()
                .map(|x| {
                    let finished = match x.is_finished() {
                        true => "Finished",
                        false => "Unfinished",
                    };

                    let has_data = match x.has_data() {
                        true => "HasData",
                        false => "Nodata",
                    };

                    let need_data = match x.is_need_data() {
                        true => "NeedData",
                        false => "UnNeeded",
                    };

                    (finished, has_data, need_data)
                })
                .collect::<Vec<_>>();

            nodes_display.push(NodeDisplay {
                inputs_status,
                outputs_status,
                id: node_index.index(),
                name: node.name().to_string(),
                details_status,
                state: String::from(match slot.state {
                    State::Idle => "Idle",
                    State::Processing => "Processing",
                    State::Finished => "Finished",
                }),
            });
        }

        if pretty {
            format!("{:#?}", nodes_display)
        } else {
            format!("{:?}", nodes_display)
        }
    }

    pub fn top_memory_plan_nodes(&self, limit: usize) -> Vec<PlanNodeMemoryUsage> {
        let mut usages = HashMap::new();

        for node_index in self.0.graph.node_indices() {
            let node = &self.0.graph[node_index];
            let Some(mem_stat) = node.tracking_payload.mem_stat.as_ref() else {
                continue;
            };
            let Some(profile) = node.tracking_payload.profile.as_ref() else {
                continue;
            };
            let Some(plan_id) = profile.plan_id else {
                continue;
            };

            if let Entry::Vacant(entry) = usages.entry(plan_id) {
                entry.insert(PlanNodeMemoryUsage {
                    identity: plan_node_memory_identity(profile),
                    current_bytes: mem_stat.get_memory_usage(),
                    peak_bytes: std::cmp::max(0, mem_stat.get_peak_memory_usage()) as usize,
                });
            }
        }

        let mut usages = usages.into_iter().collect::<Vec<_>>();
        usages.sort_by(|left, right| {
            right
                .1
                .current_bytes
                .cmp(&left.1.current_bytes)
                .then(right.1.peak_bytes.cmp(&left.1.peak_bytes))
                .then(left.0.cmp(&right.0))
        });
        usages
            .into_iter()
            .take(limit)
            .map(|(_, usage)| usage)
            .collect::<Vec<_>>()
    }

    pub fn format_top_memory_plan_nodes(&self, limit: usize) -> String {
        format!("{:?}", self.top_memory_plan_nodes(limit))
    }

    /// Change the priority
    pub fn change_priority(&self, priority: u64) {
        self.0.max_points.store(priority, Ordering::SeqCst);
    }

    pub fn record_process(&self, begin: SystemTime, elapsed_micros: usize, rows: usize) {
        self.0
            .executor_stats
            .record_process(begin, elapsed_micros, rows);
    }

    pub fn get_query_execution_stats(&self) -> ExecutorStatsSnapshot {
        self.0.executor_stats.dump_snapshot()
    }
}

impl Drop for RunningGraph {
    fn drop(&mut self) {
        let execution_stats = self.get_query_execution_stats();
        if let Ok(queue) = QueryExecutionStatsQueue::instance() {
            let _ = queue.append_data((self.get_query_id().to_string(), execution_stats));
        }
    }
}

impl Debug for Node {
    fn fmt(&self, f: &mut Formatter) -> core::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl Debug for ExecutingGraph {
    fn fmt(&self, f: &mut Formatter) -> core::fmt::Result {
        write!(
            f,
            "{:?}",
            Dot::with_attr_getters(
                &self.graph,
                &[Config::EdgeNoLabel],
                &|_, edge| format!(
                    "{} -> {}",
                    edge.weight().output_index,
                    edge.weight().input_index
                ),
                &|_, (_, _)| String::new(),
            )
        )
    }
}

impl Debug for RunningGraph {
    fn fmt(&self, f: &mut Formatter) -> core::fmt::Result {
        // let graph = self.0.read();
        write!(f, "{:?}", self.0)
    }
}

impl Debug for ScheduleQueue {
    fn fmt(&self, f: &mut Formatter) -> core::fmt::Result {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct QueueItem {
            id: usize,
            name: String,
        }

        let queue_items = |queue: &VecDeque<ProcessorWrapper>| {
            queue
                .iter()
                .map(|item| QueueItem {
                    id: item.node.index(),
                    name: item.graph.node_name(item.node).to_string(),
                })
                .collect::<Vec<_>>()
        };

        f.debug_struct("ScheduleQueue")
            .field("sync_queue", &queue_items(&self.sync_queue))
            .field("async_queue", &queue_items(&self.async_queue))
            .finish()
    }
}
