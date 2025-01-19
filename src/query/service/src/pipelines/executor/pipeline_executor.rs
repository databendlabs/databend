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

use databend_common_base::base::WatchNotify;
use databend_common_base::runtime::catch_unwind;
use databend_common_base::runtime::defer;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline_core::ExecutionInfo;
use databend_common_pipeline_core::FinishedCallbackChain;
use databend_common_pipeline_core::LockGuard;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_core::PlanProfile;
use futures_util::future::select;
use futures_util::future::Either;
use log::info;
use parking_lot::Condvar;
use parking_lot::Mutex;
use tokio::sync::oneshot::error::RecvError;

use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::GlobalQueriesExecutor;
use crate::pipelines::executor::QueryPipelineExecutor;
use crate::pipelines::executor::RunningGraph;

pub type InitCallback = Box<dyn FnOnce() -> Result<()> + Send + Sync + 'static>;

pub struct QueryWrapper {
    graph: Arc<RunningGraph>,
    settings: ExecutorSettings,
    on_init_callback: Mutex<Option<InitCallback>>,
    on_finished_chain: Mutex<FinishedCallbackChain>,
    #[allow(unused)]
    lock_guards: Vec<Arc<LockGuard>>,
    finish_condvar_wait: Arc<(Mutex<bool>, Condvar)>,
    finished_notify: Arc<WatchNotify>,
}

pub enum PipelineExecutor {
    QueryPipelineExecutor(Arc<QueryPipelineExecutor>),
    QueriesPipelineExecutor(QueryWrapper),
}

impl PipelineExecutor {
    pub fn create(mut pipeline: Pipeline, settings: ExecutorSettings) -> Result<Self> {
        if !settings.enable_queries_executor {
            Ok(PipelineExecutor::QueryPipelineExecutor(
                QueryPipelineExecutor::create(pipeline, settings)?,
            ))
        } else {
            let on_init_callback = Some(pipeline.take_on_init());
            let on_finished_chain = pipeline.take_on_finished();

            let finish_condvar = Arc::new((Mutex::new(false), Condvar::new()));

            let lock_guards = pipeline.take_lock_guards();

            let graph = RunningGraph::create(
                pipeline,
                1,
                settings.query_id.clone(),
                Some(finish_condvar.clone()),
            )?;

            Ok(PipelineExecutor::QueriesPipelineExecutor(QueryWrapper {
                graph,
                settings,
                on_init_callback: Mutex::new(on_init_callback),
                on_finished_chain: Mutex::new(on_finished_chain),
                lock_guards,
                finish_condvar_wait: finish_condvar,
                finished_notify: Arc::new(WatchNotify::new()),
            }))
        }
    }

    pub fn from_pipelines(
        mut pipelines: Vec<Pipeline>,
        settings: ExecutorSettings,
    ) -> Result<Self> {
        if !settings.enable_queries_executor {
            Ok(PipelineExecutor::QueryPipelineExecutor(
                QueryPipelineExecutor::from_pipelines(pipelines, settings)?,
            ))
        } else {
            let on_init_callback = {
                let pipelines_callback = pipelines
                    .iter_mut()
                    .map(|x| x.take_on_init())
                    .collect::<Vec<_>>();

                pipelines_callback.into_iter().reduce(|left, right| {
                    Box::new(move || {
                        left()?;
                        right()
                    })
                })
            };

            let mut on_finished_chain = FinishedCallbackChain::create();

            for pipeline in &mut pipelines {
                on_finished_chain.extend(pipeline.take_on_finished());
            }

            let finish_condvar = Arc::new((Mutex::new(false), Condvar::new()));

            let lock_guards = pipelines
                .iter_mut()
                .flat_map(|x| x.take_lock_guards())
                .collect::<Vec<_>>();

            let graph = RunningGraph::from_pipelines(
                pipelines,
                1,
                settings.query_id.clone(),
                Some(finish_condvar.clone()),
            )?;

            Ok(PipelineExecutor::QueriesPipelineExecutor(QueryWrapper {
                graph,
                settings,
                on_init_callback: Mutex::new(on_init_callback),
                on_finished_chain: Mutex::new(on_finished_chain),
                lock_guards,
                finish_condvar_wait: finish_condvar,
                finished_notify: Arc::new(WatchNotify::new()),
            }))
        }
    }

    fn init(on_init_callback: &Mutex<Option<InitCallback>>, query_id: &Arc<String>) -> Result<()> {
        // TODO: the on init callback cannot be killed.
        {
            let instant = Instant::now();
            let mut guard = on_init_callback.lock();
            if let Some(callback) = guard.take() {
                drop(guard);
                if let Err(cause) = Result::flatten(catch_unwind(callback)) {
                    return Err(cause.add_message_back("(while in query pipeline init)"));
                }
            }

            info!(
                "Init pipeline successfully, query_id: {:?}, elapsed: {:?}",
                query_id,
                instant.elapsed()
            );
        }
        Ok(())
    }

    pub fn execute(&self) -> Result<()> {
        let instants = Instant::now();
        let _guard = defer(move || {
            info!(
                "Pipeline executor finished, elapsed: {:?}",
                instants.elapsed()
            );
        });
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.execute(),
            PipelineExecutor::QueriesPipelineExecutor(query_wrapper) => {
                Self::init(
                    &query_wrapper.on_init_callback,
                    &query_wrapper.settings.query_id,
                )?;
                GlobalQueriesExecutor::instance().send_graph(query_wrapper.graph.clone())?;
                Self::start_executor_daemon(
                    query_wrapper,
                    query_wrapper.settings.max_execute_time_in_seconds,
                )?;
                let (lock, cvar) = &*query_wrapper.finish_condvar_wait;
                let mut finished = lock.lock();
                if !*finished {
                    cvar.wait(&mut finished);
                }

                query_wrapper.finished_notify.notify_waiters();

                let may_error = query_wrapper.graph.get_error();
                match may_error {
                    None => {
                        let mut finished_chain = query_wrapper.on_finished_chain.lock();
                        let info = ExecutionInfo::create(Ok(()), self.fetch_profiling(true));
                        finished_chain.apply(info)
                    }
                    Some(cause) => {
                        let mut finished_chain = query_wrapper.on_finished_chain.lock();
                        let profiling = self.fetch_profiling(true);
                        let info = ExecutionInfo::create(Err(cause.clone()), profiling);
                        finished_chain.apply(info).and_then(|_| Err(cause))
                    }
                }
            }
        }
    }

    fn start_executor_daemon(
        query_wrapper: &QueryWrapper,
        max_execute_time_in_seconds: Duration,
    ) -> Result<()> {
        if !max_execute_time_in_seconds.is_zero() {
            let this_graph = Arc::downgrade(&query_wrapper.graph);
            let finished_notify = query_wrapper.finished_notify.clone();
            GlobalIORuntime::instance().spawn(async move {
                let finished_future = Box::pin(finished_notify.notified());
                let max_execute_future = Box::pin(tokio::time::sleep(max_execute_time_in_seconds));
                if let Either::Left(_) = select(max_execute_future, finished_future).await {
                    if let Some(graph) = this_graph.upgrade() {
                        graph.should_finish(Err(ErrorCode::AbortedQuery(
                            "Aborted query, because the execution time exceeds the maximum execution time limit",
                        ))).expect("exceed max execute time, but cannot send error message");
                    }
                }
            });
        }

        Ok(())
    }

    pub fn finish<C>(&self, cause: Option<ErrorCode<C>>) {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.finish(cause),
            PipelineExecutor::QueriesPipelineExecutor(query_wrapper) => match cause {
                Some(may_error) => {
                    query_wrapper
                        .graph
                        .should_finish(Err(may_error))
                        .expect("executor cannot send error message");
                }
                None => {
                    query_wrapper
                        .graph
                        .should_finish::<()>(Ok(()))
                        .expect("executor cannot send error message");
                }
            },
        }
    }

    pub fn is_finished(&self) -> bool {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.is_finished(),
            PipelineExecutor::QueriesPipelineExecutor(query_wrapper) => {
                query_wrapper.graph.is_should_finish()
            }
        }
    }

    pub fn format_graph_nodes(&self) -> String {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => executor.format_graph_nodes(),
            PipelineExecutor::QueriesPipelineExecutor(v) => v.graph.format_graph_nodes(),
        }
    }

    pub fn fetch_profiling(&self, collect_metrics: bool) -> HashMap<u32, PlanProfile> {
        match self {
            PipelineExecutor::QueryPipelineExecutor(executor) => {
                executor.fetch_plans_profile(collect_metrics)
            }
            PipelineExecutor::QueriesPipelineExecutor(v) => match collect_metrics {
                true => v
                    .graph
                    .fetch_profiling(Some(v.settings.executor_node_id.clone())),
                false => v.graph.fetch_profiling(None),
            },
        }
    }

    pub fn change_priority(&self, priority: u8) {
        match self {
            PipelineExecutor::QueryPipelineExecutor(_) => {
                unreachable!("Logic error, cannot change priority for QueryPipelineExecutor")
            }
            PipelineExecutor::QueriesPipelineExecutor(query_wrapper) => {
                query_wrapper.graph.change_priority(priority as u64);
            }
        }
    }
}

#[async_trait::async_trait]
pub trait QueryHandle: Send + Sync + 'static {
    async fn wait(&self) -> Result<()>;

    fn is_finished(&self) -> bool;

    fn finish(&self, cause: Option<ErrorCode<()>>);

    fn fetch_profiling(&self, collect_metrics: bool) -> HashMap<u32, PlanProfile>;
}

#[async_trait::async_trait]
pub trait NewPipelineExecutor {
    async fn submit(
        &self,
        pipelines: Vec<Pipeline>,
        settings: ExecutorSettings,
    ) -> Result<Arc<dyn QueryHandle>>;
}

pub struct QueryTask {
    pub graph: Arc<RunningGraph>,
    pub settings: ExecutorSettings,
    pub on_init_callback: InitCallback,
    pub on_finished_callback: FinishedCallbackChain,
    pub holds: Vec<Arc<LockGuard>>,
    pub max_threads_num: usize,
    pub tx: Option<tokio::sync::oneshot::Sender<Result<Arc<dyn QueryHandle>>>>,
}

impl QueryTask {
    fn pipelines_max_threads(pipelines: &[Pipeline]) -> usize {
        pipelines
            .iter()
            .map(|x| x.get_max_threads())
            .max()
            .unwrap_or(0)
    }

    fn pipelines_on_init(pipelines: &mut [Pipeline]) -> InitCallback {
        let mut on_init_functions = Vec::with_capacity(pipelines.len());

        for pipeline in pipelines {
            on_init_functions.push(pipeline.take_on_init());
        }

        Box::new(move || {
            for on_init_function in on_init_functions {
                on_init_function()?;
            }

            Ok(())
        })
    }

    fn pipelines_on_finish(pipelines: &mut [Pipeline]) -> FinishedCallbackChain {
        let mut finished_chain = FinishedCallbackChain::create();
        for pipeline in pipelines {
            finished_chain.extend(pipeline.take_on_finished());
        }

        finished_chain
    }

    fn pipelines_hold(pipelines: &mut [Pipeline]) -> Vec<Arc<LockGuard>> {
        let mut holds = Vec::with_capacity(pipelines.len());
        for pipeline in pipelines {
            holds.extend(pipeline.take_lock_guards());
        }

        holds
    }

    pub fn try_create(
        mut pipelines: Vec<Pipeline>,
        tx: tokio::sync::oneshot::Sender<Result<Arc<dyn QueryHandle>>>,
        settings: ExecutorSettings,
    ) -> Result<QueryTask> {
        let on_init_callback = Self::pipelines_on_init(&mut pipelines);
        let mut on_finished_callback = Self::pipelines_on_finish(&mut pipelines);
        let holds = Self::pipelines_hold(&mut pipelines);

        if pipelines.is_empty() {
            let cause = ErrorCode::Internal("Executor Pipelines is empty.");
            let info = ExecutionInfo::create(Err(cause.clone()), HashMap::new());
            let _ignore = on_finished_callback.apply(info);
            return Err(cause);
        }

        let threads_num = Self::pipelines_max_threads(&pipelines);

        if threads_num == 0 {
            let cause = ErrorCode::Internal("Pipeline max threads cannot equals zero.");
            let info = ExecutionInfo::create(Err(cause.clone()), HashMap::new());
            let _ignore = on_finished_callback.apply(info);
            return Err(cause);
        }

        let query_id = settings.query_id.clone();

        let running_graph = match RunningGraph::from_pipelines(pipelines, 1, query_id, None) {
            Ok(running_graph) => running_graph,
            Err(cause) => {
                let info = ExecutionInfo::create(Err(cause.clone()), HashMap::new());
                let _ignore_res = on_finished_callback.apply(info);
                return Err(cause);
            }
        };

        Ok(QueryTask {
            settings,
            tx: Some(tx),
            on_init_callback,
            on_finished_callback,
            holds,
            graph: running_graph,
            max_threads_num: 0,
        })
    }
}
