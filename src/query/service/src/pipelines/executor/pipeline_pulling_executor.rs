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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::runtime::drop_guard;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::Thread;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayload;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::ExecutionInfo;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sinks::Sink;
use databend_common_pipeline_sinks::Sinker;
use fastrace::full_name;
use fastrace::prelude::*;
use parking_lot::Condvar;
use parking_lot::Mutex;

use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineExecutor;
use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::ProcessorPtr;
use crate::pipelines::PipelineBuildResult;

struct State {
    is_finished: AtomicBool,
    finish_mutex: Mutex<bool>,
    finish_condvar: Condvar,

    catch_error: Mutex<Option<ErrorCode>>,
}

impl State {
    pub fn create() -> Arc<State> {
        Arc::new(State {
            catch_error: Mutex::new(None),
            is_finished: AtomicBool::new(false),
            finish_mutex: Mutex::new(false),
            finish_condvar: Condvar::new(),
        })
    }

    pub fn finished(&self, message: Result<()>) {
        self.is_finished.store(true, Ordering::Release);

        if let Err(error) = message {
            *self.catch_error.lock() = Some(error);
        }

        {
            let mut mutex = self.finish_mutex.lock();
            *mutex = true;
            self.finish_condvar.notify_one();
        }
    }

    pub fn wait_finish(&self) {
        let mut mutex = self.finish_mutex.lock();

        while !*mutex {
            self.finish_condvar.wait(&mut mutex);
        }
    }

    pub fn is_finished(&self) -> bool {
        self.is_finished.load(Ordering::Relaxed)
    }

    pub fn try_get_catch_error(&self) -> Option<ErrorCode> {
        self.catch_error.lock().as_ref().cloned()
    }
}

// Use this executor when the pipeline is pulling pipeline (exists source but not exists sink)
pub struct PipelinePullingExecutor {
    state: Arc<State>,
    executor: Arc<PipelineExecutor>,
    receiver: Receiver<DataBlock>,
    tracking_payload: TrackingPayload,
}

impl PipelinePullingExecutor {
    fn execution_tracking_payload(query_id: &str) -> TrackingPayload {
        let mut tracking_payload = ThreadTracker::new_tracking_payload();
        tracking_payload.mem_stat = Some(MemStat::create(format!(
            "QueryExecutionMemStat-{}",
            query_id
        )));
        tracking_payload
    }

    fn wrap_pipeline(
        pipeline: &mut Pipeline,
        tx: SyncSender<DataBlock>,
        mem_stat: Arc<MemStat>,
    ) -> Result<()> {
        if pipeline.is_pushing_pipeline()? || !pipeline.is_pulling_pipeline()? {
            return Err(ErrorCode::Internal(
                "Logical error, PipelinePullingExecutor can only work on pulling pipeline.",
            ));
        }

        pipeline.add_sink(|input| {
            Ok(ProcessorPtr::create(PullingSink::create(
                tx.clone(),
                mem_stat.clone(),
                input,
            )))
        })?;

        pipeline.set_on_finished(move |_info: &ExecutionInfo| {
            drop(tx);
            Ok(())
        });

        Ok(())
    }

    pub fn try_create(
        mut pipeline: Pipeline,
        settings: ExecutorSettings,
    ) -> Result<PipelinePullingExecutor> {
        let tracking_payload = Self::execution_tracking_payload(settings.query_id.as_ref());
        let _guard = ThreadTracker::tracking(tracking_payload.clone());

        let (sender, receiver) = std::sync::mpsc::sync_channel(pipeline.output_len());

        Self::wrap_pipeline(
            &mut pipeline,
            sender,
            tracking_payload.mem_stat.clone().unwrap(),
        )?;
        let executor = PipelineExecutor::create(pipeline, settings)?;

        Ok(PipelinePullingExecutor {
            receiver,
            executor: Arc::new(executor),
            state: State::create(),
            tracking_payload,
        })
    }

    pub fn from_pipelines(
        build_res: PipelineBuildResult,
        settings: ExecutorSettings,
    ) -> Result<PipelinePullingExecutor> {
        let tracking_payload = Self::execution_tracking_payload(settings.query_id.as_ref());
        let _guard = ThreadTracker::tracking(tracking_payload.clone());

        let mut main_pipeline = build_res.main_pipeline;
        let (sender, receiver) = std::sync::mpsc::sync_channel(main_pipeline.output_len());

        Self::wrap_pipeline(
            &mut main_pipeline,
            sender,
            tracking_payload.mem_stat.clone().unwrap(),
        )?;

        let mut pipelines = build_res.sources_pipelines;
        pipelines.push(main_pipeline);
        let executor = PipelineExecutor::from_pipelines(pipelines, settings)?;
        Ok(PipelinePullingExecutor {
            receiver,
            state: State::create(),
            tracking_payload,
            executor: Arc::new(executor),
        })
    }

    #[fastrace::trace]
    pub fn start(&mut self) {
        let _guard = ThreadTracker::tracking(self.tracking_payload.clone());

        let state = self.state.clone();
        let threads_executor = self.executor.clone();
        let thread_function = Self::thread_function(state, threads_executor);
        #[allow(unused_mut)]
        let mut thread_name = Some(String::from("PullingExecutor"));

        #[cfg(debug_assertions)]
        {
            // We need to pass the thread name in the unit test, because the thread name is the test name
            if matches!(std::env::var("UNIT_TEST"), Ok(var_value) if var_value == "TRUE") {
                if let Some(cur_thread_name) = std::thread::current().name() {
                    thread_name = Some(cur_thread_name.to_string());
                }
            }
        }

        Thread::named_spawn(thread_name, thread_function);
    }

    pub fn get_inner(&self) -> Arc<PipelineExecutor> {
        self.executor.clone()
    }

    fn thread_function(state: Arc<State>, executor: Arc<PipelineExecutor>) -> impl Fn() {
        let span = Span::enter_with_local_parent(full_name!());
        move || {
            let _g = span.set_local_parent();
            state.finished(executor.execute());
        }
    }

    pub fn finish(&self, cause: Option<ErrorCode>) {
        let _guard = ThreadTracker::tracking(self.tracking_payload.clone());

        self.executor.finish(cause);
    }

    pub fn pull_data(&mut self) -> Result<Option<DataBlock>> {
        let mut need_check_graph_status = false;

        loop {
            return match self.receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(data_block) => Ok(Some(data_block)),
                Err(RecvTimeoutError::Timeout) => {
                    if self.state.is_finished() {
                        if let Some(error) = self.state.try_get_catch_error() {
                            return Err(error);
                        }

                        // It may be parallel. Let's check again.
                        if !need_check_graph_status {
                            need_check_graph_status = true;
                            self.state.wait_finish();
                            continue;
                        }

                        return Err(ErrorCode::Internal(format!(
                            "Processor graph not completed. graph nodes state: {}",
                            self.executor.format_graph_nodes()
                        )));
                    }

                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => {
                    if !self.executor.is_finished() {
                        self.executor.finish(None);
                    }

                    self.state.wait_finish();

                    return match self.state.try_get_catch_error() {
                        None => Ok(None),
                        Some(error) => Err(error),
                    };
                }
            };
        }
    }
}

impl Drop for PipelinePullingExecutor {
    fn drop(&mut self) {
        drop_guard(move || {
            let _guard = ThreadTracker::tracking(self.tracking_payload.clone());

            self.finish(None);
        })
    }
}

struct PullingSink {
    sender: Option<SyncSender<DataBlock>>,
    query_execution_mem_stat: Arc<MemStat>,
}

impl PullingSink {
    pub fn create(
        tx: SyncSender<DataBlock>,
        mem_stat: Arc<MemStat>,
        input: Arc<InputPort>,
    ) -> Box<dyn Processor> {
        Sinker::create(input, PullingSink {
            sender: Some(tx),
            query_execution_mem_stat: mem_stat,
        })
    }
}

impl Sink for PullingSink {
    const NAME: &'static str = "PullingExecutorSink";

    fn on_finish(&mut self) -> Result<()> {
        drop(self.sender.take());
        Ok(())
    }

    fn consume(&mut self, data_block: DataBlock) -> Result<()> {
        let memory_size = data_block.memory_size() as i64;
        // TODO: need moveout memory for plan tracker
        ThreadTracker::moveout_memory(memory_size);

        self.query_execution_mem_stat.moveout_memory(memory_size);

        if let Some(sender) = &self.sender {
            if let Err(cause) = sender.send(data_block) {
                return Err(ErrorCode::Internal(format!(
                    "Logical error, cannot push data into SyncSender, cause {:?}",
                    cause
                )));
            }
        }

        Ok(())
    }
}
