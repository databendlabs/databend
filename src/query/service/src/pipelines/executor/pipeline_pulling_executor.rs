// Copyright 2022 Datafuse Labs.
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

use common_base::base::Thread;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::SinkPipeBuilder;
use parking_lot::Condvar;
use parking_lot::Mutex;

use crate::pipelines::executor::executor_settings::ExecutorSettings;
use crate::pipelines::executor::PipelineExecutor;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Sink;
use crate::pipelines::processors::Sinker;
use crate::pipelines::Pipeline;
use crate::pipelines::PipelineBuildResult;

struct State {
    is_catch_error: AtomicBool,
    finish_mutex: Mutex<bool>,
    finish_condvar: Condvar,

    catch_error: Mutex<Option<ErrorCode>>,
}

impl State {
    pub fn create() -> Arc<State> {
        Arc::new(State {
            catch_error: Mutex::new(None),
            is_catch_error: AtomicBool::new(false),
            finish_mutex: Mutex::new(false),
            finish_condvar: Condvar::new(),
        })
    }

    pub fn finished(&self, message: Result<()>) {
        if let Err(error) = message {
            self.is_catch_error.store(true, Ordering::Release);
            *self.catch_error.lock() = Some(error);
        }

        let mut mutex = self.finish_mutex.lock();
        *mutex = true;
        self.finish_condvar.notify_one();
    }

    pub fn wait_finish(&self) {
        let mut mutex = self.finish_mutex.lock();

        while !*mutex {
            self.finish_condvar.wait(&mut mutex);
        }
    }

    pub fn is_catch_error(&self) -> bool {
        self.is_catch_error.load(Ordering::Relaxed)
    }

    pub fn get_catch_error(&self) -> ErrorCode {
        let catch_error = self.catch_error.lock();

        match catch_error.as_ref() {
            None => ErrorCode::LogicalError("It's a bug."),
            Some(catch_error) => catch_error.clone(),
        }
    }
}

// Use this executor when the pipeline is pulling pipeline (exists source but not exists sink)
pub struct PipelinePullingExecutor {
    state: Arc<State>,
    executor: Arc<PipelineExecutor>,
    receiver: Receiver<DataBlock>,
}

impl PipelinePullingExecutor {
    fn wrap_pipeline(pipeline: &mut Pipeline, tx: SyncSender<DataBlock>) -> Result<()> {
        if pipeline.is_pushing_pipeline()? || !pipeline.is_pulling_pipeline()? {
            return Err(ErrorCode::LogicalError(
                "Logical error, PipelinePullingExecutor can only work on pulling pipeline.",
            ));
        }

        // pipeline.resize(1)?;
        let mut sink_pipe_builder = SinkPipeBuilder::create();

        for _index in 0..pipeline.output_len() {
            let input = InputPort::create();
            sink_pipe_builder.add_sink(input.clone(), PullingSink::create(tx.clone(), input));
        }

        pipeline.add_pipe(sink_pipe_builder.finalize());
        Ok(())
    }

    pub fn try_create(
        query_need_abort: Arc<AtomicBool>,
        mut pipeline: Pipeline,
        settings: ExecutorSettings,
    ) -> Result<PipelinePullingExecutor> {
        let (sender, receiver) = std::sync::mpsc::sync_channel(pipeline.output_len());

        Self::wrap_pipeline(&mut pipeline, sender)?;
        let executor = PipelineExecutor::create(query_need_abort, pipeline, settings)?;
        Ok(PipelinePullingExecutor {
            receiver,
            executor,
            state: State::create(),
        })
    }

    pub fn from_pipelines(
        query_need_abort: Arc<AtomicBool>,
        build_res: PipelineBuildResult,
        settings: ExecutorSettings,
    ) -> Result<PipelinePullingExecutor> {
        let mut main_pipeline = build_res.main_pipeline;
        let (sender, receiver) = std::sync::mpsc::sync_channel(main_pipeline.output_len());
        Self::wrap_pipeline(&mut main_pipeline, sender)?;

        let mut pipelines = build_res.sources_pipelines;
        pipelines.push(main_pipeline);

        Ok(PipelinePullingExecutor {
            receiver,
            state: State::create(),
            executor: PipelineExecutor::from_pipelines(query_need_abort, pipelines, settings)?,
        })
    }

    pub fn start(&mut self) {
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
        move || {
            state.finished(executor.execute());
        }
    }

    pub fn finish(&self) -> Result<()> {
        self.executor.finish()
    }

    pub fn pull_data(&mut self) -> Result<Option<DataBlock>> {
        loop {
            return match self.receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(data_block) => Ok(Some(data_block)),
                Err(RecvTimeoutError::Timeout) => {
                    if self.state.is_catch_error() {
                        return Err(self.state.get_catch_error());
                    }

                    continue;
                }
                Err(_disconnected) => {
                    if !self.executor.is_finished() {
                        self.executor.finish()?;
                    }

                    self.state.wait_finish();

                    if self.state.is_catch_error() {
                        return Err(self.state.get_catch_error());
                    }

                    Ok(None)
                }
            };
        }
    }
}

impl Drop for PipelinePullingExecutor {
    fn drop(&mut self) {
        if let Err(cause) = self.finish() {
            tracing::warn!("Executor finish is failure {:?}", cause);
        }
    }
}

struct PullingSink {
    sender: Option<SyncSender<DataBlock>>,
}

impl PullingSink {
    pub fn create(tx: SyncSender<DataBlock>, input: Arc<InputPort>) -> ProcessorPtr {
        Sinker::create(input, PullingSink { sender: Some(tx) })
    }
}

impl Sink for PullingSink {
    const NAME: &'static str = "PullingExecutorSink";

    fn on_finish(&mut self) -> Result<()> {
        drop(self.sender.take());
        Ok(())
    }

    fn consume(&mut self, data_block: DataBlock) -> Result<()> {
        if let Some(sender) = &self.sender {
            if let Err(cause) = sender.send(data_block) {
                return Err(ErrorCode::LogicalError(format!(
                    "Logical error, cannot push data into SyncSender, cause {:?}",
                    cause
                )));
            }
        }

        Ok(())
    }
}
