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
use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::time::Duration;

use common_base::base::Runtime;
use common_base::base::Thread;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::pipelines::executor::executor_settings::ExecutorSettings;
use crate::pipelines::executor::PipelineExecutor;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::Sink;
use crate::pipelines::processors::Sinker;
use crate::pipelines::Pipe;
use crate::pipelines::Pipeline;
use crate::pipelines::PipelineBuildResult;

struct State {
    sender: SyncSender<Result<Option<DataBlock>>>,
}

impl State {
    pub fn create(sender: SyncSender<Result<Option<DataBlock>>>) -> Arc<State> {
        Arc::new(State { sender })
    }
}

// Use this executor when the pipeline is pulling pipeline (exists source but not exists sink)
pub struct PipelinePullingExecutor {
    state: Arc<State>,
    executor: Arc<PipelineExecutor>,
    receiver: Receiver<Result<Option<DataBlock>>>,
}

impl PipelinePullingExecutor {
    fn wrap_pipeline(
        pipeline: &mut Pipeline,
        tx: SyncSender<Result<Option<DataBlock>>>,
    ) -> Result<()> {
        if pipeline.is_pushing_pipeline()? || !pipeline.is_pulling_pipeline()? {
            return Err(ErrorCode::LogicalError(
                "Logical error, PipelinePullingExecutor can only work on pulling pipeline.",
            ));
        }

        pipeline.resize(1)?;
        let input = InputPort::create();

        pipeline.add_pipe(Pipe::SimplePipe {
            outputs_port: vec![],
            inputs_port: vec![input.clone()],
            processors: vec![PullingSink::create(tx, input)],
        });
        Ok(())
    }

    pub fn try_create(
        async_runtime: Arc<Runtime>,
        query_need_abort: Arc<AtomicBool>,
        mut pipeline: Pipeline,
        settings: ExecutorSettings,
    ) -> Result<PipelinePullingExecutor> {
        let (sender, receiver) = std::sync::mpsc::sync_channel(pipeline.output_len());
        let state = State::create(sender.clone());

        Self::wrap_pipeline(&mut pipeline, sender)?;
        let executor =
            PipelineExecutor::create(async_runtime, query_need_abort, pipeline, settings)?;
        Ok(PipelinePullingExecutor {
            receiver,
            state,
            executor,
        })
    }

    pub fn from_pipelines(
        async_runtime: Arc<Runtime>,
        query_need_abort: Arc<AtomicBool>,
        build_res: PipelineBuildResult,
        settings: ExecutorSettings,
    ) -> Result<PipelinePullingExecutor> {
        let mut main_pipeline = build_res.main_pipeline;
        let (sender, receiver) = std::sync::mpsc::sync_channel(main_pipeline.output_len());
        let state = State::create(sender.clone());
        Self::wrap_pipeline(&mut main_pipeline, sender)?;

        let mut pipelines = build_res.sources_pipelines;
        pipelines.push(main_pipeline);

        Ok(PipelinePullingExecutor {
            receiver,
            state,
            executor: PipelineExecutor::from_pipelines(
                async_runtime,
                query_need_abort,
                pipelines,
                settings,
            )?,
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
            if let Err(cause) = executor.execute() {
                if let Err(send_err) = state.sender.send(Err(cause)) {
                    tracing::warn!("Send error {:?}", send_err);
                }

                return;
            }

            if let Err(send_err) = state.sender.send(Ok(None)) {
                tracing::warn!("Send finish event error {:?}", send_err);
            }
        }
    }

    pub fn finish(&self) -> Result<()> {
        self.executor.finish()
    }

    pub fn pull_data(&mut self) -> Result<Option<DataBlock>> {
        match self.receiver.recv() {
            Ok(data_block) => data_block,
            Err(_recv_err) => Err(ErrorCode::LogicalError("Logical error, receiver error.")),
        }
    }

    pub fn try_pull_data<F>(&mut self, f: F) -> Result<Option<DataBlock>>
    where F: Fn() -> bool {
        if !self.executor.is_finished() {
            while !f() {
                return match self.receiver.recv_timeout(Duration::from_millis(100)) {
                    Ok(data_block) => data_block,
                    Err(RecvTimeoutError::Timeout) => {
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        Err(ErrorCode::LogicalError("Logical error, receiver error."))
                    }
                };
            }
            Ok(None)
        } else {
            match self.receiver.try_recv() {
                Ok(data_block) => data_block,
                // puller will not pull again once it received a None
                Err(err) => Err(ErrorCode::LogicalError(format!(
                    "Logical error, try receiver error. after executor finish {}",
                    err
                ))),
            }
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
    sender: SyncSender<Result<Option<DataBlock>>>,
}

impl PullingSink {
    pub fn create(
        tx: SyncSender<Result<Option<DataBlock>>>,
        input: Arc<InputPort>,
    ) -> ProcessorPtr {
        Sinker::create(input, PullingSink { sender: tx })
    }
}

impl Sink for PullingSink {
    const NAME: &'static str = "PullingExecutorSink";

    fn on_finish(&mut self) -> Result<()> {
        Ok(())
    }

    fn consume(&mut self, data_block: DataBlock) -> Result<()> {
        if let Err(cause) = self.sender.send(Ok(Some(data_block))) {
            return Err(ErrorCode::LogicalError(format!(
                "Logical error, cannot push data into SyncSender, cause {:?}",
                cause
            )));
        }

        Ok(())
    }
}
