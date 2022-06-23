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

use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::time::Duration;

use common_base::base::Runtime;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::pipelines::new::executor::PipelineExecutor;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::Sink;
use crate::pipelines::new::processors::Sinker;
use crate::pipelines::new::NewPipe;
use crate::pipelines::new::NewPipeline;

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
        pipeline: &mut NewPipeline,
        tx: SyncSender<Result<Option<DataBlock>>>,
    ) -> Result<()> {
        if pipeline.is_pushing_pipeline()? || !pipeline.is_pulling_pipeline()? {
            return Err(ErrorCode::LogicalError(
                "Logical error, PipelinePullingExecutor can only work on pulling pipeline.",
            ));
        }

        pipeline.resize(1)?;
        let input = InputPort::create();

        pipeline.add_pipe(NewPipe::SimplePipe {
            outputs_port: vec![],
            inputs_port: vec![input.clone()],
            processors: vec![PullingSink::create(tx, input)],
        });
        Ok(())
    }

    pub fn try_create(
        async_runtime: Arc<Runtime>,
        mut pipeline: NewPipeline,
    ) -> Result<PipelinePullingExecutor> {
        let (sender, receiver) = std::sync::mpsc::sync_channel(pipeline.output_len());
        let state = State::create(sender.clone());

        Self::wrap_pipeline(&mut pipeline, sender)?;
        let executor = PipelineExecutor::create(async_runtime, pipeline)?;
        Ok(PipelinePullingExecutor {
            receiver,
            state,
            executor,
        })
    }

    pub fn start(&mut self) {
        let state = self.state.clone();
        let threads_executor = self.executor.clone();
        let thread_function = Self::thread_function(state, threads_executor);
        std::thread::spawn(thread_function);
    }

    pub fn get_inner(&self) -> Arc<PipelineExecutor> {
        self.executor.clone()
    }

    fn thread_function(state: Arc<State>, executor: Arc<PipelineExecutor>) -> impl Fn() {
        move || {
            if let Err(cause) = executor.execute() {
                if let Err(send_err) = state.sender.send(Err(cause)) {
                    common_tracing::tracing::warn!("Send error {:?}", send_err);
                }

                return;
            }

            if let Err(send_err) = state.sender.send(Ok(None)) {
                common_tracing::tracing::warn!("Send finish event error {:?}", send_err);
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
            return match self.receiver.try_recv() {
                Ok(data_block) => data_block,
                // puller will not pull again once it received a None
                Err(err) => Err(ErrorCode::LogicalError(format!(
                    "Logical error, try receiver error. after executor finish {}",
                    err
                ))),
            };
        }
    }
}

impl Drop for PipelinePullingExecutor {
    fn drop(&mut self) {
        if let Err(cause) = self.finish() {
            common_tracing::tracing::warn!("Executor finish is failure {:?}", cause);
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
        if let Err(cause) = self.sender.send(Ok(None)) {
            return Err(ErrorCode::LogicalError(format!(
                "Logical error, cannot push data into SyncSender, cause {:?}",
                cause
            )));
        }

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
