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

use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use common_base::tokio::sync::mpsc::Receiver;
use common_base::tokio::sync::mpsc::Sender;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::Mutex;

use crate::pipelines::new::executor::pipeline_runtime_executor::PipelineRuntimeExecutor;
use crate::pipelines::new::pipe::SinkPipeBuilder;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::AsyncSink;
use crate::pipelines::new::processors::AsyncSinker;
use crate::pipelines::new::NewPipeline;

enum Executor {
    Inited(Arc<PipelineRuntimeExecutor>),
    Running(Arc<PipelineRuntimeExecutor>),
    Finished(Result<()>),
}

pub struct AsyncPipelinePullingExecutor {
    executor: Arc<Mutex<Executor>>,
    receiver: Receiver<DataBlock>,
}

impl AsyncPipelinePullingExecutor {
    fn wrap_pipeline(pipeline: &mut NewPipeline) -> Result<Receiver<DataBlock>> {
        if pipeline.pipes.is_empty() {
            return Err(ErrorCode::PipelineUnInitialized(""));
        }

        if pipeline.pipes[0].input_size() != 0 {
            return Err(ErrorCode::PipelineUnInitialized(""));
        }

        if pipeline.output_len() == 0 {
            return Err(ErrorCode::PipelineUnInitialized(""));
        }

        let (tx, rx) = common_base::tokio::sync::mpsc::channel(pipeline.output_len());
        // let (tx, rx) = std::sync::mpsc::sync_channel(pipeline.output_len());
        let mut pipe_builder = SinkPipeBuilder::create();

        for _index in 0..pipeline.output_len() {
            let input = InputPort::create();
            let pulling_sink = AsyncPullingSink::create(tx.clone(), input.clone());
            pipe_builder.add_sink(input.clone(), pulling_sink);
        }

        pipeline.add_pipe(pipe_builder.finalize());
        Ok(rx)
    }

    pub fn try_create(mut pipeline: NewPipeline) -> Result<AsyncPipelinePullingExecutor> {
        let receiver = Self::wrap_pipeline(&mut pipeline)?;
        let pipeline_executor = PipelineRuntimeExecutor::create(pipeline)?;
        let executor = Arc::new(Mutex::new(Executor::Inited(pipeline_executor)));
        Ok(AsyncPipelinePullingExecutor { receiver, executor })
    }

    pub fn start(&mut self) -> Result<()> {
        let mut executor = self.executor.lock();

        if let Executor::Inited(threads_executor) = &*executor {
            let state = self.executor.clone();
            let threads_executor = threads_executor.clone();
            let thread_function = Self::thread_function(state, threads_executor.clone());

            std::thread::spawn(thread_function);
            *executor = Executor::Running(threads_executor);
        }

        Ok(())
    }

    fn thread_function(
        state: Arc<Mutex<Executor>>,
        threads_executor: Arc<PipelineRuntimeExecutor>,
    ) -> impl Fn() {
        move || {
            let state = state.clone();
            let threads_executor = threads_executor.clone();
            futures::executor::block_on(async move {
                let res = threads_executor.execute().await;
                let mut state = state.lock();
                match res {
                    Ok(_) => {
                        *state = Executor::Finished(Ok(()));
                    }
                    Err(cause) => {
                        *state = Executor::Finished(Err(cause));
                    }
                }
            });
        }
    }

    pub fn finish(&mut self) -> Result<()> {
        let mutex_guard = self.executor.lock();
        match &*mutex_guard {
            Executor::Inited(_) => Ok(()),
            Executor::Running(executor) => executor.finish(),
            Executor::Finished(res) => res.clone(),
        }
    }

    pub fn pull_data_with_ctx(&mut self, cx: &mut Context<'_>) -> Poll<Option<DataBlock>> {
        self.receiver.poll_recv(cx)
    }

    pub async fn pull_data(&mut self) -> Option<DataBlock> {
        self.receiver.recv().await
    }
}

enum AsyncPullingSink {
    Running(Sender<DataBlock>),
    Finished,
}

impl AsyncPullingSink {
    pub fn create(tx: Sender<DataBlock>, input: Arc<InputPort>) -> ProcessorPtr {
        AsyncSinker::create(input, AsyncPullingSink::Running(tx))
    }
}

#[async_trait::async_trait]
impl AsyncSink for AsyncPullingSink {
    const NAME: &'static str = "PullingExecutorSink";

    async fn on_finish(&mut self) -> Result<()> {
        if let AsyncPullingSink::Running(_) = self {
            *self = AsyncPullingSink::Finished;
        }

        Ok(())
    }

    async fn consume(&mut self, data_block: DataBlock) -> Result<()> {
        match self {
            AsyncPullingSink::Finished => Ok(()),
            AsyncPullingSink::Running(tx) => match tx.send(data_block).await {
                Ok(_) => Ok(()),
                Err(cause) => Err(ErrorCode::LogicalError(format!("{:?}", cause))),
            },
        }
    }
}
