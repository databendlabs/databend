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
use std::sync::mpsc::SyncSender;
use std::sync::Arc;

use common_base::base::GlobalIORuntime;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use parking_lot::Mutex;

use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineExecutor;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::processors::SyncSource;
use crate::pipelines::processors::SyncSourcer;
use crate::pipelines::Pipeline;
use crate::pipelines::SourcePipeBuilder;
use crate::sessions::QueryContext;

struct State {
    finished: AtomicBool,
    has_throw_error: AtomicBool,

    throw_error: Mutex<Option<ErrorCode>>,
}

impl State {
    pub fn create() -> Arc<State> {
        Arc::new(State {
            finished: AtomicBool::new(false),
            has_throw_error: AtomicBool::new(false),
            throw_error: Mutex::new(None),
        })
    }
}

// Use this executor when the pipeline is pushing pipeline (exists sink but not exists source)
#[allow(dead_code)]
pub struct PipelinePushingExecutor {
    state: Arc<State>,
    executor: Arc<PipelineExecutor>,
    sender: SyncSender<Option<DataBlock>>,
}

#[allow(dead_code)]
impl PipelinePushingExecutor {
    fn wrap_pipeline(
        ctx: Arc<QueryContext>,
        pipeline: &mut Pipeline,
    ) -> Result<SyncSender<Option<DataBlock>>> {
        if pipeline.is_pulling_pipeline()? || !pipeline.is_pushing_pipeline()? {
            return Err(ErrorCode::LogicalError(
                "Logical error, PipelinePushingExecutor can only work on pushing pipeline.",
            ));
        }

        let (tx, rx) = std::sync::mpsc::sync_channel(pipeline.input_len());
        let mut source_pipe_builder = SourcePipeBuilder::create();

        let mut new_pipeline = Pipeline::create();
        let output = OutputPort::create();
        let pushing_source = PushingSource::create(ctx, rx, output.clone())?;
        source_pipe_builder.add_source(output, pushing_source);

        new_pipeline.add_pipe(source_pipe_builder.finalize());
        new_pipeline.resize(pipeline.input_len())?;
        for pipe in &pipeline.pipes {
            new_pipeline.add_pipe(pipe.clone())
        }

        *pipeline = new_pipeline;
        Ok(tx)
    }

    pub fn try_create(
        ctx: Arc<QueryContext>,
        query_need_abort: Arc<AtomicBool>,
        mut pipeline: Pipeline,
        settings: ExecutorSettings,
    ) -> Result<PipelinePushingExecutor> {
        let state = State::create();
        let async_runtime = GlobalIORuntime::instance();
        let sender = Self::wrap_pipeline(ctx, &mut pipeline)?;
        let executor =
            PipelineExecutor::create(async_runtime, query_need_abort, pipeline, settings)?;
        Ok(PipelinePushingExecutor {
            state,
            sender,
            executor,
        })
    }

    pub fn start(&mut self) {
        let state = self.state.clone();
        let threads_executor = self.executor.clone();
        let thread_function = Self::thread_function(state, threads_executor);
        std::thread::spawn(thread_function);
    }

    fn thread_function(state: Arc<State>, executor: Arc<PipelineExecutor>) -> impl Fn() {
        move || {
            if let Err(cause) = executor.execute() {
                state.has_throw_error.store(true, Ordering::Release);
                std::sync::atomic::fence(Ordering::Acquire);
                let mut throw_error = state.throw_error.lock();
                *throw_error = Some(cause);
                return;
            }

            state.finished.store(true, Ordering::Release);
        }
    }

    pub fn get_inner(&self) -> Arc<PipelineExecutor> {
        self.executor.clone()
    }

    pub fn finish(&self) -> Result<()> {
        self.state.finished.store(true, Ordering::Release);
        self.executor.finish()
    }

    pub fn push_data(&mut self, data: DataBlock) -> Result<()> {
        if self.state.has_throw_error.load(Ordering::Relaxed) {
            let mut throw_error = self.state.throw_error.lock();

            return match throw_error.take() {
                None => Err(ErrorCode::LogicalError("Missing error info.")),
                Some(cause) => Err(cause),
            };
        }

        if self.state.finished.load(Ordering::Relaxed) {
            return Ok(());
        }

        match self.sender.send(Some(data)) {
            Ok(_) => Ok(()),
            Err(cause) => Err(ErrorCode::LogicalError(format!(
                "Logical error, send error {:?}.",
                cause
            ))),
        }
    }
}

impl Drop for PipelinePushingExecutor {
    fn drop(&mut self) {
        if !self.state.finished.load(Ordering::Relaxed)
            && !self.state.has_throw_error.load(Ordering::Relaxed)
        {
            if let Err(cause) = self.finish() {
                tracing::warn!("Executor finish is failure {:?}", cause);
            }
        }

        if let Err(cause) = self.sender.send(None) {
            tracing::warn!("Executor send last data is failure {:?}", cause);
        }
    }
}

struct PushingSource {
    receiver: Receiver<Option<DataBlock>>,
}

impl PushingSource {
    pub fn create(
        ctx: Arc<QueryContext>,
        receiver: Receiver<Option<DataBlock>>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx, output, PushingSource { receiver })
    }
}

impl SyncSource for PushingSource {
    const NAME: &'static str = "PushingExecutorSource";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        match self.receiver.recv() {
            Ok(data) => Ok(data),
            Err(cause) => Err(ErrorCode::LogicalError(format!(
                "Logical error, receive error. {:?}",
                cause
            ))),
        }
    }
}
