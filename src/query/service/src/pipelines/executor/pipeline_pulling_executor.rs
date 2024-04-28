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

use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use async_trait::unboxed_simple;
use databend_common_base::runtime::drop_guard;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::Thread;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayload;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sinks::AsyncSink;
use databend_common_pipeline_sinks::AsyncSinker;
use log::warn;
use minitrace::full_name;
use minitrace::prelude::*;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineExecutor;
use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::ProcessorPtr;
use crate::pipelines::PipelineBuildResult;

// Use this executor when the pipeline is pulling pipeline (exists source but not exists sink)
pub struct PipelinePullingExecutor {
    is_closed_recv: bool,
    tx: Option<Sender<Result<Option<DataBlock>>>>,
    executor: Arc<PipelineExecutor>,
    receiver: Receiver<Result<Option<DataBlock>>>,
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
        tx: Sender<Result<Option<DataBlock>>>,
        mem_stat: Arc<MemStat>,
    ) -> Result<()> {
        if pipeline.is_pushing_pipeline()? || !pipeline.is_pulling_pipeline()? {
            return Err(ErrorCode::Internal(
                "Logical error, PipelinePullingExecutor can only work on pulling pipeline.",
            ));
        }

        pipeline.add_sink(|input| {
            Ok(ProcessorPtr::create(PullingAsyncSink::create(
                tx.clone(),
                mem_stat.clone(),
                input,
            )))
        })?;

        pipeline.set_on_finished(move |_may_error| {
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

        let (sender, receiver) = tokio::sync::mpsc::channel(pipeline.output_len());

        Self::wrap_pipeline(
            &mut pipeline,
            sender.clone(),
            tracking_payload.mem_stat.clone().unwrap(),
        )?;
        let executor = PipelineExecutor::create(pipeline, settings)?;

        Ok(PipelinePullingExecutor {
            is_closed_recv: false,
            receiver,
            executor: Arc::new(executor),
            tracking_payload,
            tx: Some(sender),
        })
    }

    pub fn from_pipelines(
        build_res: PipelineBuildResult,
        settings: ExecutorSettings,
    ) -> Result<PipelinePullingExecutor> {
        let tracking_payload = Self::execution_tracking_payload(settings.query_id.as_ref());
        let _guard = ThreadTracker::tracking(tracking_payload.clone());

        let mut main_pipeline = build_res.main_pipeline;
        let (sender, receiver) = tokio::sync::mpsc::channel(main_pipeline.output_len());

        Self::wrap_pipeline(
            &mut main_pipeline,
            sender.clone(),
            tracking_payload.mem_stat.clone().unwrap(),
        )?;

        let mut pipelines = build_res.sources_pipelines;
        pipelines.push(main_pipeline);
        let executor = PipelineExecutor::from_pipelines(pipelines, settings)?;
        Ok(PipelinePullingExecutor {
            is_closed_recv: false,
            receiver,
            tracking_payload,
            executor: Arc::new(executor),
            tx: Some(sender),
        })
    }

    #[minitrace::trace]
    pub fn start(&mut self) {
        let _guard = ThreadTracker::tracking(self.tracking_payload.clone());

        if let Some(tx) = self.tx.take() {
            let threads_executor = self.executor.clone();
            let thread_function = Self::thread_function(tx, threads_executor);
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
    }

    pub fn get_inner(&self) -> Arc<PipelineExecutor> {
        self.executor.clone()
    }

    fn thread_function(
        tx: Sender<Result<Option<DataBlock>>>,
        executor: Arc<PipelineExecutor>,
    ) -> impl FnOnce() {
        let span = Span::enter_with_local_parent(full_name!());
        move || {
            let _g = span.set_local_parent();
            match executor.execute() {
                Ok(_) => GlobalIORuntime::instance().block_on(async move {
                    if tx.send(Ok(None)).await.is_err() {
                        warn!("Executor result recv is closed. may aborted query or many call this.");
                    }
                    Ok(())
                }).ok(),
                Err(error) => GlobalIORuntime::instance().block_on(async move {
                    if tx.send(Err(error)).await.is_err() {
                        warn!("Executor result recv is closed. may aborted query or many call this.");
                    }
                    Ok(())
                }).ok()
            };
        }
    }

    pub fn finish(&self, cause: Option<ErrorCode>) {
        let _guard = ThreadTracker::tracking(self.tracking_payload.clone());

        self.executor.finish(cause);
    }

    pub async fn recv_data(&mut self) -> Result<Option<DataBlock>> {
        if self.is_closed_recv {
            return Ok(None);
        }

        match self.receiver.recv().await {
            None => Ok(None),
            Some(Err(error)) => Err(error),
            Some(Ok(Some(data_block))) => Ok(Some(data_block)),
            Some(Ok(None)) => {
                self.receiver.close();
                self.is_closed_recv = true;
                Ok(None)
            }
        }
    }

    pub fn poll_recv_data(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<DataBlock>>> {
        if self.is_closed_recv {
            return Poll::Ready(Ok(None));
        }

        match self.receiver.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(res) => match res {
                None => Poll::Ready(Ok(None)),
                Some(Err(error)) => Poll::Ready(Err(error)),
                Some(Ok(Some(data_block))) => Poll::Ready(Ok(Some(data_block))),
                Some(Ok(None)) => {
                    self.receiver.close();
                    self.is_closed_recv = true;
                    Poll::Ready(Ok(None))
                }
            },
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

struct PullingAsyncSink {
    sender: Option<Sender<Result<Option<DataBlock>>>>,
    query_execution_mem_stat: Arc<MemStat>,
}

impl PullingAsyncSink {
    pub fn create(
        tx: Sender<Result<Option<DataBlock>>>,
        mem_stat: Arc<MemStat>,
        input: Arc<InputPort>,
    ) -> Box<dyn Processor> {
        AsyncSinker::create(input, PullingAsyncSink {
            sender: Some(tx),
            query_execution_mem_stat: mem_stat,
        })
    }
}

#[async_trait]
impl AsyncSink for PullingAsyncSink {
    const NAME: &'static str = "PullingExecutorSink";

    async fn on_finish(&mut self) -> Result<()> {
        drop(self.sender.take());
        Ok(())
    }

    #[unboxed_simple]
    async fn consume(&mut self, data_block: DataBlock) -> Result<bool> {
        let memory_size = data_block.memory_size() as i64;
        // TODO: need moveout memory for plan tracker
        ThreadTracker::moveout_memory(memory_size);

        self.query_execution_mem_stat.moveout_memory(memory_size);

        if let Some(sender) = &self.sender {
            if let Err(cause) = sender.send(Ok(Some(data_block))).await {
                return Err(ErrorCode::Internal(format!(
                    "Logical error, cannot push data into SyncSender, cause {:?}",
                    cause
                )));
            }
        }

        Ok(false)
    }
}
