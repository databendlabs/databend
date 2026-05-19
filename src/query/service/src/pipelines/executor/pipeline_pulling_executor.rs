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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use async_channel::Receiver;
use async_channel::Sender;
use databend_common_base::runtime::Thread;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayload;
use databend_common_base::runtime::drop_guard;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::core::basic_callback;
use databend_common_pipeline::sinks::Sink;
use databend_common_pipeline::sinks::Sinker;
use fastrace::func_path;
use fastrace::prelude::*;
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::pipelines::PipelineBuildResult;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineExecutor;

struct State {
    is_finished: AtomicBool,
    finish_notify: Notify,
    catch_error: Mutex<Option<ErrorCode>>,
}

impl State {
    pub fn create() -> Arc<State> {
        Arc::new(State {
            catch_error: Mutex::new(None),
            finish_notify: Notify::new(),
            is_finished: AtomicBool::new(false),
        })
    }

    pub fn finished(&self, message: Result<()>) {
        if let Err(error) = message {
            *self.catch_error.lock() = Some(error);
        }

        self.is_finished.store(true, Ordering::Release);
        self.finish_notify.notify_waiters();
    }

    pub async fn wait_finish(&self) {
        loop {
            if self.is_finished() {
                return;
            }

            let notified = self.finish_notify.notified();
            if self.is_finished() {
                return;
            }

            notified.await;
        }
    }

    pub fn is_finished(&self) -> bool {
        self.is_finished.load(Ordering::Acquire)
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
    fn wrap_pipeline(pipeline: &mut Pipeline, tx: Sender<DataBlock>) -> Result<()> {
        if !pipeline.is_pulling_pipeline()? {
            return Err(ErrorCode::Internal(
                "Logical error, PipelinePullingExecutor can only work on pulling pipeline.",
            ));
        }

        pipeline
            .add_sink(|input| Ok(ProcessorPtr::create(PullingSink::create(tx.clone(), input))))?;

        pipeline.set_on_finished(basic_callback(move |_info: &ExecutionInfo| {
            drop(tx);
            Ok(())
        }));

        Ok(())
    }

    pub fn try_create(
        mut pipeline: Pipeline,
        settings: ExecutorSettings,
    ) -> Result<PipelinePullingExecutor> {
        let tracking_payload = ThreadTracker::new_tracking_payload();
        let _guard = ThreadTracker::tracking(tracking_payload.clone());

        let (sender, receiver) = async_channel::bounded(std::cmp::max(1, pipeline.output_len()));

        Self::wrap_pipeline(&mut pipeline, sender)?;
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
        let tracking_payload = ThreadTracker::new_tracking_payload();
        let _guard = ThreadTracker::tracking(tracking_payload.clone());

        let mut main_pipeline = build_res.main_pipeline;
        let (sender, receiver) =
            async_channel::bounded(std::cmp::max(1, main_pipeline.output_len()));

        Self::wrap_pipeline(&mut main_pipeline, sender)?;

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
        let span = Span::enter_with_local_parent(func_path!());
        move || {
            let _g = span.set_local_parent();
            state.finished(executor.execute());
        }
    }

    #[fastrace::trace]
    pub fn finish(&self, cause: Option<ErrorCode>) {
        let _guard = ThreadTracker::tracking(self.tracking_payload.clone());

        self.executor.finish(cause);
    }

    pub async fn pull_data(&mut self) -> Result<Option<DataBlock>> {
        tokio::select! {
            received = self.receiver.recv() => {
                self.handle_received(received).await
            }
            () = self.state.wait_finish() => {
                if let Some(error) = self.state.try_get_catch_error() {
                    return Err(error);
                }
                self.handle_received(self.receiver.recv().await).await
            }
        }
    }

    async fn handle_received(
        &self,
        received: std::result::Result<DataBlock, async_channel::RecvError>,
    ) -> Result<Option<DataBlock>> {
        match received {
            Ok(data_block) => Ok(Some(data_block)),
            Err(_) => {
                if !self.executor.is_finished() {
                    self.executor.finish::<()>(None);
                }

                self.state.wait_finish().await;
                self.finish_result()
            }
        }
    }

    fn finish_result(&self) -> Result<Option<DataBlock>> {
        match self.state.try_get_catch_error() {
            None => Ok(None),
            Some(error) => Err(error),
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
    sender: Option<Sender<DataBlock>>,
}

impl PullingSink {
    pub fn create(tx: Sender<DataBlock>, input: Arc<InputPort>) -> Box<dyn Processor> {
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
            if let Err(cause) = sender.send_blocking(data_block) {
                return Err(ErrorCode::Internal(format!(
                    "Logical error, cannot push data into SyncSender, cause {:?}",
                    cause
                )));
            }
        }

        Ok(())
    }
}
