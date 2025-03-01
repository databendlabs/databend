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

use std::sync::mpsc::Receiver;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::drop_guard;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::MemStat;
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
use parking_lot::Mutex;

use crate::pipelines::executor::pipeline_executor::NewPipelineExecutor;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::QueryHandle;
use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::ProcessorPtr;
use crate::pipelines::PipelineBuildResult;

// Use this executor when the pipeline is pulling pipeline (exists source but not exists sink)
pub struct PipelinePullingExecutor {
    receiver: Receiver<DataBlock>,
    tracking_payload: TrackingPayload,

    pipelines: Mutex<Vec<Pipeline>>,
    settings: Mutex<Option<ExecutorSettings>>,

    query_handle: Mutex<Option<Arc<dyn QueryHandle>>>,
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

    fn wrap_pipeline(pipeline: &mut Pipeline, tx: SyncSender<DataBlock>) -> Result<()> {
        if pipeline.is_pushing_pipeline()? || !pipeline.is_pulling_pipeline()? {
            return Err(ErrorCode::Internal(
                "Logical error, PipelinePullingExecutor can only work on pulling pipeline.",
            ));
        }

        pipeline
            .add_sink(|input| Ok(ProcessorPtr::create(PullingSink::create(tx.clone(), input))))?;

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

        Self::wrap_pipeline(&mut pipeline, sender)?;

        Ok(PipelinePullingExecutor {
            receiver,
            tracking_payload,
            pipelines: Mutex::new(vec![pipeline]),
            settings: Mutex::new(Some(settings)),
            query_handle: Mutex::new(None),
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

        Self::wrap_pipeline(&mut main_pipeline, sender)?;

        let mut pipelines = build_res.sources_pipelines;
        pipelines.push(main_pipeline);

        Ok(PipelinePullingExecutor {
            receiver,
            tracking_payload,
            pipelines: Mutex::new(pipelines),
            settings: Mutex::new(Some(settings)),
            query_handle: Mutex::new(None),
        })
    }

    pub async fn start(&mut self) -> Result<Arc<dyn QueryHandle>> {
        let (settings, pipelines) = {
            if let Some(settings) = self.settings.lock().take() {
                let mut pipelines = vec![];
                std::mem::swap(&mut pipelines, self.pipelines.lock().as_mut());
                (settings, pipelines)
            } else {
                return Ok(self.query_handle.lock().clone().unwrap());
            }
        };

        let executor = GlobalInstance::get::<Arc<dyn NewPipelineExecutor>>();
        let query_handle = executor.submit(pipelines, settings).await?;
        *self.query_handle.lock() = Some(query_handle.clone());
        Ok(query_handle)

        // let _guard = ThreadTracker::tracking(self.tracking_payload.clone());
        //
        // let state = self.state.clone();
        // let threads_executor = self.executor.clone();
        // let thread_function = Self::thread_function(state, threads_executor);
        // #[allow(unused_mut)]
        // let mut thread_name = Some(String::from("PullingExecutor"));
        //
        // #[cfg(debug_assertions)]
        // {
        //     // We need to pass the thread name in the unit test, because the thread name is the test name
        //     if matches!(std::env::var("UNIT_TEST"), Ok(var_value) if var_value == "TRUE") {
        //         if let Some(cur_thread_name) = std::thread::current().name() {
        //             thread_name = Some(cur_thread_name.to_string());
        //         }
        //     }
        // }
        //
        // Thread::named_spawn(thread_name, thread_function);
    }

    pub fn get_handle(&self) -> Arc<dyn QueryHandle> {
        self.query_handle.lock().clone().unwrap()
    }

    pub fn finish(&self, cause: Option<ErrorCode>) {
        if let Some(handle) = self.query_handle.lock().as_ref() {
            handle.finish(cause);
        }
    }

    pub fn pull_data(&mut self) -> Result<Option<DataBlock>> {
        let query_handle = self.query_handle.lock().clone().unwrap();

        loop {
            return match self.receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(data_block) => Ok(Some(data_block)),
                Err(RecvTimeoutError::Timeout) => {
                    if !query_handle.is_finished() {
                        continue;
                    }

                    GlobalIORuntime::instance().block_on(query_handle.wait())?;
                    Ok(None)
                }
                Err(RecvTimeoutError::Disconnected) => {
                    if !query_handle.is_finished() {
                        query_handle.finish(None);
                    }

                    GlobalIORuntime::instance().block_on(query_handle.wait())?;
                    Ok(None)
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
}

impl PullingSink {
    pub fn create(tx: SyncSender<DataBlock>, input: Arc<InputPort>) -> Box<dyn Processor> {
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
        let memory_size = data_block.memory_size() as i64;
        // TODO: need moveout memory for plan tracker
        ThreadTracker::moveout_memory(memory_size);

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
