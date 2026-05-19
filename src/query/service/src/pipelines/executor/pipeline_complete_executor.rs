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
use std::time::Duration;

use databend_common_base::runtime::Thread;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayload;
use databend_common_base::runtime::catch_unwind;
use databend_common_base::runtime::drop_guard;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline::core::Pipeline;
use fastrace::func_path;
use fastrace::prelude::*;
use tokio::sync::oneshot;
use tokio::time::sleep;
use tokio::time::timeout;

use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineExecutor;

const COMPLETE_EXECUTOR_JOIN_TIMEOUT: Duration = Duration::from_secs(3);

pub struct PipelineCompleteExecutor {
    executor: Arc<PipelineExecutor>,
    tracking_payload: TrackingPayload,
}

// Use this executor when the pipeline is complete pipeline (has source and sink)
impl PipelineCompleteExecutor {
    fn execution_tracking_payload(_query_id: &str) -> TrackingPayload {
        ThreadTracker::new_tracking_payload()
    }

    pub fn try_create(
        pipeline: Pipeline,
        settings: ExecutorSettings,
    ) -> Result<PipelineCompleteExecutor> {
        let tracking_payload = Self::execution_tracking_payload(settings.query_id.as_ref());
        let _guard = ThreadTracker::tracking(tracking_payload.clone());

        if !pipeline.is_complete_pipeline()? {
            return Err(ErrorCode::Internal(
                "Logical error, PipelineCompleteExecutor can only work on complete pipeline.",
            ));
        }
        let executor = PipelineExecutor::create(pipeline, settings)?;

        Ok(PipelineCompleteExecutor {
            executor: Arc::new(executor),
            tracking_payload,
        })
    }

    pub fn from_pipelines(
        pipelines: Vec<Pipeline>,
        settings: ExecutorSettings,
    ) -> Result<Arc<PipelineCompleteExecutor>> {
        let tracking_payload = Self::execution_tracking_payload(settings.query_id.as_ref());
        let _guard = ThreadTracker::tracking(tracking_payload.clone());

        for pipeline in &pipelines {
            if !pipeline.is_complete_pipeline()? {
                return Err(ErrorCode::Internal(
                    "Logical error, PipelineCompleteExecutor can only work on complete pipeline.",
                ));
            }
        }

        let executor = PipelineExecutor::from_pipelines(pipelines, settings)?;
        Ok(Arc::new(PipelineCompleteExecutor {
            executor: Arc::new(executor),
            tracking_payload,
        }))
    }

    pub fn get_inner(&self) -> Arc<PipelineExecutor> {
        self.executor.clone()
    }

    pub fn finish(&self, cause: Option<ErrorCode>) {
        let _guard = ThreadTracker::tracking(self.tracking_payload.clone());
        self.executor.finish(cause);
    }

    /// Runs the complete pipeline without blocking a Tokio worker.
    #[fastrace::trace]
    pub async fn execute(&self) -> Result<()> {
        let _guard = ThreadTracker::tracking(self.tracking_payload.clone());
        let (tx, rx) = oneshot::channel();
        let thread_function = self.thread_function();

        let join_handle = Thread::named_spawn(Some(String::from("CompleteExecutor")), move || {
            let _ = tx.send(Result::flatten(catch_unwind(thread_function)));
        });

        let res = rx.await.map_err(|_| {
            ErrorCode::Internal("Complete executor thread exited without returning result")
        })?;

        let Ok(_) = timeout(COMPLETE_EXECUTOR_JOIN_TIMEOUT, async {
            while !join_handle.is_finished() {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        else {
            return Err(ErrorCode::Internal(
                "Complete executor thread did not exit within 3s after returning result",
            ));
        };

        join_handle.join()?;
        res
    }

    fn thread_function(&self) -> impl Fn() -> Result<()> + use<> {
        let span = Span::enter_with_local_parent(func_path!());
        let executor = self.executor.clone();

        move || {
            let _g = span.set_local_parent();
            executor.execute()
        }
    }
}

impl Drop for PipelineCompleteExecutor {
    fn drop(&mut self) {
        drop_guard(move || {
            self.finish(None);
        })
    }
}
