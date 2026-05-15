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

use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineExecutor;

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

    #[fastrace::trace]
    pub fn execute(&self) -> Result<()> {
        let _guard = ThreadTracker::tracking(self.tracking_payload.clone());

        Thread::named_spawn(
            Some(String::from("CompleteExecutor")),
            self.thread_function(),
        )
        .join()
        .flatten()
    }

    /// Runs the complete pipeline from an async caller without blocking a Tokio worker.
    ///
    /// `execute` waits on the dedicated executor thread with `join`, so async
    /// paths use this wrapper to receive the result through a channel instead.
    pub async fn execute_async(self: &Arc<Self>) -> Result<()> {
        let _guard = ThreadTracker::tracking(self.tracking_payload.clone());
        let (tx, rx) = oneshot::channel();
        let thread_function = self.thread_function();

        Thread::named_spawn(Some(String::from("CompleteExecutor")), move || {
            let _ = tx.send(Result::flatten(catch_unwind(thread_function)));
        });

        rx.await.map_err(|_| {
            ErrorCode::Internal("Complete executor thread exited without returning result")
        })?
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
