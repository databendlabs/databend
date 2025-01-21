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

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::drop_guard;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayload;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline_core::Pipeline;
use parking_lot::Mutex;

use crate::pipelines::executor::pipeline_executor::NewPipelineExecutor;
use crate::pipelines::executor::pipeline_executor::QueryHandle;
use crate::pipelines::executor::ExecutorSettings;

pub struct PipelineCompleteExecutor {
    pipelines: Mutex<Vec<Pipeline>>,
    settings: Mutex<Option<ExecutorSettings>>,

    tracking_payload: TrackingPayload,
    query_handle: Mutex<Option<Arc<dyn QueryHandle>>>,
}

// Use this executor when the pipeline is complete pipeline (has source and sink)
impl PipelineCompleteExecutor {
    fn execution_tracking_payload(query_id: &str) -> TrackingPayload {
        let mut tracking_payload = ThreadTracker::new_tracking_payload();
        tracking_payload.mem_stat = Some(MemStat::create(format!(
            "QueryExecutionMemStat-{}",
            query_id
        )));
        tracking_payload
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

        Ok(PipelineCompleteExecutor {
            pipelines: Mutex::new(vec![pipeline]),
            settings: Mutex::new(Some(settings)),
            tracking_payload,
            query_handle: Mutex::new(None),
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

        Ok(Arc::new(PipelineCompleteExecutor {
            tracking_payload,
            settings: Mutex::new(Some(settings)),
            pipelines: Mutex::new(pipelines),
            query_handle: Mutex::new(None),
        }))
    }

    pub fn get_handle(&self) -> Arc<dyn QueryHandle> {
        self.query_handle.lock().clone().unwrap()
    }

    pub fn finish(&self, cause: Option<ErrorCode>) {
        if let Some(handle) = self.query_handle.lock().as_ref() {
            handle.finish(cause);
        }
    }

    pub async fn execute(&self) -> Result<Arc<dyn QueryHandle>> {
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
    }
}

impl Drop for PipelineCompleteExecutor {
    fn drop(&mut self) {
        drop_guard(move || {
            let _guard = ThreadTracker::tracking(self.tracking_payload.clone());

            self.finish(None);
        })
    }
}
