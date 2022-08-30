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
use std::sync::Arc;

use common_base::base::Runtime;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineExecutor;
use crate::pipelines::Pipeline;

pub struct PipelineCompleteExecutor {
    executor: Arc<PipelineExecutor>,
}

// Use this executor when the pipeline is complete pipeline (has source and sink)
#[allow(dead_code)]
impl PipelineCompleteExecutor {
    pub fn try_create(
        async_runtime: Arc<Runtime>,
        query_need_abort: Arc<AtomicBool>,
        pipeline: Pipeline,
        settings: ExecutorSettings,
    ) -> Result<PipelineCompleteExecutor> {
        if !pipeline.is_complete_pipeline()? {
            return Err(ErrorCode::LogicalError(
                "Logical error, PipelineCompleteExecutor can only work on complete pipeline.",
            ));
        }

        let executor =
            PipelineExecutor::create(async_runtime, query_need_abort, pipeline, settings)?;
        Ok(PipelineCompleteExecutor { executor })
    }

    pub fn from_pipelines(
        async_runtime: Arc<Runtime>,
        query_need_abort: Arc<AtomicBool>,
        pipelines: Vec<Pipeline>,
        settings: ExecutorSettings,
    ) -> Result<Arc<PipelineCompleteExecutor>> {
        for pipeline in &pipelines {
            if !pipeline.is_complete_pipeline()? {
                return Err(ErrorCode::LogicalError(
                    "Logical error, PipelineCompleteExecutor can only work on complete pipeline.",
                ));
            }
        }

        let executor =
            PipelineExecutor::from_pipelines(async_runtime, query_need_abort, pipelines, settings)?;
        Ok(Arc::new(PipelineCompleteExecutor { executor }))
    }

    pub fn get_inner(&self) -> Arc<PipelineExecutor> {
        self.executor.clone()
    }

    pub fn finish(&self) -> Result<()> {
        self.executor.finish()
    }

    pub fn execute(&self) -> Result<()> {
        self.executor.execute()
    }
}

impl Drop for PipelineCompleteExecutor {
    fn drop(&mut self) {
        if let Err(cause) = self.finish() {
            tracing::warn!("Executor finish is failure {:?}", cause);
        }
    }
}
