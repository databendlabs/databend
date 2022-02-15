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

use common_base::Runtime;
use common_base::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::pipelines::new::executor::pipeline_executor::PipelineExecutor;
use crate::pipelines::new::pipeline::NewPipeline;

#[allow(dead_code)]
pub struct PipelineRuntimeExecutor {
    base: Option<Arc<PipelineExecutor>>,
    pipeline: Option<NewPipeline>,
}

#[allow(dead_code)]
impl PipelineRuntimeExecutor {
    pub fn create(pipeline: NewPipeline) -> Result<PipelineRuntimeExecutor> {
        Ok(PipelineRuntimeExecutor {
            base: None,
            pipeline: Some(pipeline),
        })
    }

    pub fn start(&mut self, workers: usize, runtime: Runtime) -> Result<()> {
        if self.base.is_some() {
            return Err(ErrorCode::AlreadyStarted(
                "PipelineRuntimeExecutor is already started.",
            ));
        }

        match self.pipeline.take() {
            None => Err(ErrorCode::LogicalError("Logical error: it's a bug.")),
            Some(pipeline) => self.start_workers(workers, pipeline, runtime),
        }
    }

    fn start_workers(&mut self, workers: usize, pipeline: NewPipeline, rt: Runtime) -> Result<()> {
        let executor = PipelineExecutor::create(pipeline, workers)?;

        self.base = Some(executor.clone());
        for worker_num in 0..workers {
            let worker = executor.clone();
            // TODO: wait runtime shutdown.
            rt.spawn(async move {
                unsafe {
                    if let Err(cause) = worker.execute(worker_num) {
                        // TODO: worker
                        println!("Worker {} failure, cause {:?}", worker_num, cause);
                    }
                }
            });
        }
        Ok(())
    }
}
