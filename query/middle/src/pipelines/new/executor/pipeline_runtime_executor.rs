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
    threads_num: usize,
    inner_executor: Arc<PipelineExecutor>,
}

#[allow(dead_code)]
impl PipelineRuntimeExecutor {
    pub fn create(pipeline: NewPipeline) -> Result<Arc<PipelineRuntimeExecutor>> {
        let threads_num = pipeline.get_max_threads();
        let inner_executor = PipelineExecutor::create(pipeline, threads_num)?;
        Ok(Arc::new(PipelineRuntimeExecutor {
            threads_num,
            inner_executor,
        }))
    }

    pub fn finish(&self) -> Result<()> {
        self.inner_executor.finish();
        Ok(())
    }

    pub async fn execute(&self) -> Result<()> {
        // futures::executor::block_on(||);
        let mut threads = Vec::with_capacity(self.threads_num);
        let runtime = Runtime::with_worker_threads(self.threads_num * 2)?;
        for thread_num in 0..self.threads_num {
            let worker_executor = self.inner_executor.clone();
            threads.push(runtime.spawn(async move {
                unsafe {
                    match worker_executor.execute(thread_num) {
                        Ok(_) => Ok(()),
                        Err(cause) => {
                            worker_executor.finish();
                            Err(cause.add_message_back(format!(
                                " (while in processor thread {})",
                                thread_num
                            )))
                        }
                    }
                }
            }));
        }

        while let Some(join_handle) = threads.pop() {
            // flatten error.
            match join_handle.await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(cause)) => Err(cause),
                Err(cause) => {
                    self.inner_executor.finish();
                    Err(ErrorCode::LogicalError(format!("{:?}", cause)))
                }
            }?;
        }

        Ok(())
    }
}
