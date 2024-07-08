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

use databend_common_exception::Result;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::executor::physical_plans::Udf;

use crate::pipelines::processors::transforms::TransformUdfScript;
use crate::pipelines::processors::transforms::TransformUdfServer;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_udf(&mut self, udf: &Udf) -> Result<()> {
        self.build_pipeline(&udf.input)?;

        if udf.script_udf {
            let runtimes = TransformUdfScript::init_runtime(&udf.udf_funcs)?;
            self.main_pipeline.try_add_transformer(|| {
                Ok(TransformUdfScript::new(
                    self.func_ctx.clone(),
                    udf.udf_funcs.clone(),
                    runtimes.clone(),
                ))
            })
        } else {
            self.main_pipeline.try_add_async_transformer(|| {
                Ok(TransformUdfServer::new_retry_wrapper(
                    self.ctx.clone(),
                    self.func_ctx.clone(),
                    udf.udf_funcs.clone(),
                ))
            })
        }
    }
}
