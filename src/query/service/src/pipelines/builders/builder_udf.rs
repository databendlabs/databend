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
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::ProcessorProfileWrapper;
use databend_common_sql::executor::physical_plans::Udf;

use crate::pipelines::processors::transforms::TransformUdf;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_udf(&mut self, udf: &Udf) -> Result<()> {
        self.build_pipeline(&udf.input)?;

        self.main_pipeline.add_transform(|input, output| {
            let transform = TransformUdf::try_create(
                self.func_ctx.clone(),
                udf.udf_funcs.clone(),
                input,
                output,
            )?;
            if self.enable_profiling {
                Ok(ProcessorPtr::create(ProcessorProfileWrapper::create(
                    transform,
                    udf.plan_id,
                    self.proc_profs.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(transform))
            }
        })
    }
}
