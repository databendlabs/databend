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
use databend_common_sql::executor::physical_plans::AsyncFunction;

use crate::pipelines::processors::transforms::TransformSequenceNextval;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_async_function(&mut self, async_function: &AsyncFunction) -> Result<()> {
        self.build_pipeline(&async_function.input)?;

        if async_function.func_name == "nextval" {
            self.main_pipeline.add_async_transformer(|| {
                TransformSequenceNextval::new(
                    self.ctx.clone(),
                    &async_function.arguments[0],
                    &async_function.return_type,
                )
            })
        } else {
            unreachable!()
        }
        Ok(())
    }
}
