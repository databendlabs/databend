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

use common_exception::Result;
use common_pipeline_core::processors::ProcessorPtr;
use common_pipeline_transforms::processors::TransformProfileWrapper;
use common_pipeline_transforms::processors::Transformer;
use common_sql::evaluator::BlockOperator;
use common_sql::evaluator::CompoundBlockOperator;
use common_sql::executor::physical_plans::Lambda;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_lambda(&mut self, lambda: &Lambda) -> Result<()> {
        self.build_pipeline(&lambda.input)?;

        let funcs = lambda.lambda_funcs.clone();
        let op = BlockOperator::LambdaMap { funcs };

        let input_schema = lambda.input.output_schema()?;
        let num_input_columns = input_schema.num_fields();

        self.main_pipeline.add_transform(|input, output| {
            let transform = CompoundBlockOperator::new(
                vec![op.clone()],
                self.func_ctx.clone(),
                num_input_columns,
            );

            if self.enable_profiling {
                Ok(ProcessorPtr::create(TransformProfileWrapper::create(
                    transform,
                    input,
                    output,
                    lambda.plan_id,
                    self.proc_profs.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(Transformer::create(
                    input, output, transform,
                )))
            }
        })?;

        Ok(())
    }
}
