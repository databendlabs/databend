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
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::executor::physical_plans::EvalScalar;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_eval_scalar(&mut self, eval_scalar: &EvalScalar) -> Result<()> {
        self.build_pipeline(&eval_scalar.input)?;

        let input_schema = eval_scalar.input.output_schema()?;
        let exprs = eval_scalar
            .exprs
            .iter()
            .map(|(scalar, _)| scalar.as_expr(&BUILTIN_FUNCTIONS))
            .collect::<Vec<_>>();

        if exprs.is_empty() {
            return Ok(());
        }

        let op = BlockOperator::Map {
            exprs,
            projections: Some(eval_scalar.projections.clone()),
        };

        let num_input_columns = input_schema.num_fields();

        self.main_pipeline.add_transformer(|| {
            CompoundBlockOperator::new(vec![op.clone()], self.func_ctx.clone(), num_input_columns)
        });

        Ok(())
    }
}
