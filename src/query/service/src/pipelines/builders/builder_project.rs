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
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FunctionContext;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sinks::EmptySink;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::executor::physical_plans::ProjectSet;
use databend_common_sql::ColumnBinding;

use crate::pipelines::processors::transforms::TransformSRF;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub fn build_result_projection(
        func_ctx: &FunctionContext,
        input_schema: DataSchemaRef,
        result_columns: &[ColumnBinding],
        pipeline: &mut Pipeline,
        ignore_result: bool,
    ) -> Result<()> {
        if ignore_result {
            return pipeline.add_sink(|input| Ok(ProcessorPtr::create(EmptySink::create(input))));
        }

        let mut projections = Vec::with_capacity(result_columns.len());

        for column_binding in result_columns {
            let index = column_binding.index;
            projections.push(input_schema.index_of(index.to_string().as_str())?);
        }
        let num_input_columns = input_schema.num_fields();
        pipeline.add_transformer(|| {
            CompoundBlockOperator::new(
                vec![BlockOperator::Project {
                    projection: projections.clone(),
                }],
                func_ctx.clone(),
                num_input_columns,
            )
        });

        Ok(())
    }

    pub(crate) fn build_project_set(&mut self, project_set: &ProjectSet) -> Result<()> {
        self.build_pipeline(&project_set.input)?;

        let srf_exprs = project_set
            .srf_exprs
            .iter()
            .map(|(expr, _)| expr.as_expr(&BUILTIN_FUNCTIONS))
            .collect::<Vec<_>>();
        let max_block_size = self.settings.get_max_block_size()? as usize;

        self.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(TransformSRF::try_create(
                input,
                output,
                self.func_ctx.clone(),
                project_set.projections.clone(),
                srf_exprs.clone(),
                max_block_size,
            )))
        })
    }
}
