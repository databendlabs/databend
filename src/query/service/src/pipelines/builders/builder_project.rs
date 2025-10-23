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
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sinks::EmptySink;
use databend_common_pipeline_transforms::blocks::CompoundBlockOperator;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::ColumnBinding;

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

            #[cfg(debug_assertions)]
            {
                let f = input_schema.field_with_name(index.to_string().as_str())?;
                assert_eq!(
                    f.data_type(),
                    column_binding.data_type.as_ref(),
                    "Result projection schema mismatch"
                );
            }
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
}
