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

use std::sync::Arc;

use databend_common_catalog::plan::AggIndexMeta;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_transforms::processors::BlockingTransform;
use databend_common_pipeline_transforms::processors::BlockingTransformer;
use databend_common_sql::optimizer::ColumnSet;

use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::Processor;

/// Filter the input [`DataBlock`] with the predicate `expr`.
pub struct TransformFilter {
    expr: Expr,
    projections: ColumnSet,
    func_ctx: FunctionContext,
    output_data: Option<DataBlock>,
}

impl TransformFilter {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        expr: Expr,
        projections: ColumnSet,
        func_ctx: FunctionContext,
    ) -> Box<dyn Processor> {
        BlockingTransformer::create(input, output, TransformFilter {
            expr,
            projections,
            func_ctx,
            output_data: None,
        })
    }
}

impl BlockingTransform for TransformFilter {
    const NAME: &'static str = "TransformFilter";

    fn consume(&mut self, input: DataBlock) -> Result<()> {
        let num_evals = input
            .get_meta()
            .and_then(AggIndexMeta::downcast_ref_from)
            .map(|a| a.num_evals);

        let data_block = if let Some(num_evals) = num_evals {
            // It's from aggregating index.
            input.project_with_agg_index(&self.projections, num_evals)
        } else {
            let evaluator = Evaluator::new(&input, &self.func_ctx, &BUILTIN_FUNCTIONS);
            let filter = evaluator
                .run(&self.expr)?
                .try_downcast::<BooleanType>()
                .unwrap();
            let data_block = input.project(&self.projections);
            data_block.filter_boolean_value(&filter)?
        };

        if data_block.num_rows() > 0 {
            self.output_data = Some(data_block)
        }

        Ok(())
    }

    fn transform(&mut self) -> Result<Option<DataBlock>> {
        if self.output_data.is_none() {
            return Ok(None);
        }
        let data_block = self.output_data.take().unwrap();
        Ok(Some(data_block))
    }
}
