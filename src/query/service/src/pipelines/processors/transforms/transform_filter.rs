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

use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_catalog::plan::AggIndexMeta;
use databend_common_exception::Result;
use databend_common_expression::filter::FilterExecutor;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_transforms::processors::BlockingTransform;
use databend_common_pipeline_transforms::processors::BlockingTransformer;
use databend_common_sql::ColumnSet;

use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::Processor;

/// Filter the input [`DataBlock`] with the predicate `expr`.
pub struct TransformFilter {
    projections: ColumnSet,
    output_data_blocks: VecDeque<DataBlock>,
    max_block_size: usize,
    filter: FilterExecutor,
}

impl TransformFilter {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        expr: Expr,
        projections: ColumnSet,
        func_ctx: FunctionContext,
        max_block_size: usize,
    ) -> Box<dyn Processor> {
        let filter = FilterExecutor::new(
            expr,
            func_ctx,
            max_block_size,
            Some(projections.clone()),
            &BUILTIN_FUNCTIONS,
            false,
        );
        BlockingTransformer::create(input, output, TransformFilter {
            projections,
            output_data_blocks: VecDeque::new(),
            max_block_size,
            filter,
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

        if let Some(num_evals) = num_evals {
            // It's from aggregating index.
            self.output_data_blocks
                .push_back(input.project_with_agg_index(&self.projections, num_evals));
        } else {
            let blocks = input.split_by_rows_no_tail(self.max_block_size);
            for block in blocks.into_iter() {
                let data_block = self.filter.filter(block)?;
                if data_block.num_rows() > 0 {
                    self.output_data_blocks.push_back(data_block);
                }
            }
        }

        Ok(())
    }

    fn transform(&mut self) -> Result<Option<DataBlock>> {
        match !self.output_data_blocks.is_empty() {
            true => Ok(Some(self.output_data_blocks.pop_front().unwrap())),
            false => Ok(None),
        }
    }
}
