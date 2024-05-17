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

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_transforms::processors::BlockingTransform;
use databend_common_pipeline_transforms::processors::BlockingTransformer;

use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::Processor;

pub struct TransformExpressionScan {
    values: Vec<Vec<Expr>>,
    output_data_blocks: VecDeque<DataBlock>,
    func_ctx: FunctionContext,
    max_block_size: usize,
}

impl TransformExpressionScan {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        values: Vec<Vec<Expr>>,
        func_ctx: FunctionContext,
        max_block_size: usize,
    ) -> Box<dyn Processor> {
        BlockingTransformer::create(input, output, TransformExpressionScan {
            values,
            output_data_blocks: VecDeque::new(),
            func_ctx,
            max_block_size,
        })
    }
}

impl BlockingTransform for TransformExpressionScan {
    const NAME: &'static str = "TransformExpressionScan";

    fn consume(&mut self, input: DataBlock) -> Result<()> {
        let evaluator = Evaluator::new(&input, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let mut data_blocks = Vec::with_capacity(input.num_rows() * self.values.len());
        for row in self.values.iter() {
            let mut columns = Vec::with_capacity(row.len());
            for expr in row {
                let result = evaluator.run(expr)?;
                let column = BlockEntry::new(expr.data_type().clone(), result);
                columns.push(column);
            }
            let data_block = DataBlock::new(columns, input.num_rows());
            data_blocks.push(data_block);
        }

        let split_data_blocks =
            DataBlock::concat(&data_blocks)?.split_by_rows_no_tail(self.max_block_size);
        for data_block in split_data_blocks {
            self.output_data_blocks.push_back(data_block)
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
