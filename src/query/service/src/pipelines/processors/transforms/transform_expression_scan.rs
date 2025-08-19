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
    output_buffer: VecDeque<DataBlock>,
    func_ctx: FunctionContext,
}

impl TransformExpressionScan {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        values: Vec<Vec<Expr>>,
        func_ctx: FunctionContext,
    ) -> Box<dyn Processor> {
        BlockingTransformer::create(input, output, TransformExpressionScan {
            values,
            output_buffer: VecDeque::new(),
            func_ctx,
        })
    }
}

impl BlockingTransform for TransformExpressionScan {
    const NAME: &'static str = "TransformExpressionScan";

    fn consume(&mut self, input: DataBlock) -> Result<()> {
        let evaluator = Evaluator::new(&input, &self.func_ctx, &BUILTIN_FUNCTIONS);
        for row in self.values.iter() {
            let mut entries = Vec::with_capacity(row.len());
            for expr in row {
                let entry = BlockEntry::new(evaluator.run(expr)?, || {
                    (expr.data_type().clone(), input.num_rows())
                });
                entries.push(entry);
            }
            self.output_buffer
                .push_back(DataBlock::new(entries, input.num_rows()));
        }
        Ok(())
    }

    fn transform(&mut self) -> Result<Option<DataBlock>> {
        match !self.output_buffer.is_empty() {
            true => Ok(Some(self.output_buffer.pop_front().unwrap())),
            false => Ok(None),
        }
    }
}
