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

use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::filter::FilterExecutor;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;

use crate::BlockingTransform;
use crate::BlockingTransformer;

/// Filter the input [`DataBlock`] with the predicate `expr`.
pub struct TransformFilter<const GC: bool = false> {
    output_data_blocks: VecDeque<DataBlock>,
    max_block_size: usize,
    filter: FilterExecutor,
}

impl<const GC: bool> TransformFilter<GC> {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        expr: Expr,
        projections: BTreeSet<usize>,
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
        BlockingTransformer::create(input, output, TransformFilter::<GC> {
            output_data_blocks: VecDeque::new(),
            max_block_size,
            filter,
        })
    }
}

impl<const GC: bool> BlockingTransform for TransformFilter<GC> {
    const NAME: &'static str = "TransformFilter";

    fn consume(&mut self, input: DataBlock) -> Result<()> {
        let total_rows = input.num_rows();
        if total_rows == 0 {
            return Ok(());
        }

        if total_rows <= self.max_block_size {
            let mut data_block = self.filter.filter(input)?;
            if data_block.num_rows() > 0 {
                if GC {
                    data_block = data_block.maybe_gc();
                }

                self.output_data_blocks.push_back(data_block);
            }
            return Ok(());
        }

        // Account for shared parent buffers across all surviving slices.
        let blocks = input.split_by_rows_no_tail(self.max_block_size);
        drop(input);

        let mut outputs = Vec::with_capacity(blocks.len());
        for block in blocks {
            let data_block = self.filter.filter(block)?;
            if data_block.num_rows() > 0 {
                outputs.push(data_block);
            }
        }

        let outputs = DataBlock::maybe_gc_blocks(outputs);
        self.output_data_blocks.extend(outputs);

        Ok(())
    }

    fn transform(&mut self) -> Result<Option<DataBlock>> {
        match !self.output_data_blocks.is_empty() {
            true => Ok(Some(self.output_data_blocks.pop_front().unwrap())),
            false => Ok(None),
        }
    }
}
