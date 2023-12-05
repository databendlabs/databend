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
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::FunctionContext;
use common_functions::BUILTIN_FUNCTIONS;
use common_sql::optimizer::ColumnSet;

use crate::pipelines::processors::transforms::filter::build_range_selection;
use crate::pipelines::processors::transforms::filter::SelectExpr;
use crate::pipelines::processors::transforms::filter::Selector;

pub struct FilterExecutor {
    select_expr: SelectExpr,
    func_ctx: FunctionContext,
    true_selection: Vec<u32>,
    false_selection: Vec<u32>,
    projections: Option<ColumnSet>,
}

impl FilterExecutor {
    pub fn new(
        select_expr: SelectExpr,
        func_ctx: FunctionContext,
        has_or: bool,
        max_block_size: usize,
        projections: Option<ColumnSet>,
    ) -> Self {
        let true_selection = vec![0; max_block_size];
        let false_selection = if has_or {
            vec![0; max_block_size]
        } else {
            vec![]
        };
        Self {
            select_expr,
            func_ctx,
            true_selection,
            false_selection,
            projections,
        }
    }

    pub fn filter(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        let origin_count = data_block.num_rows();
        let result_count = self.select(&data_block)?;
        self.take(data_block, origin_count, result_count)
    }

    pub fn select(&mut self, data_block: &DataBlock) -> Result<usize> {
        let evaluator = Evaluator::new(data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let selector = Selector::new(evaluator, data_block.num_rows());
        selector.select(
            &self.select_expr,
            &mut self.true_selection,
            &mut self.false_selection,
        )
    }

    pub fn take(
        &self,
        data_block: DataBlock,
        origin_count: usize,
        result_count: usize,
    ) -> Result<DataBlock> {
        let data_block = if let Some(projections) = &self.projections {
            data_block.project(projections)
        } else {
            data_block
        };

        if result_count == origin_count {
            Ok(data_block)
        } else if result_count as f64 > data_block.num_rows() as f64 * 0.8
            && data_block.num_columns() > 1
        {
            let selection_ranges = build_range_selection(&self.true_selection, result_count);
            data_block.take_ranges(&selection_ranges, result_count)
        } else {
            data_block.take(&self.true_selection[0..result_count], &mut None)
        }
    }
}
