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

use core::ops::Range;
use std::collections::HashSet;

use common_exception::Result;

use crate::filter::SelectExpr;
use crate::filter::Selector;
use crate::DataBlock;
use crate::Evaluator;
use crate::FunctionContext;
use crate::FunctionRegistry;

pub struct FilterExecutor {
    select_expr: SelectExpr,
    func_ctx: FunctionContext,
    true_selection: Vec<u32>,
    false_selection: Vec<u32>,
    has_or: bool,
    projections: Option<HashSet<usize>>,
    max_block_size: usize,
    selection_range: Vec<Range<u32>>,
    fn_registry: &'static FunctionRegistry,
}

impl FilterExecutor {
    pub fn new(
        select_expr: SelectExpr,
        func_ctx: FunctionContext,
        has_or: bool,
        max_block_size: usize,
        projections: Option<HashSet<usize>>,
        fn_registry: &'static FunctionRegistry,
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
            has_or,
            projections,
            max_block_size,
            selection_range: vec![],
            fn_registry,
        }
    }

    pub fn filter(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        let origin_count = data_block.num_rows();
        let result_count = self.select(&data_block)?;
        self.take(data_block, origin_count, result_count)
    }

    pub fn select(&mut self, data_block: &DataBlock) -> Result<usize> {
        let evaluator = Evaluator::new(data_block, &self.func_ctx, self.fn_registry);
        let selector = Selector::new(evaluator, data_block.num_rows());
        selector.select(
            &self.select_expr,
            &mut self.true_selection,
            &mut self.false_selection,
        )
    }

    pub fn take(
        &mut self,
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
            let range_count = self.build_selection_range(result_count);
            data_block.take_ranges(&self.selection_range[0..range_count], result_count)
        } else {
            data_block.take(&self.true_selection[0..result_count], &mut None)
        }
    }

    // Build a range selection from a selection array, return the len of self.range_selection.
    fn build_selection_range(&mut self, count: usize) -> usize {
        if self.selection_range.is_empty() {
            self.selection_range = vec![0..0; self.max_block_size];
        }
        if self.has_or {
            self.true_selection[0..count].sort();
        }
        let selection = &self.true_selection[0..count];
        let mut start = selection[0];
        let mut idx = 1;
        let mut range_count = 0;
        while idx < count {
            if selection[idx] != selection[idx - 1] + 1 {
                self.selection_range[range_count] = start..selection[idx - 1] + 1;
                range_count += 1;
                start = selection[idx];
            }
            idx += 1;
        }
        self.selection_range[range_count] = start..selection[count - 1] + 1;
        range_count += 1;
        range_count
    }

    pub fn mut_true_selection(&mut self) -> &mut [u32] {
        &mut self.true_selection
    }
}
