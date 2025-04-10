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

use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::Result;

use crate::filter::SelectExpr;
use crate::filter::Selector;
use crate::ColumnSet;
use crate::DataBlock;
use crate::Evaluator;
use crate::Expr;
use crate::FunctionContext;
use crate::FunctionRegistry;
use crate::SelectExprBuilder;
use crate::SELECTIVITY_THRESHOLD;

// FilterExecutor is used to filter `DataBlock` by `SelectExpr`.
pub struct FilterExecutor {
    select_expr: SelectExpr,
    func_ctx: FunctionContext,
    true_selection: Vec<u32>,
    false_selection: Vec<u32>,
    has_or: bool,
    projections: Option<ColumnSet>,
    max_block_size: usize,
    selection_range: Vec<Range<u32>>,
    fn_registry: &'static FunctionRegistry,
    keep_order: bool,
}

impl FilterExecutor {
    pub fn new(
        expr: Expr,
        func_ctx: FunctionContext,
        max_block_size: usize,
        projections: Option<ColumnSet>,
        fn_registry: &'static FunctionRegistry,
        keep_order: bool,
    ) -> Self {
        let (select_expr, has_or) = SelectExprBuilder::new().build(&expr).into();

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
            keep_order,
        }
    }

    // Filter a DataBlock, return the filtered DataBlock.
    pub fn filter(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        let origin_count = data_block.num_rows();
        let result_count = self.select(&data_block)?;
        self.take(data_block, origin_count, result_count)
    }

    // Store the filtered indices of data_block in `true_selection` and return the number of filtered indices.
    pub fn select(&mut self, data_block: &DataBlock) -> Result<usize> {
        let evaluator = Evaluator::new(data_block, &self.func_ctx, self.fn_registry);
        let selector = Selector::new(evaluator, data_block.num_rows());
        selector.select(
            &mut self.select_expr,
            &mut self.true_selection,
            &mut self.false_selection,
        )
    }

    // Generate a new DataBlock from the filtered indices stored in `true_selection`.
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

        // Optimization:
        // (1) If all indices are filtered, return the original `DataBlock` directly.
        // (2) If the number of filtered indices is greater than 80% of the number of DataBlock rows,
        //     it is more efficient to use `take_range` to copy continuous memory, but we need to
        //     construct a `selection_range` before calling `take_range`, so we need to consider whether
        //     it is worth doing it, after testing, the results show that it is more efficient to
        //     construct `selection_range` and then use `take_range` only when the number of columns
        //     is greater than 1.
        // (3) Otherwise, use `take` to generate a new `DataBlock`.
        if result_count == origin_count {
            Ok(data_block)
        } else if result_count as f64 > data_block.num_rows() as f64 * SELECTIVITY_THRESHOLD
            && data_block.num_columns() > 1
        {
            let range_count = self.build_selection_range(result_count);
            data_block.take_ranges(&self.selection_range[0..range_count], result_count)
        } else {
            // If has_or is true, the order of indices may be changed, so sorting is required only
            // when has_or is true.
            if self.keep_order && self.has_or {
                self.true_selection[0..result_count].sort();
            }
            data_block.take_with_optimize_size(&self.true_selection[0..result_count])
        }
    }

    // Build a range selection from a selection array, return the len of self.range_selection.
    fn build_selection_range(&mut self, count: usize) -> usize {
        if self.selection_range.is_empty() {
            self.selection_range = vec![0..0; self.max_block_size];
        }
        // If has_or is true, the order of indices may be changed and the order is not kept, so
        // sorting is required only when has_or is true.
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

    // Update the `true_selection` by `MutableBitmap`, return the number of filtered indices.
    pub fn select_bitmap(&mut self, count: usize, bitmap: MutableBitmap) -> usize {
        let mut true_idx = 0;
        let true_selection = self.true_selection.as_mut_slice();
        unsafe {
            for i in 0..count {
                let idx = *true_selection.get_unchecked(i);
                let ret = bitmap.get(idx as usize);
                *true_selection.get_unchecked_mut(true_idx) = idx;
                true_idx += ret as usize;
            }
        }
        true_idx
    }

    // Initialize the `true_selection` by `MutableBitmap`, return the number of filtered indices.
    pub fn from_bitmap(&mut self, bitmap: MutableBitmap) -> usize {
        let mut true_idx = 0;
        let true_selection = self.true_selection.as_mut_slice();
        for (idx, ret) in bitmap.iter().enumerate() {
            unsafe { *true_selection.get_unchecked_mut(true_idx) = idx as u32 };
            true_idx += ret as usize;
        }
        true_idx
    }

    pub fn true_selection(&mut self) -> &[u32] {
        &self.true_selection
    }

    pub fn mutable_true_selection(&mut self) -> &mut [u32] {
        &mut self.true_selection
    }
}
