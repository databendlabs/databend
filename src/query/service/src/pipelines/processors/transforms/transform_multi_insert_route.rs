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

use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FilterExecutor;
use databend_common_expression::FunctionContext;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_transforms::Transform;

/// Appends one boolean column per WHEN condition of a multi-table insert.
///
/// Each condition is evaluated as a filter, so `AND`/`OR` inside it short-circuit like a
/// `WHERE` clause. For INSERT FIRST a condition is evaluated only for the rows that no earlier
/// WHEN has taken, so at most one route column is true for each row.
pub struct TransformMultiInsertRoute {
    conditions: Vec<Expr>,
    executors: Vec<FilterExecutor>,
    is_first: bool,
    func_ctx: FunctionContext,
    max_block_size: usize,
}

impl TransformMultiInsertRoute {
    pub fn new(
        conditions: Vec<Expr>,
        is_first: bool,
        func_ctx: FunctionContext,
        max_block_size: usize,
    ) -> Self {
        let executors = build_executors(&conditions, &func_ctx, max_block_size);
        Self {
            conditions,
            executors,
            is_first,
            func_ctx,
            max_block_size,
        }
    }
}

fn build_executors(
    conditions: &[Expr],
    func_ctx: &FunctionContext,
    max_block_size: usize,
) -> Vec<FilterExecutor> {
    conditions
        .iter()
        .map(|condition| {
            FilterExecutor::new(
                condition.clone(),
                func_ctx.clone(),
                max_block_size,
                None,
                &BUILTIN_FUNCTIONS,
                false,
            )
        })
        .collect()
}

impl Transform for TransformMultiInsertRoute {
    const NAME: &'static str = "MultiInsertRouteTransform";

    fn transform(&mut self, mut block: DataBlock) -> Result<DataBlock> {
        let num_rows = block.num_rows();
        if num_rows > self.max_block_size {
            self.max_block_size = num_rows;
            self.executors = build_executors(&self.conditions, &self.func_ctx, num_rows);
        }

        // INSERT FIRST: the rows that no earlier WHEN has taken, `None` for every row.
        let mut unmatched: Option<Vec<u32>> = None;
        let mut routes = Vec::with_capacity(self.executors.len());
        for executor in self.executors.iter_mut() {
            let mut matched = MutableBitmap::from_len_zeroed(num_rows);
            match &unmatched {
                _ if num_rows == 0 => {}
                None => {
                    let count = executor.select(&block)?;
                    for row in &executor.true_selection()[..count] {
                        matched.set(*row as usize, true);
                    }
                }
                Some(rows) if rows.is_empty() => {}
                Some(rows) => {
                    let rest = block.take(rows.as_slice())?;
                    let count = executor.select(&rest)?;
                    for row in &executor.true_selection()[..count] {
                        matched.set(rows[*row as usize] as usize, true);
                    }
                }
            }
            let matched = Bitmap::from(matched);
            if self.is_first {
                let rows = match unmatched.take() {
                    None => (0..num_rows as u32)
                        .filter(|row| !matched.get_bit(*row as usize))
                        .collect(),
                    Some(rows) => rows
                        .into_iter()
                        .filter(|row| !matched.get_bit(*row as usize))
                        .collect(),
                };
                unmatched = Some(rows);
            }
            routes.push(matched);
        }

        for matched in routes {
            block.add_column(Column::Boolean(matched));
        }
        Ok(block)
    }
}
