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
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::SortColumnDescription;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::pipelines::processors::transforms::range_join::filter_block;
use crate::pipelines::processors::transforms::range_join::RangeJoinState;

impl RangeJoinState {
    pub fn merge_join(&self, task_id: usize) -> Result<Vec<DataBlock>> {
        let tasks = self.tasks.read();
        let (left_idx, right_idx) = tasks[task_id];
        let left_sorted_blocks = self.left_sorted_blocks.read();
        let right_sorted_blocks = self.right_sorted_blocks.read();

        let left_sort_descriptions = self.sort_descriptions(true);
        let left_sorted_block =
            DataBlock::sort(&left_sorted_blocks[left_idx], &left_sort_descriptions, None)?;

        let right_sort_descriptions = self.sort_descriptions(false);
        let right_sort_block = DataBlock::sort(
            &right_sorted_blocks[right_idx],
            &right_sort_descriptions,
            None,
        )?;

        // Start to execute merge join algo
        let left_len = left_sorted_block.num_rows();
        let right_len = right_sort_block.num_rows();

        let left_idx_col = &left_sorted_block.columns()[1]
            .value
            .convert_to_full_column(&DataType::Number(NumberDataType::Int64), left_len);
        let left_join_key_col = &left_sorted_block.columns()[0].value.convert_to_full_column(
            self.conditions[0]
                .left_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .data_type(),
            left_sorted_block.num_rows(),
        );

        let right_idx_col = &right_sort_block.columns()[1]
            .value
            .convert_to_full_column(&DataType::Number(NumberDataType::Int64), right_len);
        let right_join_key_col = &right_sort_block.columns()[0].value.convert_to_full_column(
            self.conditions[0]
                .right_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .data_type(),
            right_sort_block.num_rows(),
        );

        let mut i = 0;
        let mut j = 0;

        let row_offset = self.row_offset.read();
        let (left_offset, right_offset) = row_offset[task_id];

        let mut result_blocks = Vec::with_capacity(left_len);
        let left_table = self.left_table.read();
        let right_table = self.right_table.read();

        while i < left_len {
            if j == right_len {
                i += 1;
                j = 0;
            }
            if i == left_len {
                break;
            }
            let left_scalar = unsafe { left_join_key_col.index_unchecked(i) };
            let right_scalar = unsafe { right_join_key_col.index_unchecked(j) };
            if compare_scalar(
                &left_scalar,
                &right_scalar,
                self.conditions[0].operator.as_str(),
            ) {
                let mut left_result_block = DataBlock::empty();
                let mut right_buffer = Vec::with_capacity(right_len - j);
                if let ScalarRef::Number(NumberScalar::Int64(left)) =
                    unsafe { left_idx_col.index_unchecked(i) }
                {
                    left_result_block = left_table[left_idx].take_compacted_indices(
                        &[(
                            ((left - 1) as usize - left_offset) as u32,
                            (right_len - j) as u32,
                        )],
                        right_len - j,
                    )?;
                }
                for k in j..right_len {
                    if let ScalarRef::Number(NumberScalar::Int64(right)) =
                        unsafe { right_idx_col.index_unchecked(k) }
                    {
                        right_buffer.push((-right - 1) as usize - right_offset);
                    }
                }
                if !left_result_block.is_empty() {
                    let mut indices = Vec::with_capacity(right_buffer.len());
                    for res in right_buffer.iter() {
                        indices.push((0u32, *res as u32, 1usize));
                    }
                    let right_result_block = DataBlock::take_blocks(
                        &right_table[right_idx..right_idx + 1],
                        &indices,
                        indices.len(),
                    );
                    // Merge left_result_block and right_result_block
                    for col in right_result_block.columns() {
                        left_result_block.add_column(col.clone());
                    }
                    for filter in self.other_conditions.iter() {
                        left_result_block = filter_block(left_result_block, filter)?;
                    }
                    result_blocks.push(left_result_block);
                }
                i += 1;
            } else {
                j += 1;
            }
        }
        Ok(result_blocks)
    }

    // Used by merge join
    fn sort_descriptions(&self, left: bool) -> Vec<SortColumnDescription> {
        let op = &self.conditions[0].operator;
        let asc = match op.as_str() {
            "gt" | "gte" => false,
            "lt" | "lte" => true,
            _ => unreachable!(),
        };
        let is_nullable = if left {
            self.conditions[0]
                .left_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .data_type()
                .is_nullable()
        } else {
            self.conditions[0]
                .right_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .data_type()
                .is_nullable()
        };
        vec![SortColumnDescription {
            offset: 0,
            asc,
            nulls_first: true,
            is_nullable,
        }]
    }
}

fn compare_scalar(left: &ScalarRef, right: &ScalarRef, op: &str) -> bool {
    if left.is_null() || right.is_null() {
        return false;
    }

    match op {
        "gte" => left.cmp(right) != std::cmp::Ordering::Less,
        "gt" => left.cmp(right) == std::cmp::Ordering::Greater,
        "lte" => left.cmp(right) != std::cmp::Ordering::Greater,
        "lt" => left.cmp(right) == std::cmp::Ordering::Less,
        _ => unreachable!(),
    }
}
