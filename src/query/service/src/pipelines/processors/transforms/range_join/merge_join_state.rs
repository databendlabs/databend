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

use std::sync::atomic::Ordering;

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::RepeatIndex;
use databend_common_expression::ScalarRef;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::Value;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::UInt64Type;

use crate::pipelines::processors::transforms::range_join::RangeJoinState;
use crate::pipelines::processors::transforms::range_join::filter_block;

impl RangeJoinState {
    pub fn range_join(&self, task_id: usize) -> Result<Vec<DataBlock>> {
        let partition_count = self.partition_count.load(Ordering::SeqCst) as usize;
        if task_id >= partition_count {
            if !self.left_match.read().is_empty() {
                return Ok(vec![self.fill_outer(task_id, true)?]);
            } else if !self.right_match.read().is_empty() {
                return Ok(vec![self.fill_outer(task_id, false)?]);
            }
            return Ok(vec![DataBlock::empty()]);
        }

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

        // Start to execute range join algo
        let left_len = left_sorted_block.num_rows();
        let right_len = right_sort_block.num_rows();

        let left_idx_col = left_sorted_block.get_by_offset(1);
        let left_join_key_col = left_sorted_block.get_by_offset(0);

        let right_idx_col = right_sort_block.get_by_offset(1);
        let right_join_key_col = right_sort_block.get_by_offset(0);

        let mut i = 0;
        let mut j = 0;

        let row_offset = self.row_offset.read();
        let (left_offset, right_offset) = row_offset[task_id];

        let mut result_blocks = Vec::with_capacity(left_len);
        let left_table = self.left_table.read();
        let right_table = self.right_table.read();
        let track_left_outer = !self.left_match.read().is_empty();
        let track_right_outer = !self.right_match.read().is_empty();
        let mut matched_left = Vec::with_capacity(left_len);
        let mut matched_right = Vec::with_capacity(right_len);

        while i < left_len {
            if j == right_len {
                i += 1;
                j = 0;
            }
            if i == left_len {
                break;
            }
            debug_assert!(left_join_key_col.index(i).is_some());
            debug_assert!(right_join_key_col.index(j).is_some());
            let left_scalar = unsafe { left_join_key_col.index_unchecked(i) };
            let right_scalar = unsafe { right_join_key_col.index_unchecked(j) };
            if compare_scalar(
                &left_scalar,
                &right_scalar,
                self.conditions[0].operator.as_str(),
            ) {
                let mut left_result_block = DataBlock::empty();
                let mut right_buffer = Vec::with_capacity(right_len - j);
                let mut right_match_buffer = Vec::with_capacity(right_len - j);
                let mut left_match_index = None;
                if let ScalarRef::Number(NumberScalar::Int64(left)) =
                    unsafe { left_idx_col.index_unchecked(i) }
                {
                    left_match_index = Some((left - 1) as usize);
                    left_result_block = left_table[left_idx].take_compacted_indices(
                        &[RepeatIndex {
                            row: ((left - 1) as usize - left_offset) as u32,
                            count: (right_len - j) as u32,
                        }],
                        right_len - j,
                    )?;
                }
                for k in j..right_len {
                    if let ScalarRef::Number(NumberScalar::Int64(right)) =
                        unsafe { right_idx_col.index_unchecked(k) }
                    {
                        right_buffer.push(((-right - 1) as usize - right_offset) as u32);
                        if track_right_outer {
                            right_match_buffer.push(((-right - 1) as usize) as u64);
                        }
                    }
                }
                if !left_result_block.is_empty() {
                    let right_result_block =
                        right_table[right_idx].take(right_buffer.as_slice())?;
                    // Merge left_result_block and right_result_block
                    left_result_block.merge_block(right_result_block);
                    if track_right_outer {
                        left_result_block.add_entry(BlockEntry::new(
                            Value::Column(Column::Number(NumberColumn::UInt64(
                                right_match_buffer.into(),
                            ))),
                            || {
                                (
                                    databend_common_expression::types::DataType::Number(
                                        databend_common_expression::types::NumberDataType::UInt64,
                                    ),
                                    left_result_block.num_rows(),
                                )
                            },
                        ));
                    }
                    for filter in self.other_conditions.iter() {
                        left_result_block = filter_block(left_result_block, filter)?;
                    }
                    if track_left_outer && !left_result_block.is_empty() {
                        if let Some(left_match_index) = left_match_index {
                            matched_left.push(left_match_index);
                        }
                    }
                    if track_right_outer && !left_result_block.is_empty() {
                        let column = &left_result_block
                            .columns()
                            .last()
                            .unwrap()
                            .value()
                            .try_downcast::<UInt64Type>()
                            .unwrap();
                        if let Value::Column(col) = column {
                            matched_right
                                .extend(UInt64Type::iter_column(col).map(|idx| idx as usize));
                        }
                        left_result_block.pop_columns(1);
                    }
                    result_blocks.push(left_result_block);
                }
                i += 1;
            } else {
                j += 1;
            }
        }

        if track_left_outer && !matched_left.is_empty() {
            let mut left_match = self.left_match.write();
            for idx in matched_left {
                left_match.set(idx, true);
            }
        }

        if track_right_outer && !matched_right.is_empty() {
            let mut right_match = self.right_match.write();
            for idx in matched_right {
                right_match.set(idx, true);
            }
        }

        self.completed_pair.fetch_add(1, Ordering::SeqCst);
        Ok(result_blocks)
    }

    // Used by range join
    fn sort_descriptions(&self, _: bool) -> Vec<SortColumnDescription> {
        let op = &self.conditions[0].operator;
        let asc = match op.as_str() {
            "gt" | "gte" => false,
            "lt" | "lte" => true,
            _ => unreachable!(),
        };
        vec![SortColumnDescription {
            offset: 0,
            asc,
            nulls_first: true,
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
