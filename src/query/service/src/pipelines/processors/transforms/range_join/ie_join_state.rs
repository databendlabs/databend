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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberColumnBuilder;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::types::UInt64Type;
use common_expression::types::ValueType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_expression::ScalarRef;
use common_expression::SortColumnDescription;
use common_expression::Value;
use common_expression::ValueRef;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_transforms::processors::transforms::sort_merge;
use common_sql::executor::RangeJoin;

use crate::pipelines::processors::transforms::range_join::ie_join_util::filter_block;
use crate::pipelines::processors::transforms::range_join::ie_join_util::order_match;
use crate::pipelines::processors::transforms::range_join::ie_join_util::probe_l1;
use crate::pipelines::processors::transforms::range_join::RangeJoinState;

pub(crate) struct IEJoinState {
    l1_data_type: DataType,
    // Sort description for L1
    pub(crate) l1_sort_descriptions: Vec<SortColumnDescription>,
    // Sort description for L2
    pub(crate) l2_sort_descriptions: Vec<SortColumnDescription>,
    // true is asc
    l1_order: bool,
    // data schema of sorted blocks
    pub(crate) data_schema: DataSchemaRef,
}

impl IEJoinState {
    pub(crate) fn new(ie_join: &RangeJoin) -> Self {
        let mut fields = Vec::with_capacity(4);
        let l1_data_type = ie_join.conditions[0]
            .left_expr
            .as_expr(&BUILTIN_FUNCTIONS)
            .data_type()
            .clone();
        let l2_data_type = ie_join.conditions[1]
            .left_expr
            .as_expr(&BUILTIN_FUNCTIONS)
            .data_type()
            .clone();
        fields.push(DataField::new("_ie_join_key_1", l1_data_type.clone()));
        fields.push(DataField::new("_ie_join_key_2", l2_data_type.clone()));
        fields.push(DataField::new(
            "_tuple_id",
            DataType::Number(NumberDataType::Int64),
        ));
        let pos_field = DataField::new("_pos", DataType::Number(NumberDataType::UInt64));
        fields.push(pos_field);

        let l1_order = !matches!(ie_join.conditions[0].operator.as_str(), "gt" | "gte");
        let l2_order = matches!(ie_join.conditions[1].operator.as_str(), "gt" | "gte");
        let l1_sort_descriptions = vec![
            SortColumnDescription {
                offset: 0,
                asc: l1_order,
                nulls_first: false,
                is_nullable: l1_data_type.is_nullable(),
            },
            SortColumnDescription {
                offset: 1,
                asc: l2_order,
                nulls_first: false,
                is_nullable: l2_data_type.is_nullable(),
            },
            SortColumnDescription {
                offset: 2,
                asc: false,
                nulls_first: false,
                is_nullable: false,
            },
        ];

        let l2_sort_descriptions = vec![
            SortColumnDescription {
                offset: 1,
                asc: l2_order,
                nulls_first: false,
                is_nullable: l2_data_type.is_nullable(),
            },
            SortColumnDescription {
                offset: 0,
                asc: l1_order,
                nulls_first: false,
                is_nullable: l1_data_type.is_nullable(),
            },
            // `_tuple_id` column
            SortColumnDescription {
                offset: 2,
                asc: false,
                nulls_first: false,
                is_nullable: false,
            },
        ];

        IEJoinState {
            l1_data_type,
            l1_sort_descriptions,
            l2_sort_descriptions,
            l1_order,
            data_schema: DataSchemaRefExt::create(fields),
        }
    }

    fn intersection(&self, left_block: &DataBlock, right_block: &DataBlock) -> bool {
        let left_len = left_block.num_rows();
        let right_len = right_block.num_rows();
        let left_l1_column = left_block.columns()[0]
            .value
            .convert_to_full_column(&self.l1_data_type, left_len);
        let right_l1_column = right_block.columns()[0]
            .value
            .convert_to_full_column(&self.l1_data_type, right_len);
        // If `left_l1_column` and `right_l1_column` have intersection && `left_l2_column` and `right_l2_column` have intersection, return true
        let (left_l1_min, left_l1_max, right_l1_min, right_l1_max) = match self.l1_order {
            true => {
                // l1 is asc
                (
                    left_l1_column.index(0).unwrap(),
                    left_l1_column.index(left_len - 1).unwrap(),
                    right_l1_column.index(0).unwrap(),
                    right_l1_column.index(right_len - 1).unwrap(),
                )
            }
            false => {
                // l1 is desc
                (
                    left_l1_column.index(left_len - 1).unwrap(),
                    left_l1_column.index(0).unwrap(),
                    right_l1_column.index(right_len - 1).unwrap(),
                    right_l1_column.index(0).unwrap(),
                )
            }
        };
        match self.l1_order {
            true => {
                // if l1_order is asc, then op1 is < / <=
                left_l1_min <= right_l1_max
            }
            false => {
                // If l1_order is desc, then op is > / >=
                right_l1_min <= left_l1_max
            }
        }
    }
}

impl RangeJoinState {
    pub fn ie_join(&self, task_id: usize) -> Result<Vec<DataBlock>> {
        let block_size = self.ctx.get_settings().get_max_block_size()? as usize;
        let tasks = self.tasks.read();
        let (left_idx, right_idx) = tasks[task_id];
        let ie_join_state = self.ie_join_state.as_ref().unwrap();
        let left_sorted_blocks = self.left_sorted_blocks.read();
        let right_sorted_blocks = self.right_sorted_blocks.read();
        let l1_sorted_block = DataBlock::sort(
            &left_sorted_blocks[left_idx],
            &ie_join_state.l1_sort_descriptions,
            None,
        )?;
        let right_block = DataBlock::sort(
            &right_sorted_blocks[right_idx],
            &ie_join_state.l1_sort_descriptions,
            None,
        )?;
        if !ie_join_state.intersection(&l1_sorted_block, &right_block) {
            return Ok(vec![DataBlock::empty()]);
        }
        let mut left_sorted_blocks = vec![l1_sorted_block, right_block];

        let data_schema = DataSchemaRefExt::create(
            ie_join_state.data_schema.fields().as_slice()[0..self.conditions.len() + 1].to_vec(),
        );

        left_sorted_blocks = sort_merge(
            data_schema,
            block_size,
            ie_join_state.l1_sort_descriptions.clone(),
            left_sorted_blocks,
        )?;

        // Add a column at the end of `left_sorted_blocks`, named `_pos`, which is used to record the position of the block in the original table
        let mut count: usize = 0;
        for block in left_sorted_blocks.iter_mut() {
            // Generate column with value [1..block.size()]
            let mut column_builder =
                NumberColumnBuilder::with_capacity(&NumberDataType::UInt64, block.num_rows());
            for idx in count..(count + block.num_rows()) {
                column_builder.push(NumberScalar::UInt64(idx as u64));
            }
            block.add_column(BlockEntry::new(
                DataType::Number(NumberDataType::UInt64),
                Value::Column(Column::Number(column_builder.build())),
            ));
            count += block.num_rows();
        }
        // Merge `left_sorted_blocks` to one block
        let mut merged_blocks = DataBlock::concat(&left_sorted_blocks)?;
        // extract the second column
        let l1 = &merged_blocks.columns()[0].value.convert_to_full_column(
            self.conditions[0]
                .left_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .data_type(),
            merged_blocks.num_rows(),
        );
        let l1_index_column = merged_blocks.columns()[2].value.convert_to_full_column(
            &DataType::Number(NumberDataType::UInt64),
            merged_blocks.num_rows(),
        );

        let mut l2_sorted_blocks = Vec::with_capacity(left_sorted_blocks.len());
        for block in left_sorted_blocks.iter() {
            l2_sorted_blocks.push(DataBlock::sort(
                block,
                &ie_join_state.l2_sort_descriptions,
                None,
            )?);
        }
        merged_blocks = DataBlock::concat(&sort_merge(
            ie_join_state.data_schema.clone(),
            block_size,
            ie_join_state.l2_sort_descriptions.clone(),
            l2_sorted_blocks,
        )?)?;

        // The pos col of l2 sorted blocks is permutation array
        let mut p_array = Vec::with_capacity(merged_blocks.num_rows());
        let column = &merged_blocks
            .columns()
            .last()
            .unwrap()
            .value
            .try_downcast::<UInt64Type>()
            .unwrap();
        if let ValueRef::Column(col) = column.as_ref() {
            for val in UInt64Type::iter_column(&col) {
                p_array.push(val)
            }
        }
        // Initialize bit_array
        let bit_array = Bitmap::new_constant(false, p_array.len()).make_mut();

        let l2 = &merged_blocks.columns()[1].value.convert_to_full_column(
            self.conditions[0]
                .right_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .data_type(),
            merged_blocks.num_rows(),
        );

        drop(left_sorted_blocks);

        Ok(vec![self.ie_join_finalize(
            l1,
            l2,
            l1_index_column,
            &p_array,
            bit_array,
            task_id,
        )?])
    }

    pub fn ie_join_finalize(
        &self,
        l1: &Column,
        l2: &Column,
        l1_index_column: Column,
        p_array: &[u64],
        mut bit_array: MutableBitmap,
        task_id: usize,
    ) -> Result<DataBlock> {
        let block_size = self.ctx.get_settings().get_max_block_size()? as usize;
        let row_offset = self.row_offset.read();
        let (left_offset, right_offset) = row_offset[task_id];
        let tasks = self.tasks.read();
        let (left_idx, right_idx) = tasks[task_id];
        let len = p_array.len();
        let mut left_buffer = Vec::with_capacity(block_size);
        let mut right_buffer = Vec::with_capacity(block_size);
        let mut off1;
        let mut off2 = 0;
        for (idx, p) in p_array.iter().enumerate() {
            if let ScalarRef::Number(NumberScalar::Int64(val)) =
                unsafe { l1_index_column.index_unchecked(*p as usize) }
            {
                if val < 0 {
                    continue;
                }
            }
            while off2 < len {
                let order =
                    unsafe { l2.index_unchecked(idx) }.cmp(&unsafe { l2.index_unchecked(off2) });
                if !order_match(&self.conditions[1].operator, order) {
                    break;
                }
                let p2 = p_array[off2];
                if let ScalarRef::Number(NumberScalar::Int64(val)) =
                    unsafe { l1_index_column.index_unchecked(p2 as usize) }
                {
                    if val < 0 {
                        bit_array.set(p2 as usize, true);
                    }
                }
                off2 += 1;
            }
            off1 = probe_l1(l1, *p as usize, &self.conditions[0].operator);
            if off1 >= len {
                continue;
            }
            let mut j = off1;
            while j < len {
                if bit_array.get(j) {
                    // right, left
                    if let ScalarRef::Number(NumberScalar::Int64(right)) =
                        unsafe { l1_index_column.index_unchecked(j) }
                    {
                        right_buffer.push((-right - 1) as usize - right_offset);
                    }
                    if let ScalarRef::Number(NumberScalar::Int64(left)) =
                        unsafe { l1_index_column.index_unchecked(*p as usize) }
                    {
                        left_buffer.push((left - 1) as usize - left_offset);
                    }
                }
                j += 1;
            }
        }
        if left_buffer.is_empty() {
            return Ok(DataBlock::empty());
        }
        let left_table = self.left_table.read();
        let right_table = self.right_table.read();
        let mut indices = Vec::with_capacity(left_buffer.len());
        for res in left_buffer.iter() {
            indices.push((0u32, *res as u32, 1usize));
        }
        let mut left_result_block =
            DataBlock::take_blocks(&left_table[left_idx..left_idx + 1], &indices, indices.len());
        indices.clear();
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
        Ok(left_result_block)
    }
}
