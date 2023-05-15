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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::parquet::fallible_streaming_iterator::Convert;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberColumnBuilder;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::types::UInt64Type;
use common_expression::types::ValueType;
use common_expression::with_number_mapped_type;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_expression::Evaluator;
use common_expression::FunctionContext;
use common_expression::RemoteExpr;
use common_expression::ScalarRef;
use common_expression::SortColumnDescription;
use common_expression::Value;
use common_expression::ValueRef;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_transforms::processors::transforms::sort_merge_by_data_type;
use common_pipeline_transforms::processors::transforms::Compactor;
use common_sql::executor::IEJoin;
use common_sql::executor::IEJoinCondition;
use common_sql::plans::JoinType;
use parking_lot::RwLock;

pub struct IEJoinState {
    // L1: sort by the first join key
    sorted_blocks: RwLock<Vec<DataBlock>>,
    // L2: sort `sorted_blocks` again, by the second join key
    l2_sorted_blocks: RwLock<Vec<DataBlock>>,
    // True is asc
    l1_order: bool,
    l2_order: bool,
    eq_off: u8,
    // Left table result buffer
    left_buffer: RwLock<Vec<usize>>,
    // Right table result buffer
    right_buffer: RwLock<Vec<usize>>,
    // permutation array
    p_array: RwLock<Vec<u64>>,
    // Bit array
    bit_array: RwLock<MutableBitmap>,
    // data schema of sorted blocks
    data_schema: DataSchemaRef,
    // Sort description for L1
    sort_columns_descriptions: Vec<SortColumnDescription>,
    left_table: RwLock<DataBlock>,
    right_table: RwLock<DataBlock>,
    join_type: JoinType,
    conditions: Vec<IEJoinCondition>,
    other_conditions: Vec<RemoteExpr>,
    right_finished: RwLock<bool>,
}

impl IEJoinState {
    pub fn new(ie_join: &IEJoin) -> Self {
        let mut fields = vec![];
        let mut is_nullable = ie_join.conditions[0]
            .left_expr
            .as_expr(&BUILTIN_FUNCTIONS)
            .data_type()
            .is_nullable();
        let mut l1_order = matches!(ie_join.conditions[0].operator.as_str(), "gt" | "gte");
        let mut l2_order = !matches!(ie_join.conditions[1].operator.as_str(), "gt" | "gte");
        for (idx, condition) in ie_join.conditions.iter().enumerate() {
            let field = DataField::new(
                format!("_ie_join_key_{idx}").as_str(),
                condition
                    .left_expr
                    .as_expr(&BUILTIN_FUNCTIONS)
                    .data_type()
                    .clone(),
            );
            fields.push(field);
        }
        let pos_field = DataField::new("_pos", DataType::Number(NumberDataType::UInt64));
        fields.push(pos_field);
        let eq_off = if matches!(ie_join.conditions[0].operator.as_str(), "lte" | "gte")
            && matches!(ie_join.conditions[1].operator.as_str(), "lte" | "gte")
        {
            0
        } else {
            1
        };
        IEJoinState {
            sorted_blocks: Default::default(),
            l2_sorted_blocks: Default::default(),
            l1_order,
            l2_order,
            eq_off,
            left_buffer: RwLock::new(Vec::with_capacity(65535)),
            right_buffer: RwLock::new(Vec::with_capacity(65535)),
            p_array: Default::default(),
            bit_array: Default::default(),
            data_schema: DataSchemaRefExt::create(fields),
            sort_columns_descriptions: vec![SortColumnDescription {
                offset: 1,
                asc: l1_order,
                nulls_first: false,
                is_nullable,
            }],
            left_table: RwLock::new(DataBlock::empty()),
            right_table: RwLock::new(DataBlock::empty()),
            join_type: ie_join.join_type.clone(),
            conditions: ie_join.conditions.clone(),
            other_conditions: ie_join.other_conditions.clone(),
            right_finished: RwLock::new(false),
        }
    }

    pub fn set_right_finished(&self) {
        let mut right_finished = self.right_finished.write();
        *right_finished = true;
    }

    pub fn sink_right(&self, block: DataBlock) -> Result<()> {
        // First, sink block to right table
        let mut current_rows;
        {
            let mut right_table = self.right_table.write();
            current_rows = right_table.num_rows();
            *right_table = DataBlock::concat(&vec![right_table.clone(), block.clone()])?;
        }
        // Second, generate keys block by join keys
        // For example, if join keys are [t1.a + t1.b, t1.c], then key blocks will contain two columns: [t1.a + t1.b, t1.c]
        // We can get the key blocks by evaluating the join keys expressions on the block
        let mut columns = vec![];
        // First, generate idx column from current_rows to current_rows + block.num_rows()
        let mut column_builder = ColumnBuilder::with_capacity(
            &DataType::Number(NumberDataType::UInt64),
            block.num_rows(),
        );
        for idx in current_rows..(current_rows + block.num_rows()) {
            column_builder.push(ScalarRef::Number(NumberScalar::UInt64(idx as u64)));
        }
        columns.push(column_builder.build());
        for (idx, condition) in self.conditions.iter().enumerate() {
            let func_ctx = FunctionContext::default();
            let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
            let expr = condition.right_expr.as_expr(&BUILTIN_FUNCTIONS);
            let column = evaluator
                .run(&expr)?
                .convert_to_full_column(expr.data_type(), block.num_rows());
            columns.push(column);
        }
        // Last, add a marker column to indicate the block is from right table
        let mut marker_column = ColumnBuilder::repeat(
            &ScalarRef::Boolean(false),
            block.num_rows(),
            &DataType::Boolean,
        )
        .build();
        columns.push(marker_column);
        // Sort columns by the first column
        let keys_block = DataBlock::new_from_columns(columns);
        let sorted_keys_block =
            DataBlock::sort(&keys_block, &self.sort_columns_descriptions, None)?;
        {
            let mut sorted_blocks = self.sorted_blocks.write();
            sorted_blocks.push(sorted_keys_block);
        }
        Ok(())
    }

    // Todo(xudong): move some vars to state and refine code
    pub fn sink_left(&self, block: DataBlock) -> Result<()> {
        // First, sink block to left table
        let mut current_rows = 0;
        {
            let mut left_table = self.left_table.write();
            current_rows = left_table.num_rows();
            *left_table = DataBlock::concat(&vec![left_table.clone(), block.clone()])?;
        }
        // Second, generate keys block by join keys
        // For example, if join keys are [t1.a + t2.b, t1.c], then key blocks will contain two columns: [t1.a + t2.b, t1.c]
        // We can get the key blocks by evaluating the join keys expressions on the block
        let mut columns = vec![];
        // First, generate idx column from current_rows to current_rows + block.num_rows()
        let mut column_builder = ColumnBuilder::with_capacity(
            &DataType::Number(NumberDataType::UInt64),
            block.num_rows(),
        );
        for idx in current_rows..(current_rows + block.num_rows()) {
            column_builder.push(ScalarRef::Number(NumberScalar::UInt64(idx as u64)));
        }
        columns.push(column_builder.build());
        dbg!(&columns);
        for (idx, condition) in self.conditions.iter().enumerate() {
            let func_ctx = FunctionContext::default();
            let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
            let expr = condition.left_expr.as_expr(&BUILTIN_FUNCTIONS);
            let column = evaluator
                .run(&expr)?
                .convert_to_full_column(expr.data_type(), block.num_rows());
            columns.push(column);
        }
        // Last, add a marker column to indicate the block is from right table
        let mut marker_column = ColumnBuilder::repeat(
            &ScalarRef::Boolean(true),
            block.num_rows(),
            &DataType::Boolean,
        )
        .build();
        columns.push(marker_column);
        // Sort columns by the first column
        let keys_block = DataBlock::new_from_columns(columns);
        let sorted_keys_block =
            DataBlock::sort(&keys_block, &self.sort_columns_descriptions, None)?;
        {
            let mut sorted_blocks = self.sorted_blocks.write();
            sorted_blocks.push(sorted_keys_block);
        }
        Ok(())
    }

    pub fn merge_sort(&self) -> Result<()> {
        // Merge sort `sorted_blocks`
        let mut sorted_blocks = self.sorted_blocks.write();
        // Create `SortMergeCompactor` then compact
        let data_schema = DataSchemaRefExt::create(
            self.data_schema.fields().as_slice()[0..self.conditions.len()].to_vec(),
        );
        *sorted_blocks = sort_merge_by_data_type(
            self.conditions[0]
                .left_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .data_type(),
            data_schema,
            sorted_blocks.len(),
            self.sort_columns_descriptions.clone(),
            &sorted_blocks,
        )?;
        // Add a column at the end of `sorted_blocks`, named `_pos`, which is used to record the position of the block in the original table
        let mut count: usize = 1;
        for block in sorted_blocks.iter_mut() {
            // Generate column with value [1..block.size()]
            let mut column_builder =
                NumberColumnBuilder::with_capacity(&NumberDataType::UInt64, block.num_rows());
            for idx in count..(count + block.num_rows()) {
                column_builder.push(NumberScalar::UInt64(idx as u64));
            }
            block.add_column(BlockEntry {
                data_type: DataType::Number(NumberDataType::UInt64),
                value: Value::Column(Column::Number(column_builder.build())),
            });
            count += block.num_rows();
        }
        // Sort `sorted_blocks` by the second join key
        // todo(xudong): leverage multi threads and sort partial then merge
        let is_nullable = self.conditions[0]
            .right_expr
            .as_expr(&BUILTIN_FUNCTIONS)
            .data_type()
            .is_nullable();
        let mut l2_sorted_blocks = self.l2_sorted_blocks.write();
        *l2_sorted_blocks = sort_merge_by_data_type(
            self.conditions[0]
                .right_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .data_type(),
            self.data_schema.clone(),
            sorted_blocks.len(),
            vec![SortColumnDescription {
                offset: 2,
                asc: self.l2_order,
                nulls_first: false,
                is_nullable,
            }],
            &sorted_blocks,
        )?;
        // The pos col of l2 sorted blocks is permutation array
        let mut p_array = self.p_array.write();
        for block in l2_sorted_blocks.iter() {
            let column = &block
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
        }
        // l2_sorted_blocks.clear();
        // Initialize bit_array
        let mut bit_array = self.bit_array.write();
        bit_array.extend_constant(p_array.len(), false);
        dbg!(&sorted_blocks);
        dbg!(&l2_sorted_blocks);
        Ok(())
    }

    pub fn finalize(&self) -> Result<DataBlock> {
        let len = self.p_array.read().len();
        let l1 = &self.sorted_blocks.read()[0];
        let mut left_buffer = self.left_buffer.write();
        left_buffer.clear();
        let mut right_buffer = self.right_buffer.write();
        right_buffer.clear();
        let l1_index_column = l1
            .columns()
            .first()
            .unwrap()
            .value
            .convert_to_full_column(&DataType::Number(NumberDataType::UInt64), len);
        let l1_marker_column = l1
            .columns()
            .last()
            .unwrap()
            .value
            .convert_to_full_column(&DataType::Boolean, len);
        for (idx, p) in self.p_array.read().iter().enumerate() {
            let mut bit_array = self.bit_array.write();
            bit_array.set(idx, true);
            for j in (p + self.eq_off as u64)..len as u64 {
                if bit_array.get(j as usize) {
                    match (
                        l1_marker_column.index(j as usize).unwrap(),
                        l1_marker_column.index(*p as usize).unwrap(),
                    ) {
                        (ScalarRef::Boolean(false), ScalarRef::Boolean(true)) => {
                            // right, left
                            if let ScalarRef::Number(NumberScalar::UInt64(left)) =
                                l1_index_column.index(*p as usize).unwrap()
                            {
                                left_buffer.push(left as usize);
                            }
                            if let ScalarRef::Number(NumberScalar::UInt64(right)) =
                                l1_index_column.index(j as usize).unwrap()
                            {
                                right_buffer.push(right as usize);
                            }
                        }
                        (ScalarRef::Boolean(true), ScalarRef::Boolean(false)) => {
                            // left, right
                            if let ScalarRef::Number(NumberScalar::UInt64(left)) =
                                l1_index_column.index(j as usize).unwrap()
                            {
                                left_buffer.push(left as usize);
                            }
                            if let ScalarRef::Number(NumberScalar::UInt64(right)) =
                                l1_index_column.index(*p as usize).unwrap()
                            {
                                right_buffer.push(right as usize);
                            }
                        }
                        _ => continue,
                    }
                }
            }
        }
        let left_table = self.left_table.read();
        let right_table = self.right_table.read();
        let mut indices = Vec::with_capacity(left_buffer.len());
        for res in left_buffer.iter() {
            indices.push((0usize, *res, 1usize));
        }
        let mut left_result_block =
            DataBlock::take_blocks(&vec![&*left_table], &indices, indices.len());
        indices.clear();
        for res in right_buffer.iter() {
            indices.push((0usize, *res, 1usize));
        }
        let right_result_block =
            DataBlock::take_blocks(&vec![&*right_table], &indices, indices.len());
        // Merge left_result_block and right_result_block
        for col in right_result_block.columns() {
            left_result_block.add_column(col.clone());
        }
        dbg!(&left_result_block);
        Ok(left_result_block)
    }
}
