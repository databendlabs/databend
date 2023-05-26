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

use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_base::base::tokio::sync::Notify;
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
use common_pipeline_transforms::processors::transforms::sort_merge;
use common_sql::executor::IEJoin;
use common_sql::executor::IEJoinCondition;
use parking_lot::RwLock;

use crate::pipelines::processors::transforms::ie_join::ie_join_util::filter_block;
use crate::pipelines::processors::transforms::ie_join::ie_join_util::order_match;
use crate::pipelines::processors::transforms::ie_join::ie_join_util::probe_l1;
use crate::sessions::QueryContext;

struct IEConditionState {
    l1_data_type: DataType,
    // Sort description for L1
    l1_sort_descriptions: Vec<SortColumnDescription>,
    // Sort description for L2
    l2_sort_descriptions: Vec<SortColumnDescription>,
    // true is asc
    l1_order: bool,
}

pub struct IEJoinState {
    ctx: Arc<QueryContext>,
    ie_condition_state: IEConditionState,
    right_sorted_blocks: RwLock<Vec<DataBlock>>,
    // L1: sort by the first join key
    l1_sorted_blocks: RwLock<Vec<DataBlock>>,
    // data schema of sorted blocks
    data_schema: DataSchemaRef,
    // The origin data for left/right table
    left_table: RwLock<Vec<DataBlock>>,
    right_table: RwLock<Vec<DataBlock>>,
    // IEJoin related
    // Currently only support inner join
    // join_type: JoinType,
    conditions: Vec<IEJoinCondition>,
    other_conditions: Vec<RemoteExpr>,
    // Pipeline event related
    partition_finished: RwLock<bool>,
    finished_notify: Arc<Notify>,
    left_sinker_count: AtomicU64,
    right_sinker_count: AtomicU64,
    // Task that need to be executed, pair.0 is left table block, pair.1 is right table block
    tasks: RwLock<Vec<(usize, usize)>>,
    // Row index offset for left/right
    row_offset: RwLock<Vec<(usize, usize)>>,
    finished_tasks: AtomicU64,
}

impl IEJoinState {
    pub fn new(ctx: Arc<QueryContext>, ie_join: &IEJoin) -> Self {
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
            ctx,
            ie_condition_state: IEConditionState {
                l1_data_type,
                l1_sort_descriptions,
                l2_sort_descriptions,
                l1_order,
            },
            right_sorted_blocks: Default::default(),
            l1_sorted_blocks: Default::default(),
            data_schema: DataSchemaRefExt::create(fields),
            left_table: RwLock::new(Vec::new()),
            right_table: RwLock::new(Vec::new()),
            conditions: ie_join.conditions.clone(),
            other_conditions: ie_join.other_conditions.clone(),
            partition_finished: RwLock::new(false),
            finished_notify: Arc::new(Default::default()),
            left_sinker_count: AtomicU64::new(0),
            right_sinker_count: AtomicU64::new(0),
            tasks: RwLock::new(vec![]),
            row_offset: Default::default(),
            finished_tasks: AtomicU64::new(0),
        }
    }

    pub fn sink_right(&self, block: DataBlock) -> Result<()> {
        // Sink block to right table
        let mut right_table = self.right_table.write();
        right_table.push(block);
        Ok(())
    }

    pub fn sink_left(&self, block: DataBlock) -> Result<()> {
        // Sink block to left table
        let mut left_table = self.left_table.write();
        left_table.push(block);
        Ok(())
    }

    pub fn partition(&self) -> Result<()> {
        let left_table = self.left_table.read();
        let right_table = self.right_table.read();

        let mut l1_sorted_blocks = self.l1_sorted_blocks.write();
        let mut right_sorted_blocks = self.right_sorted_blocks.write();

        let mut current_rows = 0;
        for left_block in left_table.iter() {
            // Generate keys block by join keys
            // For example, if join keys are [t1.a + t2.b, t1.c], then key blocks will contain two columns: [t1.a + t2.b, t1.c]
            // We can get the key blocks by evaluating the join keys expressions on the block
            let mut columns = Vec::with_capacity(3);
            // Append join keys columns
            for condition in self.conditions.iter() {
                let func_ctx = FunctionContext::default();
                let evaluator = Evaluator::new(left_block, &func_ctx, &BUILTIN_FUNCTIONS);
                let expr = condition.left_expr.as_expr(&BUILTIN_FUNCTIONS);
                let column = evaluator
                    .run(&expr)?
                    .convert_to_full_column(expr.data_type(), left_block.num_rows());
                columns.push(column);
            }
            // Generate idx column from current_rows to current_rows + block.num_rows()
            let mut column_builder = ColumnBuilder::with_capacity(
                &DataType::Number(NumberDataType::Int64),
                left_block.num_rows(),
            );
            for idx in current_rows..(current_rows + left_block.num_rows()) {
                column_builder.push(ScalarRef::Number(NumberScalar::Int64((idx + 1) as i64)));
            }
            columns.push(column_builder.build());
            let keys_block = DataBlock::new_from_columns(columns);
            l1_sorted_blocks.push(keys_block);
            current_rows += left_block.num_rows();
        }

        current_rows = 0;
        for right_block in right_table.iter() {
            // Generate keys block by join keys
            // For example, if join keys are [t1.a + t2.b, t1.c], then key blocks will contain two columns: [t1.a + t2.b, t1.c]
            // We can get the key blocks by evaluating the join keys expressions on the block
            let mut columns = Vec::with_capacity(3);
            // Append join keys columns
            for condition in self.conditions.iter() {
                let func_ctx = FunctionContext::default();
                let evaluator = Evaluator::new(right_block, &func_ctx, &BUILTIN_FUNCTIONS);
                let expr = condition.right_expr.as_expr(&BUILTIN_FUNCTIONS);
                let column = evaluator
                    .run(&expr)?
                    .convert_to_full_column(expr.data_type(), right_block.num_rows());
                columns.push(column);
            }
            // Generate idx column from current_rows to current_rows + block.num_rows()
            let mut column_builder = ColumnBuilder::with_capacity(
                &DataType::Number(NumberDataType::Int64),
                right_block.num_rows(),
            );
            for idx in current_rows..(current_rows + right_block.num_rows()) {
                column_builder.push(ScalarRef::Number(NumberScalar::Int64(-(idx as i64 + 1))));
            }
            columns.push(column_builder.build());
            let keys_block = DataBlock::new_from_columns(columns);
            right_sorted_blocks.push(keys_block);
            current_rows += right_block.num_rows();
        }
        // Add tasks
        let mut row_offset = self.row_offset.write();
        let mut left_offset = 0;
        let mut right_offset = 0;
        let mut tasks = self.tasks.write();
        for (left_idx, left_block) in l1_sorted_blocks.iter().enumerate() {
            for (right_idx, right_block) in right_sorted_blocks.iter().enumerate() {
                row_offset.push((left_offset, right_offset));
                tasks.push((left_idx, right_idx));
                right_offset += right_block.num_rows();
            }
            right_offset = 0;
            left_offset += left_block.num_rows();
        }
        Ok(())
    }

    pub fn ie_join(&self, task_id: usize) -> Result<DataBlock> {
        let block_size = self.ctx.get_settings().get_max_block_size()? as usize;
        let tasks = self.tasks.read();
        let (left_idx, right_idx) = tasks[task_id];
        let l1_sorted_blocks = self.l1_sorted_blocks.read();
        let right_sorted_blocks = self.right_sorted_blocks.read();
        let l1_sorted_block = DataBlock::sort(
            &l1_sorted_blocks[left_idx],
            &self.ie_condition_state.l1_sort_descriptions,
            None,
        )?;
        let right_block = DataBlock::sort(
            &right_sorted_blocks[right_idx],
            &self.ie_condition_state.l1_sort_descriptions,
            None,
        )?;
        if !self.intersection(&l1_sorted_block, &right_block) {
            return Ok(DataBlock::empty());
        }
        let mut l1_sorted_blocks = vec![l1_sorted_block, right_block];

        let data_schema = DataSchemaRefExt::create(
            self.data_schema.fields().as_slice()[0..self.conditions.len() + 1].to_vec(),
        );

        l1_sorted_blocks = sort_merge(
            data_schema,
            block_size,
            self.ie_condition_state.l1_sort_descriptions.clone(),
            &l1_sorted_blocks,
        )?;

        // Add a column at the end of `l1_sorted_blocks`, named `_pos`, which is used to record the position of the block in the original table
        let mut count: usize = 0;
        for block in l1_sorted_blocks.iter_mut() {
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
        // Merge `l1_sorted_blocks` to one block
        let mut merged_blocks = DataBlock::concat(&l1_sorted_blocks)?;
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

        let mut l2_sorted_blocks = Vec::with_capacity(l1_sorted_blocks.len());
        for block in l1_sorted_blocks.iter() {
            l2_sorted_blocks.push(DataBlock::sort(
                block,
                &self.ie_condition_state.l2_sort_descriptions,
                None,
            )?);
        }
        merged_blocks = DataBlock::concat(&sort_merge(
            self.data_schema.clone(),
            block_size,
            self.ie_condition_state.l2_sort_descriptions.clone(),
            &l2_sorted_blocks,
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
        let mut bit_array = MutableBitmap::with_capacity(p_array.len());
        bit_array.extend_constant(p_array.len(), false);

        let l2 = &merged_blocks.columns()[1].value.convert_to_full_column(
            self.conditions[0]
                .right_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .data_type(),
            merged_blocks.num_rows(),
        );

        drop(l2_sorted_blocks);
        drop(l1_sorted_blocks);

        self.finalize(l1, l2, l1_index_column, &p_array, bit_array, task_id)
    }

    pub fn finalize(
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
            indices.push((0usize, *res, 1usize));
        }
        let mut left_result_block =
            DataBlock::take_blocks(&[&left_table[left_idx]], &indices, indices.len());
        indices.clear();
        for res in right_buffer.iter() {
            indices.push((0usize, *res, 1usize));
        }
        let right_result_block =
            DataBlock::take_blocks(&[&right_table[right_idx]], &indices, indices.len());
        // Merge left_result_block and right_result_block
        for col in right_result_block.columns() {
            left_result_block.add_column(col.clone());
        }
        for filter in self.other_conditions.iter() {
            left_result_block = filter_block(left_result_block, filter)?;
        }
        Ok(left_result_block)
    }

    fn intersection(&self, left_block: &DataBlock, right_block: &DataBlock) -> bool {
        let left_len = left_block.num_rows();
        let right_len = right_block.num_rows();
        let left_l1_column = left_block.columns()[0]
            .value
            .convert_to_full_column(&self.ie_condition_state.l1_data_type, left_len);
        let right_l1_column = right_block.columns()[0]
            .value
            .convert_to_full_column(&self.ie_condition_state.l1_data_type, right_len);
        // If `left_l1_column` and `right_l1_column` have intersection && `left_l2_column` and `right_l2_column` have intersection, return true
        let (left_l1_min, left_l1_max, right_l1_min, right_l1_max) =
            match self.ie_condition_state.l1_order {
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
        match self.ie_condition_state.l1_order {
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

impl IEJoinState {
    pub fn left_attach(&self) {
        self.left_sinker_count
            .fetch_add(1, atomic::Ordering::SeqCst);
    }

    pub fn left_detach(&self) -> Result<()> {
        self.left_sinker_count
            .fetch_sub(1, atomic::Ordering::SeqCst);
        if self.left_sinker_count.load(atomic::Ordering::Relaxed) == 0 {
            loop {
                if self.right_sinker_count.load(atomic::Ordering::Relaxed) == 0 {
                    // Left and right both finish sink
                    // Partition left/right table
                    self.partition()?;
                    // Set partition finished
                    let mut partition_finished = self.partition_finished.write();
                    *partition_finished = true;
                    self.finished_notify.notify_waiters();
                    break;
                }
            }
        }
        Ok(())
    }

    pub fn right_attach(&self) {
        self.right_sinker_count
            .fetch_add(1, atomic::Ordering::SeqCst);
    }

    pub fn task_id(&self) -> Option<usize> {
        let task_id = self.finished_tasks.fetch_add(1, atomic::Ordering::SeqCst);
        if task_id >= self.tasks.read().len() as u64 {
            return None;
        }
        Some(task_id as usize)
    }

    pub fn right_detach(&self) {
        self.right_sinker_count
            .fetch_sub(1, atomic::Ordering::SeqCst);
    }

    pub(crate) async fn wait_merge_finish(&self) -> Result<()> {
        if !*self.partition_finished.read() {
            self.finished_notify.notified().await;
        }
        Ok(())
    }
}
