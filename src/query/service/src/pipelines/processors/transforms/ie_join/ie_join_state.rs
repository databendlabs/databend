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
use common_expression::RemoteExpr;
use common_expression::SortColumnDescription;
use common_functions::BUILTIN_FUNCTIONS;
use common_sql::executor::IEJoin;
use common_sql::executor::IEJoinCondition;
use common_sql::plans::JoinType;
use parking_lot::RwLock;

pub struct IEJoinState {
    sorted_blocks: RwLock<Vec<DataBlock>>,
    left_table: RwLock<DataBlock>,
    right_table: RwLock<DataBlock>,
    join_type: JoinType,
    conditions: Vec<IEJoinCondition>,
    other_conditions: Vec<RemoteExpr>,
    right_finished: RwLock<bool>,
}

impl IEJoinState {
    pub fn new(ie_join: &IEJoin) -> Self {
        IEJoinState {
            sorted_blocks: Default::default(),
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
        {
            let mut right_table = self.right_table.write();
            *right_table = DataBlock::concat(&vec![right_table.clone(), block.clone()])?;
        }
        // Second, generate keys block by join keys
        // For example, if join keys are [t1.a + t2.b, t1.c], then key blocks will contain two columns: [t1.a + t2.b, t1.c]
        // We can get the key blocks by evaluating the join keys expressions on the block
        let mut columns = vec![];
        let mut is_nullable = false;
        for (idx, condition) in self.conditions.iter().enumerate() {
            let func_ctx = FunctionContext::default();
            let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
            let expr = condition.right_expr.as_expr(&BUILTIN_FUNCTIONS);
            if idx == 0 {
                is_nullable = expr.data_type().is_nullable();
            }
            let column = evaluator
                .run(&expr)?
                .convert_to_full_column(expr.data_type(), block.num_rows());
            columns.push(column);
        }
        // Sort columns by the first column
        let keys_block = DataBlock::new_from_columns(columns);
        let asc = if matches!(self.conditions[0].operator.as_str(), "gt" | "gte") {
            true
        } else {
            false
        };
        let sort_desc = vec![SortColumnDescription {
            offset: 0,
            asc,
            nulls_first: false,
            is_nullable,
        }];
        let sorted_keys_block = DataBlock::sort(&keys_block, &sort_desc, None)?;
        {
            let mut sorted_blocks = self.sorted_blocks.write();
            sorted_blocks.push(sorted_keys_block);
        }
        Ok(())
    }

    pub fn sink_left(&self, block: DataBlock) -> Result<()> {
        // First, sink block to left table
        {
            let mut left_table = self.left_table.write();
            *left_table = DataBlock::concat(&vec![left_table.clone(), block.clone()])?;
        }
        // Second, generate keys block by join keys
        // For example, if join keys are [t1.a + t2.b, t1.c], then key blocks will contain two columns: [t1.a + t2.b, t1.c]
        // We can get the key blocks by evaluating the join keys expressions on the block
        let mut columns = vec![];
        let mut is_nullable = false;
        for (idx, condition) in self.conditions.iter().enumerate() {
            let func_ctx = FunctionContext::default();
            let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
            let expr = condition.left_expr.as_expr(&BUILTIN_FUNCTIONS);
            if idx == 0 {
                is_nullable = expr.data_type().is_nullable();
            }
            let column = evaluator
                .run(&expr)?
                .convert_to_full_column(expr.data_type(), block.num_rows());
            columns.push(column);
        }
        // Sort columns by the first column
        let keys_block = DataBlock::new_from_columns(columns);
        let asc = if matches!(self.conditions[0].operator.as_str(), "gt" | "gte") {
            true
        } else {
            false
        };
        let sort_desc = vec![SortColumnDescription {
            offset: 0,
            asc,
            nulls_first: false,
            is_nullable,
        }];
        let sorted_keys_block = DataBlock::sort(&keys_block, &sort_desc, None)?;
        {
            let mut sorted_blocks = self.sorted_blocks.write();
            sorted_blocks.push(sorted_keys_block);
        }
        Ok(())
    }
}
