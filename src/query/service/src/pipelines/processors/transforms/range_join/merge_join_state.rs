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

use parking_lot::RwLock;
use common_catalog::table_context::TableContext;
use common_sql::executor::RangeJoin;
use crate::pipelines::processors::transforms::RangeJoinState;
use common_exception::Result;
use common_expression::{ColumnBuilder, DataBlock, Evaluator, FunctionContext, ScalarRef, SortColumnDescription};
use common_expression::types::{DataType, NumberDataType, NumberScalar};
use common_functions::BUILTIN_FUNCTIONS;

pub struct MergeJoinState {
}


impl MergeJoinState {
    pub(crate) fn new(merge_join: &RangeJoin) -> Self {
        MergeJoinState { }
    }
}


impl RangeJoinState {
    pub fn merge_join(&self, task_id: usize) -> Result<DataBlock> {
        let tasks = self.tasks.read();
        let (left_idx, right_idx) = tasks[task_id];
        let left_sorted_blocks = self.left_sorted_blocks.read();
        let right_sorted_blocks = self.right_sorted_blocks.read();

        let left_sort_descriptions = self.sort_descriptions(true);
        let left_sorted_block = DataBlock::sort(&left_sorted_blocks[left_idx], &left_sort_descriptions, None)?;

        let right_sort_descriptions = self.sort_descriptions(false);
        let right_sort_block = DataBlock::sort(&right_sorted_blocks[right_idx], &right_sort_descriptions, None)?;

        // Start to execute merge join algo
        todo!()
    }

    // Used by merge join
    fn sort_descriptions(&self, left: bool) -> Vec<SortColumnDescription> {
        let op = &self.conditions[0].operator;
        let asc = match op.as_str() {
            "gt" | "gte" => true,
            "lt" | "lte" => false,
            _ => unreachable!(),
        };
        let nullable = if left {
            self.conditions[0].left_expr.as_expr(&BUILTIN_FUNCTIONS).data_type().is_nullable()
        } else {
            self.conditions[0].right_expr.as_expr(&BUILTIN_FUNCTIONS).data_type().is_nullable()
        };
        vec![SortColumnDescription {
            offset: 0,
            asc,
            nulls_first: false,
            is_nullable,
        }]
    }
}