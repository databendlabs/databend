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

use common_base::base::tokio::sync::Notify;
use common_exception::Result;
use common_expression::{BlockEntry, Column, ColumnBuilder, DataBlock, DataSchemaRefExt, Evaluator, FunctionContext, ScalarRef, Value, ValueRef};
use common_expression::RemoteExpr;
use common_sql::executor::RangeJoin;
use common_sql::executor::RangeJoinCondition;
use common_sql::executor::RangeJoinType;
use common_sql::plans::JoinType;
use parking_lot::RwLock;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_catalog::table_context::TableContext;
use common_expression::types::{DataType, NumberColumnBuilder, NumberDataType, NumberScalar, UInt64Type, ValueType};
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_transforms::processors::transforms::sort_merge;

use crate::pipelines::processors::transforms::range_join::ie_join_state::IEJoinState;
use crate::pipelines::processors::transforms::range_join::merge_join_state::MergeJoinState;
use crate::sessions::QueryContext;

pub struct RangeJoinState {
    pub(crate) ctx: Arc<QueryContext>,
    // The origin data for left/right table
    pub(crate) left_table: RwLock<Vec<DataBlock>>,
    pub(crate) right_table: RwLock<Vec<DataBlock>>,
    pub(crate) conditions: Vec<RangeJoinCondition>,
    pub(crate) join_type: JoinType,
    pub(crate) other_conditions: Vec<RemoteExpr>,
    // Pipeline event related
    pub(crate) partition_finished: RwLock<bool>,
    pub(crate) finished_notify: Arc<Notify>,
    pub(crate) left_sinker_count: RwLock<usize>,
    pub(crate) right_sinker_count: RwLock<usize>,
    // Task that need to be executed, pair.0 is left table block, pair.1 is right table block
    pub(crate) tasks: RwLock<Vec<(usize, usize)>>,
    // Row index offset for left/right
    pub(crate) row_offset: RwLock<Vec<(usize, usize)>>,
    pub(crate) finished_tasks: AtomicU64,
    // IEJoin state
    pub(crate) ie_join_state: Option<IEJoinState>,
    // MergeJoin state
    pub(crate) merge_join_state: Option<MergeJoinState>,
}

impl RangeJoinState {
    pub fn new(ctx: Arc<QueryContext>, range_join: &RangeJoin) -> Self {
        match range_join.range_join_type {
            RangeJoinType::IEJoin => {
                let ie_join_state = IEJoinState::new(range_join);
                Self {
                    ctx,
                    left_table: RwLock::new(vec![]),
                    right_table: RwLock::new(vec![]),
                    conditions: range_join.conditions.clone(),
                    join_type: range_join.join_type.clone(),
                    other_conditions: range_join.other_conditions.clone(),
                    partition_finished: RwLock::new(false),
                    finished_notify: Arc::new(Notify::new()),
                    left_sinker_count: RwLock::new(0),
                    right_sinker_count: RwLock::new(0),
                    tasks: RwLock::new(vec![]),
                    row_offset: RwLock::new(vec![]),
                    finished_tasks: AtomicU64::new(0),
                    ie_join_state: Some(ie_join_state),
                    merge_join_state: None,
                }
            }
            RangeJoinType::Merge => todo!(),
        }
    }

    pub(crate) fn sink_right(&self, block: DataBlock) -> Result<()> {
        // Sink block to right table
        let mut right_table = self.right_table.write();
        right_table.push(block);
        Ok(())
    }

    pub(crate) fn sink_left(&self, block: DataBlock) -> Result<()> {
        // Sink block to left table
        let mut left_table = self.left_table.write();
        left_table.push(block);
        Ok(())
    }

    pub(crate) fn left_attach(&self) {
        let mut left_sinker_count = self.left_sinker_count.write();
        *left_sinker_count += 1;
    }

    pub(crate) fn left_detach(&self) -> Result<()> {
        let right_sinker_count = self.right_sinker_count.read();
        let mut left_sinker_count = self.left_sinker_count.write();
        *left_sinker_count -= 1;
        if *left_sinker_count == 0 && *right_sinker_count == 0 {
            // Left and right both finish sink
            // Partition left/right table
            if let Some(ie_join_state) = &self.ie_join_state {
                self.ie_join_partition()?;
            } else {
                todo!()
            }
            // Set partition finished
            let mut partition_finished = self.partition_finished.write();
            *partition_finished = true;
            self.finished_notify.notify_waiters();
        }
        Ok(())
    }

    pub(crate) fn right_attach(&self) {
        let mut right_sinker_count = self.right_sinker_count.write();
        *right_sinker_count += 1;
    }

    pub fn task_id(&self) -> Option<usize> {
        let task_id = self.finished_tasks.fetch_add(1, atomic::Ordering::SeqCst);
        if task_id >= self.tasks.read().len() as u64 {
            return None;
        }
        Some(task_id as usize)
    }

    pub(crate) fn right_detach(&self) -> Result<()> {
        let mut right_sinker_count = self.right_sinker_count.write();
        *right_sinker_count -= 1;
        let left_sinker_count = self.left_sinker_count.read();
        if *right_sinker_count == 0 && *left_sinker_count == 0 {
            // Left and right both finish sink
            // Partition left/right table
            if let Some(ie_join_state) = &self.ie_join_state {
                self.ie_join_partition()?;
            } else {
                todo!()
            }
            // Set partition finished
            let mut partition_finished = self.partition_finished.write();
            *partition_finished = true;
            self.finished_notify.notify_waiters();
        }
        Ok(())
    }

    pub(crate) async fn wait_merge_finish(&self) -> Result<()> {
        if !*self.partition_finished.read() {
            self.finished_notify.notified().await;
        }
        Ok(())
    }



}

