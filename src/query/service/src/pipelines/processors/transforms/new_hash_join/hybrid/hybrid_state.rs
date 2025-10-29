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

use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;

use crate::pipelines::processors::transforms::new_hash_join::common::CStyleCell;
use crate::pipelines::processors::transforms::HashJoinFactory;
use crate::pipelines::processors::HashJoinDesc;
use crate::sessions::QueryContext;

#[allow(dead_code)]
pub struct HybridHashJoinState {
    pub mutex: Mutex<()>,

    pub spills_queue: CStyleCell<VecDeque<DataBlock>>,

    pub has_spilled: AtomicBool,
    pub level: usize,
    pub factory: Arc<HashJoinFactory>,

    pub ctx: Arc<QueryContext>,
    pub function_ctx: FunctionContext,
    pub hash_method_kind: HashMethodKind,
    pub desc: Arc<HashJoinDesc>,
}

impl HybridHashJoinState {
    pub fn create(
        ctx: Arc<QueryContext>,
        function_context: FunctionContext,
        hash_method_kind: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        level: usize,
        factory: Arc<HashJoinFactory>,
    ) -> Arc<HybridHashJoinState> {
        Arc::new(HybridHashJoinState {
            ctx,
            desc,
            level,
            factory,
            hash_method_kind,
            mutex: Mutex::new(()),
            spills_queue: CStyleCell::new(VecDeque::new()),
            has_spilled: AtomicBool::new(false),
            function_ctx: function_context,
        })
    }

    pub fn steal_transform_task(&self) -> Option<(bool, DataBlock)> {
        let locked = self.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);

        self.spills_queue
            .as_mut()
            .pop_front()
            .map(|v| (self.spills_queue.is_empty(), v))
    }
}

impl Drop for HybridHashJoinState {
    fn drop(&mut self) {
        self.factory.remove_hybrid_state(self.level)
    }
}
