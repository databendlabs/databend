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
use std::sync::Arc;
use std::sync::Mutex;

use databend_common_expression::DataBlock;

use crate::pipelines::processors::transforms::HashJoinFactory;
use crate::pipelines::processors::transforms::new_hash_join::common::CStyleCell;
use crate::sessions::QueryContext;

pub struct HybridHashJoinState {
    pub mutex: Mutex<()>,
    pub ctx: Arc<QueryContext>,

    // Current recursion level (0 = initial)
    pub level: usize,
    // Maximum allowed spill level
    pub max_level: usize,

    // Factory for creating new join states
    pub factory: Arc<HashJoinFactory>,

    // Flag indicating whether spill has been triggered (for multi-thread sync)
    pub spilled: CStyleCell<bool>,

    // Queue of data blocks to be transitioned to grace mode
    // Multiple processors can pop from this queue to help with the transition
    pub transition_queue: CStyleCell<VecDeque<DataBlock>>,

    // Flag indicating whether transition has been initialized
    pub transition_initialized: CStyleCell<bool>,
}

impl HybridHashJoinState {
    pub fn create(
        ctx: Arc<QueryContext>,
        level: usize,
        max_level: usize,
        factory: Arc<HashJoinFactory>,
    ) -> Arc<HybridHashJoinState> {
        Arc::new(HybridHashJoinState {
            ctx,
            level,
            max_level,
            factory,
            mutex: Mutex::new(()),
            spilled: CStyleCell::new(false),
            transition_queue: CStyleCell::new(VecDeque::new()),
            transition_initialized: CStyleCell::new(false),
        })
    }

    pub fn can_spill(&self) -> bool {
        self.level < self.max_level
    }
}
