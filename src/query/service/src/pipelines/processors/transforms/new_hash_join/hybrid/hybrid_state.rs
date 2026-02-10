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

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use concurrent_queue::ConcurrentQueue;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_sql::plans::JoinType;

use crate::pipelines::processors::transforms::HashJoinFactory;
use crate::pipelines::processors::transforms::HybridHashJoin;
use crate::pipelines::processors::transforms::new_hash_join::grace::GraceHashJoinState;
use crate::sessions::QueryContext;

pub struct HybridHashJoinState {
    pub ctx: Arc<QueryContext>,

    // Current recursion level (0 = initial)
    pub level: usize,
    // Maximum allowed spill level
    pub max_level: usize,

    // Factory for creating new join states
    pub factory: Arc<HashJoinFactory>,

    // Flag indicating whether spill has been triggered (for multi-thread sync)
    pub spilled: AtomicBool,
    // Flag indicating whether spill has ever happened for this join instance.
    // Once set, it should never be cleared, so runtime filters stay disabled
    // for the lifetime of this join.
    pub ever_spilled: AtomicBool,

    pub transition_queue: ConcurrentQueue<DataBlock>,
}

impl HybridHashJoinState {
    pub fn create(
        ctx: Arc<QueryContext>,
        level: usize,
        factory: Arc<HashJoinFactory>,
    ) -> Result<Arc<HybridHashJoinState>> {
        let settings = ctx.get_settings();
        let max_level = settings.get_max_hash_join_spill_level()? as usize;

        Ok(Arc::new(HybridHashJoinState {
            ctx,
            level,
            max_level,
            factory,
            spilled: AtomicBool::new(false),
            ever_spilled: AtomicBool::new(false),
            transition_queue: ConcurrentQueue::unbounded(),
        }))
    }

    pub fn can_next_layer_join(&self) -> bool {
        self.level < self.max_level
    }

    pub fn check_spilled(&self) -> bool {
        self.spilled.load(Ordering::Acquire)
    }

    pub fn set_spilled(&self) -> bool {
        self.ever_spilled.store(true, Ordering::Release);
        !self.spilled.swap(true, Ordering::AcqRel)
    }

    pub fn has_spilled_once(&self) -> bool {
        self.ever_spilled.load(Ordering::Acquire)
    }

    pub fn create_grace_state(&self) -> Result<Arc<GraceHashJoinState>> {
        self.factory.create_grace_state(self.level + 1)
    }

    pub fn create_hybrid_join(&self, typ: JoinType) -> Result<HybridHashJoin> {
        self.factory.create_hybrid_join(typ, self.level + 1)
    }
}
