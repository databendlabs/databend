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
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use concurrent_queue::ConcurrentQueue;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;

use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::HashJoinFactory;
use crate::pipelines::processors::transforms::new_hash_join::grace::GraceHashJoinState;
use crate::sessions::QueryContext;
use crate::sessions::TableContextSettings;

/// States shared by all processors of one hybrid hash join level.
#[derive(Clone)]
pub struct HybridLevelStates {
    pub basic: Arc<BasicHashJoinState>,
    pub hybrid: Arc<HybridHashJoinState>,
}

/// States shared by all processors once a level switches to grace mode: the grace state
/// that partitions this level's data and the states of the level that restores it.
#[derive(Clone)]
pub struct NextLevelStates {
    pub grace: Arc<GraceHashJoinState>,
    pub level: HybridLevelStates,
}

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
    // Count logical build input before spill repartitioning so the runtime-filter packet can
    // globalize build-side emptiness without depending on in-memory state that spill resets.
    build_input_rows: AtomicUsize,

    pub transition_queue: ConcurrentQueue<DataBlock>,

    // Owned here so that every restore partition of the parent grace join gets fresh
    // next-level states: they are dropped together with this level's grace mode.
    next_level: Mutex<Option<NextLevelStates>>,
}

impl HybridHashJoinState {
    pub fn create(
        ctx: Arc<QueryContext>,
        level: usize,
        factory: Arc<HashJoinFactory>,
    ) -> Result<Arc<HybridHashJoinState>> {
        // On SSE4.2, fast_hash (_mm_crc32_u64) only sets the low 32 bits.
        #[cfg(target_feature = "sse4.2")]
        const HASH_JOIN_SPILL_MAX_LEVEL: usize = 7;
        #[cfg(not(target_feature = "sse4.2"))]
        const HASH_JOIN_SPILL_MAX_LEVEL: usize = 15;

        let settings = ctx.get_settings();
        let max_spill_level = settings.get_max_hash_join_spill_level()? as usize;
        let max_level = (max_spill_level).min(HASH_JOIN_SPILL_MAX_LEVEL);

        Ok(Arc::new(HybridHashJoinState {
            ctx,
            level,
            max_level,
            factory,
            spilled: AtomicBool::new(false),
            ever_spilled: AtomicBool::new(false),
            build_input_rows: AtomicUsize::new(0),
            transition_queue: ConcurrentQueue::unbounded(),
            next_level: Mutex::new(None),
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

    pub fn add_build_input_rows(&self, rows: usize) {
        self.build_input_rows.fetch_add(rows, Ordering::AcqRel);
    }

    pub fn build_input_rows(&self) -> usize {
        self.build_input_rows.load(Ordering::Acquire)
    }

    /// Returns the next-level states, creating them on the first call after a reset.
    pub fn next_level_states(&self) -> Result<NextLevelStates> {
        let mut next_level = self
            .next_level
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        if let Some(states) = next_level.as_ref() {
            return Ok(states.clone());
        }

        let states = NextLevelStates {
            grace: GraceHashJoinState::create(self.ctx.clone()),
            level: self.factory.create_level_states(self.level + 1)?,
        };
        *next_level = Some(states.clone());
        Ok(states)
    }

    pub fn reset_next_level_states(&self) {
        let mut next_level = self
            .next_level
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *next_level = None;
    }
}
