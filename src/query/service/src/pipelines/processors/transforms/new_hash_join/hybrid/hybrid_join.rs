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
use std::sync::PoisonError;

use databend_common_base::base::ProgressValues;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethodKind;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_sql::plans::JoinType;

use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::GraceHashJoin;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::new_hash_join::grace::GraceMemoryJoin;
use crate::pipelines::processors::transforms::new_hash_join::hybrid::hybrid_state::HybridHashJoinState;
use crate::pipelines::processors::transforms::new_hash_join::join::Join;
use crate::pipelines::processors::transforms::new_hash_join::join::JoinStream;
use crate::sessions::QueryContext;

/// Hybrid hash join mode:
/// - Memory: In-memory hash join (the default starting mode)
/// - Grace: Grace hash join with spilling to disk
enum HybridJoinMode {
    Memory(Box<dyn GraceMemoryJoin>),
    Grace(Box<GraceHashJoin<HybridHashJoin>>),
}

/// HybridHashJoin combines memory hash join and grace hash join.
///
/// It starts in Memory mode and automatically transitions to Grace mode
/// when memory pressure is detected (and the current level is below max_level).
///
/// The recursive spill mechanism works as follows:
/// 1. HybridHashJoin at level 0 starts in Memory mode
/// 2. When memory pressure triggers, it transitions to Grace mode with a nested HybridHashJoin at level 1
/// 3. The nested HybridHashJoin at level 1 can also transition to Grace mode if needed
/// 4. This continues until level reaches max_level, at which point no more spilling occurs
pub struct HybridHashJoin {
    mode: HybridJoinMode,
    memory_settings: MemorySettings,
    state: Arc<HybridHashJoinState>,
    basic_state: Arc<BasicHashJoinState>,

    // Parameters needed for creating GraceHashJoin during transition
    ctx: Arc<QueryContext>,
    function_ctx: FunctionContext,
    hash_method_kind: HashMethodKind,
    desc: Arc<HashJoinDesc>,
    join_type: JoinType,
}

unsafe impl Send for HybridHashJoin {}
unsafe impl Sync for HybridHashJoin {}

impl HybridHashJoin {
    pub fn create(
        ctx: Arc<QueryContext>,
        function_ctx: FunctionContext,
        hash_method_kind: HashMethodKind,
        desc: Arc<HashJoinDesc>,
        memory_settings: MemorySettings,
        state: Arc<HybridHashJoinState>,
        basic_state: Arc<BasicHashJoinState>,
        memory_join: Box<dyn GraceMemoryJoin>,
        join_type: JoinType,
    ) -> HybridHashJoin {
        HybridHashJoin {
            ctx,
            desc,
            state,
            join_type,
            function_ctx,
            basic_state,
            memory_settings,
            hash_method_kind,
            mode: HybridJoinMode::Memory(memory_join),
        }
    }

    /// Trigger spill: mark as spilled and initialize the transition queue
    fn trigger_spill(&mut self) -> Result<()> {
        let locked = self.state.mutex.lock();
        let _locked = locked.unwrap_or_else(PoisonError::into_inner);

        // Double-check: another thread might have already triggered spill
        if !*self.state.spilled {
            // I am the first processor to trigger spill

            // 1. Move BasicHashJoinState.chunks to transition_queue
            //    so other processors can help process the data
            let chunks = std::mem::take(self.basic_state.chunks.as_mut());
            *self.state.transition_queue.as_mut() = chunks.into();
            *self.state.transition_initialized.as_mut() = true;

            // 2. Clear other BasicHashJoinState fields
            *self.basic_state.build_rows.as_mut() = 0;
            self.basic_state.build_queue.as_mut().clear();
            self.basic_state.scan_map.as_mut().clear();
            self.basic_state.scan_queue.as_mut().clear();

            // 3. Set spilled flag
            *self.state.spilled.as_mut() = true;
        }

        drop(_locked);

        // Switch to grace mode (will process data from transition_queue)
        self.switch_to_grace_mode()
    }

    /// Switch from Memory mode to Grace mode
    fn switch_to_grace_mode(&mut self) -> Result<()> {
        // 1. Get shared grace_state (all processors share the same state)
        let grace_state = self
            .state
            .factory
            .create_grace_state(self.state.level + 1)?;

        // 2. Create new HybridHashJoin as the memory_hash_join for GraceHashJoin
        let sub_hybrid = self.state.factory.create_hybrid_join(
            self.join_type,
            self.state.level + 1,
            self.state.max_level,
        )?;

        // 3. Create GraceHashJoin (each processor creates its own instance, but shares grace_state)
        let shift_bits = self.state.level * 4;
        let mut grace_join = GraceHashJoin::create(
            self.ctx.clone(),
            self.function_ctx.clone(),
            self.hash_method_kind.clone(),
            self.desc.clone(),
            grace_state,
            sub_hybrid,
            shift_bits,
        )?;

        // 4. Pop data from transition_queue and spill (multiple processors compete)
        loop {
            let block = {
                let locked = self.state.mutex.lock();
                let _locked = locked.unwrap_or_else(PoisonError::into_inner);
                self.state.transition_queue.as_mut().pop_front()
            };

            let Some(block) = block else {
                break;
            };
            grace_join.add_block(Some(block))?;
        }

        // 5. Switch mode
        self.mode = HybridJoinMode::Grace(Box::new(grace_join));
        Ok(())
    }
}

impl Join for HybridHashJoin {
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        // 1. Check if another processor has already triggered spill
        if *self.state.spilled && matches!(self.mode, HybridJoinMode::Memory(_)) {
            self.switch_to_grace_mode()?;
        }

        // 2. Process data based on current mode
        match &mut self.mode {
            HybridJoinMode::Memory(memory_join) => {
                memory_join.add_block(data)?;

                // 3. Check if we need to spill
                if self.memory_settings.check_spill() && self.state.can_spill() {
                    self.trigger_spill()?;
                }
                Ok(())
            }
            HybridJoinMode::Grace(grace_join) => grace_join.add_block(data),
        }
    }

    fn final_build(&mut self) -> Result<Option<ProgressValues>> {
        // ===== Critical sync point =====
        // Some processors might not have detected spilled during add_block.
        // final_build is a sync point (outer barrier), need to check again.
        if *self.state.spilled && matches!(self.mode, HybridJoinMode::Memory(_)) {
            self.switch_to_grace_mode()?;
        }

        match &mut self.mode {
            HybridJoinMode::Memory(join) => join.final_build(),
            HybridJoinMode::Grace(join) => join.final_build(),
        }
    }

    fn add_runtime_filter_packet(&self, packet: JoinRuntimeFilterPacket) {
        match &self.mode {
            HybridJoinMode::Memory(join) => join.add_runtime_filter_packet(packet),
            HybridJoinMode::Grace(join) => join.add_runtime_filter_packet(packet),
        }
    }

    fn build_runtime_filter(&self) -> Result<JoinRuntimeFilterPacket> {
        match &self.mode {
            HybridJoinMode::Memory(join) => join.build_runtime_filter(),
            HybridJoinMode::Grace(join) => join.build_runtime_filter(),
        }
    }

    fn probe_block(&mut self, data: DataBlock) -> Result<Box<dyn JoinStream + '_>> {
        match &mut self.mode {
            HybridJoinMode::Memory(join) => join.probe_block(data),
            HybridJoinMode::Grace(join) => join.probe_block(data),
        }
    }

    fn final_probe(&mut self) -> Result<Option<Box<dyn JoinStream + '_>>> {
        match &mut self.mode {
            HybridJoinMode::Memory(join) => join.final_probe(),
            HybridJoinMode::Grace(join) => join.final_probe(),
        }
    }
}

impl GraceMemoryJoin for HybridHashJoin {
    fn reset_memory(&mut self) {
        // 1. Reset spilled and transition state
        {
            let locked = self.state.mutex.lock();
            let _locked = locked.unwrap_or_else(PoisonError::into_inner);
            *self.state.spilled.as_mut() = false;
            *self.state.transition_initialized.as_mut() = false;
            self.state.transition_queue.as_mut().clear();
        }

        // 2. Reset based on current mode
        match &mut self.mode {
            HybridJoinMode::Memory(join) => {
                // Memory mode: reset the inner join
                join.reset_memory();
            }
            HybridJoinMode::Grace(_) => {
                // Grace mode: need to reset back to Memory mode
                // Get a fresh basic_state and memory_join from factory
                let new_basic_state = self
                    .state
                    .factory
                    .create_basic_state(self.state.level)
                    .expect("Failed to create basic state");

                // Ensure basic_state is clean
                {
                    let locked = new_basic_state.mutex.lock();
                    let _locked = locked.unwrap_or_else(PoisonError::into_inner);
                    new_basic_state.chunks.as_mut().clear();
                    *new_basic_state.build_rows.as_mut() = 0;
                    new_basic_state.build_queue.as_mut().clear();
                    new_basic_state.scan_map.as_mut().clear();
                    new_basic_state.scan_queue.as_mut().clear();
                }

                // Create new memory join
                let new_memory_join = self
                    .state
                    .factory
                    .create_memory_join(self.join_type, self.state.level)
                    .expect("Failed to create memory join");

                self.basic_state = new_basic_state;
                self.mode = HybridJoinMode::Memory(new_memory_join);
            }
        }
    }
}
