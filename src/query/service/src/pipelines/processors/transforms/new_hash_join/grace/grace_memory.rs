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

use std::sync::PoisonError;

use crate::pipelines::processors::transforms::BasicHashJoinState;
use crate::pipelines::processors::transforms::HashJoinHashTable;
use crate::pipelines::processors::transforms::InnerHashJoin;
use crate::pipelines::processors::transforms::Join;
use crate::pipelines::processors::transforms::memory::AntiLeftHashJoin;
use crate::pipelines::processors::transforms::memory::AntiRightHashJoin;
use crate::pipelines::processors::transforms::memory::OuterRightHashJoin;
use crate::pipelines::processors::transforms::memory::SemiLeftHashJoin;
use crate::pipelines::processors::transforms::memory::SemiRightHashJoin;
use crate::pipelines::processors::transforms::memory::left_join::OuterLeftHashJoin;

pub trait GraceMemoryJoin: Join {
    fn reset_memory(&mut self);
}

fn reset_basic_state(state: &BasicHashJoinState) {
    let locked = state.mutex.lock();
    let _locked = locked.unwrap_or_else(PoisonError::into_inner);

    if !state.columns.is_empty() {
        state.columns.as_mut().clear();
    }

    if !state.chunks.is_empty() {
        state.chunks.as_mut().clear();
    }

    if *state.build_rows != 0 {
        *state.build_rows.as_mut() = 0;
    }

    if !state.column_types.is_empty() {
        state.column_types.as_mut().clear();
    }

    if !state.arenas.is_empty() {
        state.arenas.as_mut().clear();
    }

    if !state.build_queue.is_empty() {
        state.build_queue.as_mut().clear();
    }

    if !state.scan_map.is_empty() {
        state.scan_map.as_mut().clear();
    }

    if !state.scan_queue.is_empty() {
        state.scan_queue.as_mut().clear();
    }

    *state.hash_table.as_mut() = HashJoinHashTable::Null;
}

impl GraceMemoryJoin for InnerHashJoin {
    fn reset_memory(&mut self) {
        self.performance_context.clear();
        reset_basic_state(&self.basic_state);
    }
}

impl GraceMemoryJoin for OuterLeftHashJoin {
    fn reset_memory(&mut self) {
        self.performance_context.clear();
        reset_basic_state(&self.basic_state);
    }
}

impl GraceMemoryJoin for SemiLeftHashJoin {
    fn reset_memory(&mut self) {
        self.performance_context.clear();
        reset_basic_state(&self.basic_state);
    }
}

impl GraceMemoryJoin for AntiLeftHashJoin {
    fn reset_memory(&mut self) {
        self.performance_context.clear();
        reset_basic_state(&self.basic_state);
    }
}

impl GraceMemoryJoin for OuterRightHashJoin {
    fn reset_memory(&mut self) {
        self.performance_context.clear();
        reset_basic_state(&self.basic_state);
    }
}

impl GraceMemoryJoin for SemiRightHashJoin {
    fn reset_memory(&mut self) {
        self.performance_context.clear();
        reset_basic_state(&self.basic_state);
    }
}

impl GraceMemoryJoin for AntiRightHashJoin {
    fn reset_memory(&mut self) {
        self.performance_context.clear();
        reset_basic_state(&self.basic_state);
    }
}
