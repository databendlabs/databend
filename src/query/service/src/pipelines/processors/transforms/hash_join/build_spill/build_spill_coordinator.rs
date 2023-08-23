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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_exception::Result;

use crate::pipelines::processors::transforms::hash_join::HashJoinBuildState;

/// Coordinate all hash join build processors to spill.
/// It's shared by all hash join build processors.
/// When hash join build needs to spill, all processor will stop executing and prepare to spill.
/// The last one will be as the coordinator to spill all processors and then wake up all processors to continue executing.
pub struct BuildSpillCoordinator {
    /// Need to spill, if one of the builders need to spill, this flag will be set to true.
    need_spill: AtomicBool,
}

impl BuildSpillCoordinator {
    // Start to spill.
    fn spill(&self) -> Result<()> {
        todo!()
    }

    // Called by hash join build processor, if current processor need to spill, then set `need_spill` to true.
    pub fn need_spill(&self) -> Result<()> {
        self.need_spill.store(true, Ordering::SeqCst);
        todo!()
    }

    // If current waiting spilling builder is the last one, then spill all builders.
    fn wait_spill(&mut self) -> Result<()> {
        todo!()
    }
}
