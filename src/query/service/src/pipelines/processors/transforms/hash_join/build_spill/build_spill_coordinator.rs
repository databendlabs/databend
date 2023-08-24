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

use common_base::base::tokio::sync::Notify;
use common_exception::Result;
use parking_lot::RwLock;

use crate::pipelines::processors::transforms::hash_join::HashJoinBuildState;

/// Coordinate all hash join build processors to spill.
/// It's shared by all hash join build processors.
/// When hash join build needs to spill, all processor will stop executing and prepare to spill.
/// The last one will be as the coordinator to spill all processors and then wake up all processors to continue executing.
pub struct BuildSpillCoordinator {
    /// Need to spill, if one of the builders need to spill, this flag will be set to true.
    need_spill: AtomicBool,
    /// Current waiting spilling processor count.
    pub(crate) waiting_spill_count: RwLock<usize>,
    /// Total processor count.
    pub(crate) total_builder_count: RwLock<usize>,
    /// Notify all waiting spilling processors to start spill.
    pub(crate) notify_spill: Arc<Notify>,
}

impl BuildSpillCoordinator {
    pub fn create() -> Arc<Self> {
        Arc::new(Self {
            need_spill: AtomicBool::new(false),
            waiting_spill_count: RwLock::new(0),
            total_builder_count: RwLock::new(0),
            notify_spill: Arc::new(Default::default()),
        })
    }

    // Start to spill.
    fn spill(&self) -> Result<()> {
        self.notify_spill.notify_waiters();
        todo!()
    }

    // Called by hash join build processor, if current processor need to spill, then set `need_spill` to true.
    pub fn need_spill(&self) -> Result<()> {
        self.need_spill.store(true, Ordering::SeqCst);
        self.wait_spill()?;
        Ok(())
    }

    // If current waiting spilling builder is the last one, then spill all builders.
    pub(crate) fn wait_spill(&self) -> Result<bool> {
        {
            let mut waiting_spill_count = self.waiting_spill_count.write();
            *waiting_spill_count += 1;
            if *waiting_spill_count == *self.total_builder_count.read() {
                self.spill()?;
                // No need to wait spill, the processor is the last one
                return Ok(false);
            }
        }
        Ok(true)
    }

    // Get the need_spill flag.
    pub fn get_need_spill(&self) -> bool {
        self.need_spill.load(Ordering::SeqCst)
    }

    // Wait for notify to spill
    pub async fn wait_spill_notify(&self) {
        self.notify_spill.notified().await
    }
}
