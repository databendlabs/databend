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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_base::base::tokio::sync::watch;
use databend_common_base::base::tokio::sync::watch::Receiver;
use databend_common_base::base::tokio::sync::watch::Sender;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use log::info;
use parking_lot::Mutex;
use parking_lot::RwLock;

/// Coordinate all hash join build processors to spill.
/// It's shared by all hash join build processors.
/// When hash join build needs to spill, all processor will stop executing and prepare to spill.
/// The last one will be as the coordinator to spill all processors and then wake up all processors to continue executing.
pub struct BuildSpillCoordinator {
    /// Need to spill, if one of the builders need to spill, this flag will be set to true.
    need_spill: AtomicBool,
    /// Current waiting spilling processor count.
    pub(crate) waiting_spill_count: AtomicUsize,
    /// Total processor count.
    pub(crate) total_builder_count: usize,
    /// Spill tasks, the size is the same as the total active processor count.
    pub(crate) spill_tasks: Mutex<VecDeque<Vec<(u8, DataBlock)>>>,
    /// When a build processor won't trigger spill, the field will plus one
    pub(crate) non_spill_processors: RwLock<usize>,
    /// If there is the last active processor, send true to watcher channel
    pub(crate) ready_spill_watcher: Sender<bool>,
    pub(crate) dummy_ready_spill_receiver: Receiver<bool>,
    pub(crate) mutex: Mutex<()>,
}

impl BuildSpillCoordinator {
    pub fn create(total_builder_count: usize) -> Arc<Self> {
        let (ready_spill_watcher, dummy_ready_spill_receiver) = watch::channel(false);
        Arc::new(Self {
            need_spill: Default::default(),
            waiting_spill_count: Default::default(),
            total_builder_count,
            spill_tasks: Default::default(),
            non_spill_processors: Default::default(),
            ready_spill_watcher,
            dummy_ready_spill_receiver,
            mutex: Default::default(),
        })
    }

    // Called by hash join build processor, if current processor need to spill, then set `need_spill` to true.
    pub fn need_spill(&self) -> Result<()> {
        self.need_spill.store(true, Ordering::Relaxed);
        Ok(())
    }

    // If current waiting spilling builder is the last one, then spill all builders.
    pub(crate) fn wait_spill(&self) -> Result<bool> {
        let _ = self.mutex.lock();
        if *self.dummy_ready_spill_receiver.borrow() {
            self.ready_spill_watcher
                .send(false)
                .map_err(|_| ErrorCode::TokioError("ready_spill_watcher channel is closed"))?;
        }
        let non_spill_processors = self.non_spill_processors.read();
        let old_val = self.waiting_spill_count.fetch_add(1, Ordering::Release);
        let waiting_spill_count = old_val + 1;
        info!(
            "waiting_spill_count: {:?}, non_spill_processors: {:?}, total_builder_count: {:?}",
            waiting_spill_count, *non_spill_processors, self.total_builder_count
        );

        if (waiting_spill_count + *non_spill_processors == self.total_builder_count)
            && self.get_need_spill()
        {
            self.no_need_spill();
            // Reset waiting_spill_count
            self.waiting_spill_count.store(0, Ordering::Relaxed);
            // No need to wait spill, the processor is the last one
            return Ok(false);
        }
        Ok(true)
    }

    // Get the need_spill flag.
    pub fn get_need_spill(&self) -> bool {
        self.need_spill.load(Ordering::Relaxed)
    }

    // Set the need_spill flag to false.
    pub fn no_need_spill(&self) {
        self.need_spill.store(false, Ordering::Relaxed);
    }

    // Wait the last processor to notify spill
    pub async fn wait_spill_notify(&self) -> Result<()> {
        let mut rx = self.ready_spill_watcher.subscribe();
        if *rx.borrow() {
            return Ok(());
        }
        rx.changed().await.map_err(|_| {
            ErrorCode::TokioError("ready_spill_watcher channel's sender is dropped")
        })?;
        debug_assert!(*rx.borrow());
        Ok(())
    }

    // Get active processor count
    pub fn active_processor_num(&self) -> usize {
        self.total_builder_count - *self.non_spill_processors.read()
    }
}
