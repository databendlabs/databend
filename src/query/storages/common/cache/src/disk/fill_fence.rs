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

//! Orders disk fills against invalidations. A fill takes a ticket before it reads the object
//! from storage, and the end of a change of an object (`DiskCache::begin_change`) takes a later
//! point of the same clock; a pool then rejects an entry whose fill started before the last
//! invalidation of its object. Bytes
//! read before an object was rewritten or deleted are so never published after it, however
//! long they wait in the write queue.
//!
//! The pools keep invalidation points only while a fill started before them runs; when the
//! oldest fill finishes, the fence tells them which points they can drop.

use std::collections::BTreeSet;
use std::sync::Arc;

use parking_lot::Mutex;

/// Called with a point when the oldest fill finishes: no fill running started before it.
pub type OnAdvance = Box<dyn Fn(u64) + Send + Sync>;

#[derive(Default)]
pub struct FillFence {
    state: Mutex<FenceState>,
    on_advance: Option<OnAdvance>,
}

#[derive(Default)]
struct FenceState {
    clock: u64,
    /// The start points of the fills not finished yet.
    active: BTreeSet<u64>,
}

/// Held by a fill from before its storage read until its entry is published or dropped.
/// Clones share the start point, for the units of one read.
#[derive(Clone)]
pub struct FillTicket(Arc<Ticket>);

struct Ticket {
    fence: Arc<FillFence>,
    started: u64,
}

impl FillFence {
    /// A fence that calls `on_advance` when the oldest fill finishes.
    pub fn with_on_advance(on_advance: OnAdvance) -> Self {
        FillFence {
            state: Mutex::default(),
            on_advance: Some(on_advance),
        }
    }

    /// Starts a fill.
    pub fn begin(self: &Arc<Self>) -> FillTicket {
        let mut state = self.state.lock();
        let started = state.clock;
        state.clock += 1;
        state.active.insert(started);
        FillTicket(Arc::new(Ticket {
            fence: self.clone(),
            started,
        }))
    }

    /// A point after every fill started so far, for an invalidation, and the start of the
    /// oldest fill still running: invalidations before it cannot reject anything any more.
    pub fn invalidation_point(&self) -> (u64, Option<u64>) {
        let mut state = self.state.lock();
        let at = state.clock;
        state.clock += 1;
        (at, state.active.first().copied())
    }
}

impl FillTicket {
    /// The point the fill started at.
    pub fn started(&self) -> u64 {
        self.0.started
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        let advanced = {
            let mut state = self.fence.state.lock();
            let oldest = state.active.first() == Some(&self.started);
            state.active.remove(&self.started);
            // No fill running started before the new oldest one, or before the clock.
            oldest.then(|| state.active.first().copied().unwrap_or(state.clock))
        };
        // Outside the fence lock: the callback takes the pool locks.
        if let (Some(point), Some(on_advance)) = (advanced, &self.fence.on_advance) {
            on_advance(point);
        }
    }
}
