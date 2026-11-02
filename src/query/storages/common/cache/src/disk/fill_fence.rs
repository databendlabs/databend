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
//! from storage, and `DiskCache::evict_object` takes a later point of the same clock; a pool
//! then rejects an entry whose fill started before the last invalidation of its object. Bytes
//! read before an object was rewritten or deleted are so never published after it, however
//! long they wait in the write queue.

use std::collections::BTreeSet;
use std::sync::Arc;

use parking_lot::Mutex;

#[derive(Default)]
pub struct FillFence {
    state: Mutex<FenceState>,
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
        self.fence.state.lock().active.remove(&self.started);
    }
}
