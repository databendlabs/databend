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

use crate::runtime::ThreadTracker;
use crate::runtime::TrackingPayload;
use crate::runtime::memory::GLOBAL_MEM_STAT;
use crate::runtime::memory::MemStat;
use crate::runtime::memory::ParentMemStat;

/// Measures the memory that the calls it tracks allocated since it was last marked and that is
/// still allocated: what a structure holds after those calls, including memory its parts
/// allocate on their own (for example the values of an `array_agg` state), which the structure
/// cannot size itself.
///
/// The calls run under a child of the memory stat of the current thread, so its parents (the
/// plan, the query, the workload group) still account for them and enforce their limits. An
/// allocation is accounted to the stat it was made under until it is freed, on any thread, so
/// each mark starts a new child: what was allocated before it is freed into the previous one.
/// Like the rest of memory tracking, it only sees allocations of at least 512 bytes.
///
/// It is not the memory of an operator, which the memory stat of each plan already gives: it
/// fits an owner that builds something, hands it over and marks the meter.
///
/// Each call flushes the per-thread allocation buffer twice, so track coarse calls (a batch of
/// rows, not a row).
#[derive(Default)]
pub struct MemoryMeter {
    /// The tracking payload of the thread at the first call, whose memory stat is the parent.
    base: Option<TrackingPayload>,
    /// The tracking payload of the calls since the last mark, and its memory stat.
    current: Option<(Arc<TrackingPayload>, Arc<MemStat>)>,
}

impl MemoryMeter {
    /// Run `f` with its allocations accounted to the meter.
    pub fn track<R>(&mut self, f: impl FnOnce() -> R) -> R {
        let base = self
            .base
            .get_or_insert_with(ThreadTracker::new_tracking_payload);
        let (payload, _) = self.current.get_or_insert_with(|| {
            let parent = match &base.mem_stat {
                Some(mem_stat) => ParentMemStat::Normal(mem_stat.clone()),
                None => ParentMemStat::StaticRef(&GLOBAL_MEM_STAT),
            };
            let mem_stat = MemStat::create_child(None, 0, parent);
            let mut payload = base.clone();
            payload.mem_stat = Some(mem_stat.clone());
            (Arc::new(payload), mem_stat)
        });
        let guard = ThreadTracker::tracking_inner(payload.clone());
        let result = f();
        // Allocations are buffered per thread; flush them into the child before it is read.
        // The parents still check their limits on the allocations that follow.
        let _ = guard.flush();
        result
    }

    /// Bytes the tracked calls allocated since the last mark and that are still allocated.
    pub fn bytes(&self) -> usize {
        self.current
            .as_ref()
            .map_or(0, |(_, mem_stat)| mem_stat.get_memory_usage())
    }

    /// Start counting from now, for example once what was built has been handed over.
    pub fn mark(&mut self) {
        self.current = None;
    }
}
