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

use std::cell::Cell;
use std::time::Duration;

thread_local! {
    /// Nanoseconds the current thread has spent blocked in
    /// `OperatorRangeReader::read` waiting for a range to arrive from the fetch worker.
    ///
    /// Synchronous readers (for example recluster group sources running as
    /// `SyncSource`) wrap their `read()` calls with [`blocked_nanos`] snapshots to tell
    /// "waiting on object storage" apart from "decoding the bytes that already arrived".
    /// The executor's `CpuTime` for a sync processor cannot make that distinction.
    static BLOCKED_NANOS: Cell<u64> = const { Cell::new(0) };
}

/// Total time this thread has spent blocked on object-storage reads so far. Monotonic
/// within a thread; callers diff two snapshots.
#[inline]
pub fn blocked_nanos() -> u64 {
    BLOCKED_NANOS.with(Cell::get)
}

#[inline]
pub(crate) fn record_blocked(elapsed: Duration) {
    BLOCKED_NANOS.with(|c| c.set(c.get().saturating_add(elapsed.as_nanos() as u64)));
}
