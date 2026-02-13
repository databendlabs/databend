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

#[derive(Default)]
pub struct QueryLogDeduplicator {
    // Guards "start" query log emission for a shared query context.
    started: AtomicBool,
    // Guards "finish" query log emission for a shared query context.
    finished: AtomicBool,
    // Guards profile log emission for a shared query context.
    profile: AtomicBool,
}

#[derive(Clone, Copy, Debug)]
pub enum QueryLogEmitPoint {
    Start,
    Finish,
    Profile,
}

impl QueryLogDeduplicator {
    /// Returns true only for the first caller at the given emit point.
    pub fn try_log(&self, emit_point: QueryLogEmitPoint) -> bool {
        let gate = match emit_point {
            QueryLogEmitPoint::Start => &self.started,
            QueryLogEmitPoint::Finish => &self.finished,
            QueryLogEmitPoint::Profile => &self.profile,
        };

        gate.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}
