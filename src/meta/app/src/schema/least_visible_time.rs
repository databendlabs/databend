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

use chrono::DateTime;
use chrono::Utc;

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct LeastVisibleTime {
    pub time: DateTime<Utc>,
}

/// One batched LVT publication entry: `(table_id, observed_lvt_seq, candidate_lvt)`.
pub type LvtUpdate = (u64, u64, LeastVisibleTime);

impl LeastVisibleTime {
    pub fn new(time: DateTime<Utc>) -> Self {
        Self { time }
    }

    /// A writable fence without a historical lower bound. Every representable snapshot
    /// timestamp is >= this value, including timestamps before the Unix epoch.
    pub fn unbounded() -> Self {
        Self::new(DateTime::<Utc>::MIN_UTC)
    }

    pub fn is_unbounded(&self) -> bool {
        self.time == DateTime::<Utc>::MIN_UTC
    }
}
