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

use std::fmt;
use std::fmt::Formatter;

/// Snapshot stat.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SnapshotStat {
    /// Total number of entries in the snapshot.
    ///
    /// Including meta entries, such as seq, nodes, generic kv, and expire index
    pub entry_cnt: u64,

    /// Size in bytes of the snapshot file
    pub size: u64,
}

impl fmt::Display for SnapshotStat {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{ entry_cnt: {}, size: {} }}",
            self.entry_cnt, self.size
        )
    }
}
