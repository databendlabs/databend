// Copyright 2021 Datafuse Labs.
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

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone, Debug, Default)]
pub struct Statistics {
    /// Total rows of the query read.
    pub read_rows: usize,
    /// Total bytes of the query read.
    pub read_bytes: usize,
    /// Number of partitions scanned, (after pruning)
    pub partitions_scanned: usize,
    /// Number of partitions, (before pruning)
    pub partitions_total: usize,
    /// Is the statistics exact.
    pub is_exact: bool,
}

impl Statistics {
    pub fn new_estimated(
        read_rows: usize,
        read_bytes: usize,
        partitions_scanned: usize,
        partitions_total: usize,
    ) -> Self {
        Statistics {
            read_rows,
            read_bytes,
            partitions_scanned,
            partitions_total,
            is_exact: false,
        }
    }

    pub fn new_exact(
        read_rows: usize,
        read_bytes: usize,
        partitions_scanned: usize,
        partitions_total: usize,
    ) -> Self {
        Statistics {
            read_rows,
            read_bytes,
            partitions_scanned,
            partitions_total,
            is_exact: true,
        }
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }
}
