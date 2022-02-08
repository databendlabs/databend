// Copyright 2022 Datafuse Labs.
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

#[derive(Clone, Debug, Default)]
pub struct DalMetrics {
    /// Read bytes.
    pub read_bytes: usize,
    /// Seek times of read.
    pub read_seeks: usize,
    /// Cost(in ms) of read bytes.
    pub read_byte_cost_ms: usize,
    /// Cost(in ms) of seek by reading.
    pub read_seek_cost_ms: usize,
    /// Bytes written by data access layer
    pub write_bytes: usize,
    /// Number of rows written
    pub write_rows: usize,
    /// Number of partitions scanned, after pruning
    pub partitions_scanned: usize,
    /// Number of partitions, before pruning
    pub partitions_total: usize,
}
