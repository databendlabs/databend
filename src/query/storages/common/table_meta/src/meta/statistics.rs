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

use std::collections::HashMap;

use databend_common_base::base::uuid::Uuid;
use databend_common_expression::ColumnId;

use crate::meta::ColumnStatistics;

pub type FormatVersion = u64;
pub type SnapshotId = Uuid;
pub type Location = (String, FormatVersion);
pub type ClusterKey = (u32, String);
pub type StatisticsOfColumns = HashMap<ColumnId, ColumnStatistics>;

// Assigned to executors, describes that which blocks of given segment, an executor should take care of
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct BlockSlotDescription {
    // number of slots
    pub num_slots: usize,
    // index of slot that current executor should take care of:
    // let `block_index` be the index of block in segment,
    // `block_index` mod `num_slots` == `slot_index` indicates that the block should be taken care of by current executor
    // otherwise, the block should be taken care of by other executors
    pub slot: u32,
}
