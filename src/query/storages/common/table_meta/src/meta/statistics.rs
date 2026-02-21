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
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::io::Cursor;

use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::types::DataType;
use databend_common_frozen_api::FrozenAPI;
use databend_common_storage::MetaHLL;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::meta::ColumnStatistics;
use crate::meta::SegmentStatistics;
use crate::meta::format::compress;
use crate::meta::format::encode;
use crate::meta::format::read_and_deserialize;

pub type FormatVersion = u64;
pub type SnapshotId = Uuid;
pub type Location = (String, FormatVersion);
pub type ClusterKey = (u32, String);
pub type StatisticsOfColumns = HashMap<ColumnId, ColumnStatistics>;
pub type BlockHLL = HashMap<ColumnId, MetaHLL>;
pub type RawBlockHLL = Vec<u8>;

// Assigned to executors, describes that which blocks of given segment, an executor should take care of
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct BlockSlotDescription {
    // number of slots
    pub num_slots: usize,
    // index of slot that current executor should take care of:
    // let `block_index` be the index of block in segment, and `segment_location` the segment location,
    // hash(segment_location, block_index) mod `num_slots` == `slot_index` indicates that the block should be taken care of by current executor
    // otherwise, the block should be taken care of by other executors
    pub slot: u32,
}

impl BlockSlotDescription {
    pub fn matches(&self, segment_location: &Location, block_idx: usize) -> bool {
        let mut hasher = DefaultHasher::new();
        segment_location.hash(&mut hasher);
        block_idx.hash(&mut hasher);
        (hasher.finish() % self.num_slots as u64) == self.slot as u64
    }
}

pub fn supported_stat_type(data_type: &DataType) -> bool {
    let inner_type = data_type.remove_nullable();
    matches!(
        inner_type,
        DataType::Number(_)
            | DataType::Date
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::String
            | DataType::Decimal(_)
    )
}

pub fn encode_column_hll(hll: &BlockHLL) -> Result<RawBlockHLL> {
    let encoding = SegmentStatistics::encoding();
    let compression = SegmentStatistics::compression();

    let data = encode(&encoding, hll)?;
    let data_compress = compress(&compression, data)?;
    Ok(data_compress)
}

pub fn decode_column_hll(data: &RawBlockHLL) -> Result<Option<BlockHLL>> {
    if data.is_empty() {
        return Ok(None);
    }
    let encoding = SegmentStatistics::encoding();
    let compression = SegmentStatistics::compression();
    let mut reader = Cursor::new(&data);
    let res = read_and_deserialize(&mut reader, data.len() as u64, &encoding, &compression)?;
    Ok(Some(res))
}

pub fn merge_column_hll(mut lhs: BlockHLL, rhs: BlockHLL) -> BlockHLL {
    merge_column_hll_mut(&mut lhs, &rhs);
    lhs
}

pub fn merge_column_hll_mut(lhs: &mut BlockHLL, rhs: &BlockHLL) {
    for (column_id, column_hll) in rhs.iter() {
        lhs.entry(*column_id)
            .and_modify(|hll| hll.merge(column_hll))
            .or_insert_with(|| column_hll.clone());
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub enum BlockHLLState {
    Serialized(RawBlockHLL),
    Deserialized(BlockHLL),
}

impl BlockHLLState {
    pub fn merge_column_hll(lhs: &mut BlockHLL, rhs: &Option<BlockHLLState>) {
        if let Some(BlockHLLState::Deserialized(v)) = rhs {
            merge_column_hll_mut(lhs, v);
        }
    }

    pub fn encode_column_hll(hll: Option<BlockHLLState>) -> Result<Option<RawBlockHLL>> {
        hll.map(|h| match h {
            BlockHLLState::Deserialized(v) => encode_column_hll(&v),
            BlockHLLState::Serialized(v) => Ok(v),
        })
        .transpose()
    }
}
