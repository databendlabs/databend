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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io::Cursor;

use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_frozen_api::FrozenAPI;
use databend_common_storage::MetaHLL;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::meta::ColumnStatistics;
use crate::meta::SegmentStatistics;
use crate::meta::SpatialStatistics;
use crate::meta::format::compress;
use crate::meta::format::encode;
use crate::meta::format::read_and_deserialize;

pub type FormatVersion = u64;
pub type SnapshotId = Uuid;
pub type Location = (String, FormatVersion);
pub type ClusterKey = (u32, String);
pub type StatisticsOfColumns = HashMap<ColumnId, ColumnStatistics>;
pub type StatisticsOfSpatialColumns = HashMap<ColumnId, SpatialStatistics>;
pub type BlockHLL = HashMap<ColumnId, MetaHLL>;
pub type BlockTopN = HashMap<ColumnId, ColumnTopN>;
pub type RawBlockHLL = Vec<u8>;
pub const DEFAULT_TOP_N_SIZE: usize = 100;

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq, FrozenAPI)]
pub struct ColumnTopN {
    pub values: Vec<ColumnTopNEntry>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct ColumnTopNEntry {
    pub scalar: Scalar,
    /// Space-Saving estimated frequency. The true frequency is in
    /// `[count - error, count]`.
    pub count: u64,
    pub error: u64,
}

impl PartialOrd for ColumnTopNEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ColumnTopNEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .count
            .cmp(&self.count)
            .then_with(|| self.error.cmp(&other.error))
            .then_with(|| self.scalar.cmp(&other.scalar))
    }
}

impl ColumnTopN {
    pub fn get(&self, scalar: &Scalar) -> Option<u64> {
        self.get_entry(scalar).map(|entry| entry.count)
    }

    pub fn get_entry(&self, scalar: &Scalar) -> Option<&ColumnTopNEntry> {
        self.values.iter().find(|entry| &entry.scalar == scalar)
    }
}

#[derive(Clone, Debug)]
pub struct ColumnTopNBuilder {
    capacity: usize,
    values: Vec<ColumnTopNEntry>,
}

impl Default for ColumnTopNBuilder {
    fn default() -> Self {
        Self::new(DEFAULT_TOP_N_SIZE)
    }
}

impl ColumnTopNBuilder {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            capacity,
            values: Vec::with_capacity(capacity),
        }
    }

    pub fn add(&mut self, scalar: Scalar, count: u64) {
        self.add_with_error(scalar, count, 0);
    }

    pub fn merge(&mut self, other: ColumnTopN) {
        for entry in other.values {
            self.add_with_error(entry.scalar, entry.count, entry.error);
        }
    }

    pub fn finish(self) -> ColumnTopN {
        ColumnTopN {
            values: self.values,
        }
    }

    fn add_with_error(&mut self, scalar: Scalar, count: u64, error: u64) {
        if count == 0 {
            return;
        }
        if let Some(index) = self.values.iter().position(|entry| entry.scalar == scalar) {
            let mut entry = self.values.remove(index);
            entry.count = entry.count.saturating_add(count);
            entry.error = entry.error.saturating_add(error);
            self.insert_entry(entry);
            return;
        }

        if self.values.len() < self.capacity {
            self.insert_entry(ColumnTopNEntry {
                scalar,
                count,
                error,
            });
            return;
        }

        let Some(min_entry) = self.values.pop() else {
            return;
        };
        let min_count = min_entry.count;
        self.insert_entry(ColumnTopNEntry {
            scalar,
            count: min_count.saturating_add(count),
            error: min_count.saturating_add(error),
        });
    }

    fn insert_entry(&mut self, entry: ColumnTopNEntry) {
        let index = self
            .values
            .binary_search_by(|existing| existing.cmp(&entry))
            .unwrap_or_else(|index| index);
        self.values.insert(index, entry);
    }
}

pub fn merge_column_top_n_mut(lhs: &mut BlockTopN, rhs: BlockTopN) {
    for (column_id, column_top_n) in rhs {
        match lhs.entry(column_id) {
            Entry::Occupied(mut entry) => {
                let mut builder = ColumnTopNBuilder::default();
                builder.merge(std::mem::take(entry.get_mut()));
                builder.merge(column_top_n);
                *entry.get_mut() = builder.finish();
            }
            Entry::Vacant(entry) => {
                entry.insert(column_top_n);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::NumberScalar;

    use super::*;

    fn uint_scalar(value: u64) -> Scalar {
        Scalar::Number(NumberScalar::UInt64(value))
    }

    #[test]
    fn test_column_top_n_builder_uses_space_saving_capacity() {
        let mut builder = ColumnTopNBuilder::new(2);
        builder.add(uint_scalar(1), 5);
        builder.add(uint_scalar(2), 3);
        builder.add(uint_scalar(3), 1);

        let top_n = builder.finish();
        assert_eq!(top_n.values.len(), 2);
        assert_eq!(top_n.get(&uint_scalar(1)), Some(5));

        let entry = top_n.get_entry(&uint_scalar(3)).unwrap();
        assert_eq!(entry.count, 4);
        assert_eq!(entry.error, 3);
    }
}

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
