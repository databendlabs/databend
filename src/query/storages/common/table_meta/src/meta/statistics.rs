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
use databend_common_expression::ScalarRef;
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

#[derive(Serialize, Deserialize, Clone, Debug, Default, FrozenAPI)]
pub struct ColumnTopN {
    pub values: Vec<ColumnTopNEntry>,
    #[serde(skip)]
    #[doc(hidden)]
    pub min_index: Option<usize>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct ColumnTopNEntry {
    pub scalar: Scalar,
    /// Cached frequency for this scalar.
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
        self.scalar
            .cmp(&other.scalar)
            .then_with(|| other.count.cmp(&self.count))
            .then_with(|| self.error.cmp(&other.error))
    }
}

impl PartialEq for ColumnTopN {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

impl Eq for ColumnTopN {}

impl ColumnTopN {
    pub fn get(&self, scalar: &Scalar) -> Option<u64> {
        self.get_entry(scalar).map(|entry| entry.count)
    }

    pub fn get_entry(&self, scalar: &Scalar) -> Option<&ColumnTopNEntry> {
        self.find(&scalar.as_ref())
            .ok()
            .and_then(|index| self.values.get(index))
    }

    pub fn add_with_size(&mut self, top_n_size: usize, scalar: ScalarRef<'_>, count: u64) {
        self.add_ref_with_options(top_n_size, scalar, count, 0);
    }

    pub fn merge_with_size(&mut self, other: ColumnTopN, top_n_size: usize) {
        if top_n_size == 0 {
            self.values.clear();
            self.min_index = None;
            return;
        }

        let lhs_missing_error = self.absent_error(top_n_size);
        let rhs_missing_error = other.absent_error(top_n_size);
        let lhs_values = std::mem::take(&mut self.values);
        self.min_index = None;

        let mut lhs_iter = lhs_values.into_iter().peekable();
        let mut rhs_iter = other.values.into_iter().peekable();
        let mut values = Vec::with_capacity(top_n_size.saturating_mul(2));

        loop {
            match (lhs_iter.peek(), rhs_iter.peek()) {
                (Some(lhs), Some(rhs)) => match lhs.scalar.cmp(&rhs.scalar) {
                    Ordering::Less => {
                        let mut entry = lhs_iter.next().unwrap();
                        entry.count = entry.count.saturating_add(rhs_missing_error);
                        entry.error = entry.error.saturating_add(rhs_missing_error);
                        values.push(entry);
                    }
                    Ordering::Equal => {
                        let mut entry = lhs_iter.next().unwrap();
                        let rhs = rhs_iter.next().unwrap();
                        entry.count = entry.count.saturating_add(rhs.count);
                        entry.error = entry.error.saturating_add(rhs.error);
                        values.push(entry);
                    }
                    Ordering::Greater => {
                        let mut entry = rhs_iter.next().unwrap();
                        entry.count = entry.count.saturating_add(lhs_missing_error);
                        entry.error = entry.error.saturating_add(lhs_missing_error);
                        values.push(entry);
                    }
                },
                (Some(_), None) => {
                    for mut entry in lhs_iter {
                        entry.count = entry.count.saturating_add(rhs_missing_error);
                        entry.error = entry.error.saturating_add(rhs_missing_error);
                        values.push(entry);
                    }
                    break;
                }
                (None, Some(_)) => {
                    for mut entry in rhs_iter {
                        entry.count = entry.count.saturating_add(lhs_missing_error);
                        entry.error = entry.error.saturating_add(lhs_missing_error);
                        values.push(entry);
                    }
                    break;
                }
                (None, None) => break,
            }
        }

        self.values = values;
        self.prune_to_capacity(top_n_size);
    }

    pub fn finish_with_size(mut self, top_n_size: usize) -> Self {
        self.prune_to_capacity(top_n_size);
        self
    }

    fn add_ref_with_options(
        &mut self,
        capacity: usize,
        scalar: ScalarRef<'_>,
        count: u64,
        error: u64,
    ) {
        if capacity == 0 || count == 0 || matches!(scalar, ScalarRef::Null) {
            return;
        }

        if let Ok(index) = self.find(&scalar) {
            self.update_existing(index, count, error);
            return;
        }

        let mut entry = ColumnTopNEntry {
            scalar: scalar.to_owned(),
            count,
            error,
        };
        if self.values.len() >= capacity
            && let Some(min_entry) = self.remove_min()
        {
            entry.count = entry.count.saturating_add(min_entry.count);
            entry.error = entry.error.saturating_add(min_entry.count);
        }
        self.insert_new(entry);
    }

    fn find(&self, scalar: &ScalarRef<'_>) -> std::result::Result<usize, usize> {
        self.values
            .binary_search_by(|entry| entry.scalar.as_ref().cmp(scalar))
    }

    fn update_existing(&mut self, index: usize, count: u64, error: u64) {
        let was_min = self.min_index == Some(index);
        let entry = &mut self.values[index];
        entry.count = entry.count.saturating_add(count);
        entry.error = entry.error.saturating_add(error);
        if was_min {
            self.min_index = None;
        }
    }

    fn insert_new(&mut self, entry: ColumnTopNEntry) {
        let index = self
            .find(&entry.scalar.as_ref())
            .unwrap_or_else(|index| index);
        self.values.insert(index, entry);
        self.on_insert(index);
    }

    fn prune_to_capacity(&mut self, capacity: usize) {
        if self.values.len() <= capacity {
            return;
        }

        while self.values.len() > capacity {
            if self.remove_min().is_none() {
                break;
            }
        }
    }

    fn remove_min(&mut self) -> Option<ColumnTopNEntry> {
        let (min_index, next_min_index) = match self.min_index.take() {
            Some(index) if index < self.values.len() => (index, None),
            _ => self.find_min_and_next_index()?,
        };
        let entry = self.values.remove(min_index);
        self.min_index =
            next_min_index.map(|index| if index > min_index { index - 1 } else { index });
        Some(entry)
    }

    fn absent_error(&self, capacity: usize) -> u64 {
        if self.values.len() < capacity {
            return 0;
        }
        self.values
            .iter()
            .map(|entry| entry.count)
            .min()
            .unwrap_or(0)
    }

    fn on_insert(&mut self, index: usize) {
        if let Some(min_index) = &mut self.min_index {
            if *min_index >= index {
                *min_index += 1;
            }
        }
        match self.min_index {
            None => self.min_index = Some(index),
            Some(min_index) => {
                let count = self.values[index].count;
                let min_count = self.values[min_index].count;
                if count < min_count || (count == min_count && index > min_index) {
                    self.min_index = Some(index);
                }
            }
        }
    }

    fn find_min_and_next_index(&self) -> Option<(usize, Option<usize>)> {
        let first = self.values.first()?;
        let mut min_index = 0;
        let mut min_count = first.count;
        let mut previous_min_index = None;
        let mut second_min: Option<(usize, u64)> = None;

        for (index, entry) in self.values.iter().enumerate().skip(1) {
            let count = entry.count;
            if count < min_count {
                second_min = Some((min_index, min_count));
                min_index = index;
                min_count = count;
                previous_min_index = None;
            } else if count == min_count {
                previous_min_index = Some(min_index);
                min_index = index;
            } else {
                match second_min {
                    None => second_min = Some((index, count)),
                    Some((second_index, second_count))
                        if count < second_count
                            || (count == second_count && index > second_index) =>
                    {
                        second_min = Some((index, count));
                    }
                    _ => {}
                }
            }
        }

        Some((
            min_index,
            previous_min_index.or(second_min.map(|(index, _)| index)),
        ))
    }

    #[cfg(test)]
    fn add_ref_for_test(&mut self, capacity: usize, scalar: ScalarRef<'_>, count: u64, error: u64) {
        self.add_ref_with_options(capacity, scalar, count, error);
    }

    #[cfg(test)]
    fn add_entry_for_test(&mut self, capacity: usize, entry: ColumnTopNEntry) {
        self.add_ref_with_options(capacity, entry.scalar.as_ref(), entry.count, entry.error);
    }
}

pub fn merge_column_top_n_mut(lhs: &mut BlockTopN, rhs: BlockTopN, top_n_size: usize) {
    for (column_id, column_top_n) in rhs {
        match lhs.entry(column_id) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().merge_with_size(column_top_n, top_n_size);
            }
            Entry::Vacant(entry) => {
                entry.insert(column_top_n.finish_with_size(top_n_size));
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
    fn column_top_n_replaces_min_with_error() {
        let mut top_n = ColumnTopN::default();
        top_n.add_entry_for_test(2, ColumnTopNEntry {
            scalar: uint_scalar(1),
            count: 5,
            error: 0,
        });
        top_n.add_entry_for_test(2, ColumnTopNEntry {
            scalar: uint_scalar(2),
            count: 3,
            error: 0,
        });
        top_n.add_entry_for_test(2, ColumnTopNEntry {
            scalar: uint_scalar(3),
            count: 1,
            error: 0,
        });
        assert_eq!(top_n.values.len(), 2);
        assert_eq!(top_n.get(&uint_scalar(1)), Some(5));
        assert_eq!(top_n.get(&uint_scalar(2)), None);
        let entry = top_n.get_entry(&uint_scalar(3)).unwrap();
        assert_eq!(entry.count, 4);
        assert_eq!(entry.error, 3);
    }

    #[test]
    fn column_top_n_merge_accounts_for_missing_side_error() {
        let mut lhs = ColumnTopN::default();
        lhs.add_entry_for_test(2, ColumnTopNEntry {
            scalar: uint_scalar(1),
            count: 10,
            error: 0,
        });
        lhs.add_entry_for_test(2, ColumnTopNEntry {
            scalar: uint_scalar(2),
            count: 5,
            error: 0,
        });
        let mut rhs = ColumnTopN::default();
        rhs.add_entry_for_test(2, ColumnTopNEntry {
            scalar: uint_scalar(1),
            count: 7,
            error: 0,
        });
        rhs.add_entry_for_test(2, ColumnTopNEntry {
            scalar: uint_scalar(3),
            count: 4,
            error: 0,
        });

        lhs.merge_with_size(rhs, 2);

        assert_eq!(lhs.get(&uint_scalar(1)), Some(17));
        let entry = lhs.get_entry(&uint_scalar(2)).unwrap();
        assert_eq!(entry.count, 9);
        assert_eq!(entry.error, 4);
    }

    #[test]
    fn column_top_n_adds_scalar_refs() {
        let mut top_n = ColumnTopN::default();
        top_n.add_ref_for_test(2, ScalarRef::String("b"), 1, 0);
        top_n.add_ref_for_test(2, ScalarRef::String("a"), 1, 0);
        top_n.add_ref_for_test(2, ScalarRef::String("a"), 4, 0);
        top_n.add_ref_for_test(2, ScalarRef::String("c"), 1, 0);

        assert_eq!(top_n.values.len(), 2);
        assert!(
            top_n
                .values
                .windows(2)
                .all(|pair| pair[0].scalar < pair[1].scalar)
        );
        assert_eq!(top_n.get(&Scalar::String("a".to_string())), Some(5));
        assert_eq!(top_n.get(&Scalar::String("a".to_string())), Some(5));
        assert_eq!(top_n.get(&Scalar::String("b".to_string())), None);
        let entry = top_n
            .get_entry(&Scalar::String("c".to_string()))
            .expect("new scalar should replace the current minimum");
        assert_eq!(entry.count, 2);
        assert_eq!(entry.error, 1);
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
