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

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::Hash;
use std::hash::Hasher;
use std::io::Cursor;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::DataType;
use databend_common_frozen_api::FrozenAPI;
use databend_common_storage::MetaHLL;
use serde::Deserialize;
use serde::Serialize;
use siphasher::sip::SipHasher24;
use uuid::Uuid;

use crate::meta::ClusterStatistics;
use crate::meta::ColumnStatistics;
use crate::meta::SegmentStatistics;
use crate::meta::SpatialStatistics;
use crate::meta::VectorColumnStatistics;
use crate::meta::VectorDistanceType;
use crate::meta::format::compress;
use crate::meta::format::encode;
use crate::meta::format::read_and_deserialize;
use crate::meta::stat_cmp::try_cmp_stat_scalar_slices;
use crate::meta::stat_cmp::try_cmp_stat_scalars;
use crate::table::ClusterType;
use crate::table::HILBERT_CLUSTER_DIMENSIONS;

pub type FormatVersion = u64;
pub type SnapshotId = Uuid;
pub type Location = (String, FormatVersion);
pub type ClusterKey = (u32, String);
pub type StatisticsOfColumns = HashMap<ColumnId, ColumnStatistics>;
pub type StatisticsOfSpatialColumns = HashMap<ColumnId, SpatialStatistics>;
pub type StatisticsOfVectorColumns =
    HashMap<(ColumnId, VectorDistanceType), VectorColumnStatistics>;
pub type BlockHLL = HashMap<ColumnId, MetaHLL>;
pub type BlockTopN = HashMap<ColumnId, ColumnTopN>;
pub type BlockCountMinSketch = HashMap<ColumnId, ColumnCountMinSketch>;
pub type RawBlockHLL = Vec<u8>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterKeyInfo {
    pub cluster_key: ClusterKey,
    pub cluster_type: ClusterType,
}

impl ClusterKeyInfo {
    pub fn new(cluster_key: ClusterKey, cluster_type: ClusterType) -> Self {
        Self {
            cluster_key,
            cluster_type,
        }
    }

    pub fn cluster_key_id(&self) -> u32 {
        self.cluster_key.0
    }
}

/// Return the trailing Hilbert MBR bounds when the persisted layout is valid.
/// Linear partition dimensions may precede the Hilbert dimensions; callers must
/// already have established that the table uses Hilbert clustering.
pub fn valid_cluster_stats_hilbert_minmax(
    stats: &ClusterStatistics,
    expected_dim_len: usize,
) -> Option<(&[Scalar], &[Scalar])> {
    let min = stats.min();
    let max = stats.max();
    if min.len() != max.len() || min.len() < expected_dim_len {
        return None;
    }
    let start = min.len() - expected_dim_len;
    let min = &min[start..];
    let max = &max[start..];
    if min
        .iter()
        .zip(max)
        .any(|(min, max)| min.as_ref().cmp(&max.as_ref()) == Ordering::Greater)
    {
        return None;
    }
    Some((min, max))
}

/// Conservative scalar-key overlap check; incomplete stats are treated as overlapping.
pub fn cluster_stats_scalar_overlap(left: &ClusterStatistics, right: &ClusterStatistics) -> bool {
    let left_min = left.min();
    let left_max = left.max();
    let right_min = right.min();
    let right_max = right.max();
    if left_min.len() != left_max.len()
        || right_min.len() != right_max.len()
        || left_min.len() != right_min.len()
    {
        return true;
    }

    // An inconclusive comparison cannot prove the ranges are apart, so report overlap. This
    // happens when a metadata-only decimal precision widening leaves one side tagged with the
    // previous `DecimalSize` in a way that cannot be reconciled.
    match (
        scalar_tuple_cmp(left_min, right_max),
        scalar_tuple_cmp(right_min, left_max),
    ) {
        (Some(left), Some(right)) => left != Ordering::Greater && right != Ordering::Greater,
        _ => true,
    }
}

pub fn reduce_cluster_statistics<T: Borrow<Option<ClusterStatistics>>>(
    cluster_stats: &[T],
    cluster_key_info: Option<&ClusterKeyInfo>,
) -> Option<ClusterStatistics> {
    let cluster_key_info = cluster_key_info?;
    let cluster_key_id = cluster_key_info.cluster_key_id();
    let cluster_type = cluster_key_info.cluster_type;
    if cluster_stats.is_empty() {
        return None;
    }

    let mut min_stats = Vec::with_capacity(cluster_stats.len());
    let mut max_stats = Vec::with_capacity(cluster_stats.len());
    let mut levels = Vec::with_capacity(cluster_stats.len());
    for stats in cluster_stats {
        let stats = stats.borrow().as_ref()?;
        if stats.cluster_key_id != cluster_key_id {
            return None;
        }
        min_stats.push(stats.min().as_slice());
        max_stats.push(stats.max().as_slice());
        levels.push(stats.level);
    }

    let (min, max) = reduce_cluster_min_max(&min_stats, &max_stats, cluster_type)?;
    let level = levels.into_iter().max().unwrap_or(0);
    Some(ClusterStatistics::new(cluster_key_id, min, max, level))
}

pub fn reduce_cluster_min_max(
    min_stats: &[&[Scalar]],
    max_stats: &[&[Scalar]],
    cluster_type: ClusterType,
) -> Option<(Vec<Scalar>, Vec<Scalar>)> {
    debug_assert_eq!(min_stats.len(), max_stats.len());
    if min_stats.is_empty() {
        return None;
    }

    match cluster_type {
        ClusterType::Linear => {
            // Pick the extremes explicitly instead of `min_by`/`max_by`: those swallow an
            // inconclusive comparison as `Equal` and would take each end from a different input,
            // producing a range whose min and max disagree. Bail out instead.
            let mut min = min_stats[0];
            let mut max = max_stats[0];
            for next in min_stats.iter().skip(1) {
                if scalar_tuple_cmp(next, min)? == Ordering::Less {
                    min = next;
                }
            }
            for next in max_stats.iter().skip(1) {
                if scalar_tuple_cmp(next, max)? == Ordering::Greater {
                    max = next;
                }
            }
            Some((min.to_vec(), max.to_vec()))
        }
        ClusterType::Hilbert => {
            if min_stats.len() != max_stats.len()
                || min_stats.iter().zip(max_stats).any(|(min, max)| {
                    min.len() != HILBERT_CLUSTER_DIMENSIONS
                        || max.len() != HILBERT_CLUSTER_DIMENSIONS
                        || min.iter().zip(*max).any(|(min, max)| {
                            // An inconclusive comparison means we cannot confirm min <= max, so
                            // reject the input rather than build a range we cannot verify.
                            !matches!(
                                try_cmp_stat_scalars(&min.as_ref(), &max.as_ref()),
                                Some(Ordering::Less | Ordering::Equal)
                            )
                        })
                })
            {
                return None;
            }

            let mut min = min_stats[0].to_vec();
            let mut max = max_stats[0].to_vec();
            for (next_min, next_max) in min_stats.iter().zip(max_stats).skip(1) {
                for dimension in 0..HILBERT_CLUSTER_DIMENSIONS {
                    if try_cmp_stat_scalars(&next_min[dimension].as_ref(), &min[dimension].as_ref())
                        == Some(Ordering::Less)
                    {
                        min[dimension] = next_min[dimension].clone();
                    }
                    if try_cmp_stat_scalars(&next_max[dimension].as_ref(), &max[dimension].as_ref())
                        == Some(Ordering::Greater)
                    {
                        max[dimension] = next_max[dimension].clone();
                    }
                }
            }
            Some((min, max))
        }
    }
}

/// Lexicographic comparison of cluster statistics tuples.
///
/// Returns `None` when the tuples are not comparable, so callers must decide what that means for
/// them rather than silently treating the values as equal.
fn scalar_tuple_cmp(left: &[Scalar], right: &[Scalar]) -> Option<Ordering> {
    try_cmp_stat_scalar_slices(left, right)
}

const COUNT_MIN_SKETCH_WIDTH: usize = 2048;
const MAX_COUNT_MIN_SKETCH_WIDTH: usize = 1 << 20;
const COUNT_MIN_SKETCH_DEPTH: usize = 5;
const MIN_COUNT_MIN_SKETCH_ERROR_RATE: f64 = 2.0 / MAX_COUNT_MIN_SKETCH_WIDTH as f64;
const MAX_COUNT_MIN_SKETCH_ERROR_RATE: f64 = 1.0;

const COUNT_MIN_SKETCH_HASH0_KEY0_V1: u64 = 0x459b_4c9e_5d6e_f817_u64;
const COUNT_MIN_SKETCH_HASH0_KEY1_V1: u64 = 0x7d31_8741_1d8c_2f23_u64;
const COUNT_MIN_SKETCH_HASH1_KEY0_V1: u64 = 0xd7a6_5b21_f07c_3e91_u64;
const COUNT_MIN_SKETCH_HASH1_KEY1_V1: u64 = 0x238f_9c4d_6b15_a0e7_u64;

#[derive(Serialize, Deserialize, Clone, Debug, Default, FrozenAPI)]
pub struct ColumnTopN {
    #[serde(default)]
    pub capacity: usize,
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
        self.capacity == other.capacity && self.values == other.values
    }
}

impl Eq for ColumnTopN {}

impl ColumnTopN {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            values: vec![],
            min_index: None,
        }
    }

    pub fn get(&self, scalar: &Scalar) -> Option<u64> {
        self.get_entry(scalar).map(|entry| entry.count)
    }

    pub fn get_entry(&self, scalar: &Scalar) -> Option<&ColumnTopNEntry> {
        self.find(&scalar.as_ref())
            .ok()
            .and_then(|index| self.values.get(index))
    }

    pub fn add(&mut self, scalar: ScalarRef<'_>, count: u64) {
        self.add_ref_with_options(scalar, count, 0);
    }

    pub fn merge(&mut self, other: ColumnTopN) -> Result<()> {
        if self.capacity == 0 {
            self.capacity = other.capacity;
        }
        let lhs_missing_error = self.absent_error();
        let rhs_missing_error = other.absent_error();

        if other.capacity != 0 {
            self.capacity = other.capacity;
        }

        if self.capacity == 0 {
            self.values.clear();
            self.min_index = None;
            return Ok(());
        }

        let lhs_values = std::mem::take(&mut self.values);
        self.min_index = None;

        let mut lhs_iter = lhs_values.into_iter().peekable();
        let mut rhs_iter = other.values.into_iter().peekable();
        let mut values = Vec::with_capacity(self.capacity.saturating_mul(2));

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
        self.prune_to_capacity();
        Ok(())
    }

    pub fn finish(mut self) -> Self {
        self.prune_to_capacity();
        self
    }

    fn add_ref_with_options(&mut self, scalar: ScalarRef<'_>, count: u64, error: u64) {
        if self.capacity == 0 || count == 0 || matches!(scalar, ScalarRef::Null) {
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
        if self.values.len() >= self.capacity
            && let Some(min_entry) = self.remove_min()
        {
            entry.count = entry.count.saturating_add(min_entry.count);
            entry.error = entry.error.saturating_add(min_entry.count);
        }
        self.insert_new(entry);
    }

    fn find(&self, scalar: &ScalarRef<'_>) -> std::result::Result<usize, usize> {
        // `binary_search_by` needs a total order, and raw `Ord` supplies one only by collapsing
        // incomparable pairs to `Equal`. Compare through the statistics-aware helper and treat an
        // inconclusive result as `Greater`, so the search still terminates and the existing
        // `partial_cmp` recheck below rejects a bogus hit.
        match self.values.binary_search_by(|entry| {
            try_cmp_stat_scalars(&entry.scalar.as_ref(), scalar).unwrap_or(Ordering::Greater)
        }) {
            Ok(index)
                if try_cmp_stat_scalars(&self.values[index].scalar.as_ref(), scalar)
                    == Some(Ordering::Equal) =>
            {
                Ok(index)
            }
            Ok(index) | Err(index) => Err(index),
        }
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

    fn prune_to_capacity(&mut self) {
        if self.values.len() <= self.capacity {
            return;
        }

        while self.values.len() > self.capacity {
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

    fn absent_error(&self) -> u64 {
        if self.values.len() < self.capacity {
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
            None if self.values.len() == 1 => self.min_index = Some(index),
            None => {}
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
    fn add_ref_for_test(&mut self, scalar: ScalarRef<'_>, count: u64, error: u64) {
        self.add_ref_with_options(scalar, count, error);
    }

    #[cfg(test)]
    fn add_entry_for_test(&mut self, entry: ColumnTopNEntry) {
        self.add_ref_with_options(entry.scalar.as_ref(), entry.count, entry.error);
    }
}

pub fn merge_column_top_n_mut(lhs: &mut BlockTopN, rhs: BlockTopN) -> Result<()> {
    for (column_id, column_top_n) in rhs {
        match lhs.entry(column_id) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().merge(column_top_n)?;
            }
            Entry::Vacant(entry) => {
                entry.insert(column_top_n.finish());
            }
        }
    }
    Ok(())
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct ColumnCountMinSketch {
    width: usize,
    depth: usize,
    counters: Vec<u64>,
}

impl Default for ColumnCountMinSketch {
    fn default() -> Self {
        Self::new(COUNT_MIN_SKETCH_WIDTH, COUNT_MIN_SKETCH_DEPTH)
    }
}

impl ColumnCountMinSketch {
    pub fn new(width: usize, depth: usize) -> Self {
        debug_assert!(width > 0);
        debug_assert!(depth > 0);
        Self {
            width,
            depth,
            counters: vec![0; width.saturating_mul(depth)],
        }
    }

    pub fn with_error_rate(error_rate: f64) -> Result<Self> {
        Ok(Self::new(
            Self::width_for_error_rate(error_rate)?,
            COUNT_MIN_SKETCH_DEPTH,
        ))
    }

    pub fn width_for_error_rate(error_rate: f64) -> Result<usize> {
        if error_rate <= 0.0 || !error_rate.is_finite() {
            return Err(ErrorCode::TableOptionInvalid(format!(
                "count-min sketch error rate must be finite and greater than zero, got: {error_rate}"
            )));
        }
        if error_rate > MAX_COUNT_MIN_SKETCH_ERROR_RATE {
            return Err(ErrorCode::TableOptionInvalid(format!(
                "count-min sketch error rate must be no greater than {MAX_COUNT_MIN_SKETCH_ERROR_RATE}, got: {error_rate}"
            )));
        }
        if error_rate < MIN_COUNT_MIN_SKETCH_ERROR_RATE {
            return Err(ErrorCode::TableOptionInvalid(format!(
                "count-min sketch error rate must be at least {MIN_COUNT_MIN_SKETCH_ERROR_RATE}, got: {error_rate}"
            )));
        }
        Ok((2.0 / error_rate).ceil() as usize)
    }

    pub fn add_with_count(&mut self, scalar: ScalarRef<'_>, count: u64) {
        if count == 0 || matches!(scalar, ScalarRef::Null) || self.width == 0 || self.depth == 0 {
            return;
        }

        let hashes = self.hash_scalar(&scalar);
        for row in 0..self.depth {
            let offset = self.offset_with_hashes(hashes, row);
            if let Some(counter) = self.counters.get_mut(offset) {
                *counter = counter.saturating_add(count);
            }
        }
    }

    pub fn estimate(&self, scalar: &Scalar) -> Option<u64> {
        let scalar = scalar.as_ref();
        if matches!(scalar, ScalarRef::Null) || self.width == 0 || self.depth == 0 {
            return None;
        }

        let hashes = self.hash_scalar(&scalar);
        (0..self.depth)
            .filter_map(|row| self.counters.get(self.offset_with_hashes(hashes, row)))
            .copied()
            .min()
    }

    pub fn is_empty(&self) -> bool {
        self.counters.iter().all(|counter| *counter == 0)
    }

    pub fn error_bound(&self, total_count: u64) -> u64 {
        if self.width == 0 {
            return total_count;
        }
        total_count
            .saturating_mul(2)
            .div_ceil(u64::try_from(self.width).unwrap_or(u64::MAX))
    }

    pub fn merge(&mut self, other: &Self) {
        if self.width != other.width
            || self.depth != other.depth
            || self.counters.len() != other.counters.len()
        {
            log::warn!(
                "Skip merging incompatible count-min sketches: lhs=({}, {}, {}), rhs=({}, {}, {})",
                self.width,
                self.depth,
                self.counters.len(),
                other.width,
                other.depth,
                other.counters.len()
            );
            return;
        }

        for (lhs, rhs) in self.counters.iter_mut().zip(other.counters.iter()) {
            *lhs = lhs.saturating_add(*rhs);
        }
    }

    fn hash_scalar(&self, value: &ScalarRef<'_>) -> (u64, u64) {
        (
            count_min_sketch_hash_v1(
                value,
                COUNT_MIN_SKETCH_HASH0_KEY0_V1,
                COUNT_MIN_SKETCH_HASH0_KEY1_V1,
            ),
            count_min_sketch_hash_v1(
                value,
                COUNT_MIN_SKETCH_HASH1_KEY0_V1,
                COUNT_MIN_SKETCH_HASH1_KEY1_V1,
            ),
        )
    }

    fn offset_with_hashes(&self, hashes: (u64, u64), row: usize) -> usize {
        let (hash0, hash1) = hashes;
        let hash = hash0.wrapping_add((row as u64).wrapping_mul(hash1 | 1));
        let bucket = if self.width.is_power_of_two() {
            hash as usize & (self.width - 1)
        } else {
            hash as usize % self.width
        };
        row.saturating_mul(self.width).saturating_add(bucket)
    }
}

fn count_min_sketch_hash_v1<Q: ?Sized + Hash>(value: &Q, key0: u64, key1: u64) -> u64 {
    let mut hasher = SipHasher24::new_with_keys(key0, key1);
    value.hash(&mut hasher);
    hasher.finish()
}

pub fn merge_column_count_min_sketch_mut(lhs: &mut BlockCountMinSketch, rhs: BlockCountMinSketch) {
    for (column_id, column_sketch) in rhs {
        match lhs.entry(column_id) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().merge(&column_sketch);
            }
            Entry::Vacant(entry) => {
                entry.insert(column_sketch);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::DecimalScalar;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::NumberScalar;

    use super::*;
    use crate::meta::ClusterStatistics;

    fn uint_scalar(value: u64) -> Scalar {
        Scalar::Number(NumberScalar::UInt64(value))
    }

    fn int32_scalar(value: i32) -> Scalar {
        Scalar::Number(NumberScalar::Int32(value))
    }

    #[test]
    fn cluster_stats_scalar_overlap_uses_lexicographic_ranges() {
        let stats = |min: &[i32], max: &[i32]| {
            ClusterStatistics::new(
                0,
                min.iter().copied().map(int32_scalar).collect(),
                max.iter().copied().map(int32_scalar).collect(),
                0,
            )
        };

        let left = stats(&[1, 100], &[2, 200]);
        assert!(cluster_stats_scalar_overlap(
            &left,
            &stats(&[2, 0], &[3, 50])
        ));
        assert!(!cluster_stats_scalar_overlap(
            &left,
            &stats(&[2, 201], &[3, 0])
        ));
    }

    #[test]
    fn explicit_cluster_type_controls_min_max_reduction() {
        let min0 = vec![int32_scalar(0), int32_scalar(100)];
        let max0 = vec![int32_scalar(5), int32_scalar(200)];
        let min1 = vec![int32_scalar(1), int32_scalar(0)];
        let max1 = vec![int32_scalar(6), int32_scalar(50)];

        let (linear_min, linear_max) =
            reduce_cluster_min_max(&[&min0, &min1], &[&max0, &max1], ClusterType::Linear).unwrap();
        assert_eq!(linear_min, min0);
        assert_eq!(linear_max, max1);

        let (hilbert_min, hilbert_max) =
            reduce_cluster_min_max(&[&min0, &min1], &[&max0, &max1], ClusterType::Hilbert).unwrap();
        assert_eq!(hilbert_min, vec![int32_scalar(0), int32_scalar(0)]);
        assert_eq!(hilbert_max, vec![int32_scalar(6), int32_scalar(200)]);

        let marker = Vec::new();
        assert!(
            reduce_cluster_min_max(&[&marker, &min0], &[&marker, &max0], ClusterType::Hilbert,)
                .is_none()
        );

        let reversed_min = vec![int32_scalar(5), int32_scalar(2)];
        let reversed_max = vec![int32_scalar(1), int32_scalar(4)];
        assert!(
            reduce_cluster_min_max(&[&reversed_min], &[&reversed_max], ClusterType::Hilbert)
                .is_none()
        );
    }

    #[test]
    fn valid_hilbert_stats_reject_malformed_ranges() {
        let stats = |dimensions: (i32, i32, i32, i32)| {
            ClusterStatistics::new(
                0,
                vec![int32_scalar(dimensions.0), int32_scalar(dimensions.2)],
                vec![int32_scalar(dimensions.1), int32_scalar(dimensions.3)],
                0,
            )
        };

        let valid = stats((0, 3, 4, 7));
        assert!(valid_cluster_stats_hilbert_minmax(&valid, 2).is_some());
        assert!(valid_cluster_stats_hilbert_minmax(&valid, 3).is_none());

        let reversed_dimension = stats((3, 0, 4, 7));
        assert!(valid_cluster_stats_hilbert_minmax(&reversed_dimension, 2).is_none());
    }

    #[test]
    fn column_top_n_ignores_incomparable_scalar_lookup() {
        let mut top_n = ColumnTopN::with_capacity(2);
        top_n.add_entry_for_test(ColumnTopNEntry {
            scalar: int32_scalar(5),
            count: 10,
            error: 0,
        });

        assert_eq!(top_n.get(&int32_scalar(5)), Some(10));
        assert_eq!(top_n.get(&uint_scalar(5)), None);
    }

    #[test]
    fn column_top_n_replaces_min_with_error() {
        let mut top_n = ColumnTopN::with_capacity(2);
        top_n.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(1),
            count: 5,
            error: 0,
        });
        top_n.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(2),
            count: 3,
            error: 0,
        });
        top_n.add_entry_for_test(ColumnTopNEntry {
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
        let mut lhs = ColumnTopN::with_capacity(2);
        lhs.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(1),
            count: 10,
            error: 0,
        });
        lhs.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(2),
            count: 5,
            error: 0,
        });
        let mut rhs = ColumnTopN::with_capacity(2);
        rhs.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(1),
            count: 7,
            error: 0,
        });
        rhs.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(3),
            count: 4,
            error: 0,
        });

        lhs.merge(rhs).unwrap();

        assert_eq!(lhs.get(&uint_scalar(1)), Some(17));
        let entry = lhs.get_entry(&uint_scalar(2)).unwrap();
        assert_eq!(entry.count, 9);
        assert_eq!(entry.error, 4);
    }

    #[test]
    fn column_top_n_merge_uses_rhs_capacity() {
        let mut lhs = ColumnTopN::with_capacity(1);
        lhs.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(1),
            count: 10,
            error: 0,
        });
        let mut rhs = ColumnTopN::with_capacity(2);
        rhs.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(2),
            count: 5,
            error: 0,
        });

        lhs.merge(rhs).unwrap();

        assert_eq!(lhs.capacity, 2);
        assert_eq!(lhs.values.len(), 2);
        assert_eq!(lhs.get(&uint_scalar(1)), Some(10));
        assert_eq!(lhs.get(&uint_scalar(2)), Some(15));

        let mut rhs = ColumnTopN::with_capacity(1);
        rhs.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(3),
            count: 20,
            error: 0,
        });

        lhs.merge(rhs).unwrap();

        assert_eq!(lhs.capacity, 1);
        assert_eq!(lhs.values.len(), 1);
    }

    #[test]
    fn column_top_n_merge_infers_legacy_capacity_before_missing_error() {
        let mut lhs = ColumnTopN {
            capacity: 0,
            values: vec![ColumnTopNEntry {
                scalar: uint_scalar(1),
                count: 10,
                error: 0,
            }],
            min_index: None,
        };
        let mut rhs = ColumnTopN::with_capacity(10);
        rhs.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(2),
            count: 5,
            error: 0,
        });

        lhs.merge(rhs).unwrap();

        assert_eq!(lhs.capacity, 10);
        assert_eq!(lhs.get(&uint_scalar(1)), Some(10));
        assert_eq!(lhs.get(&uint_scalar(2)), Some(5));
    }

    #[test]
    fn column_top_n_merge_defers_pruning_until_after_matching_entries() {
        let mut lhs = ColumnTopN::with_capacity(4);
        lhs.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(1),
            count: 100,
            error: 0,
        });
        lhs.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(2),
            count: 90,
            error: 0,
        });
        lhs.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(3),
            count: 95,
            error: 0,
        });
        lhs.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(4),
            count: 1,
            error: 0,
        });

        let mut rhs = ColumnTopN::with_capacity(2);
        rhs.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(2),
            count: 15,
            error: 0,
        });
        rhs.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(5),
            count: 1,
            error: 0,
        });

        lhs.merge(rhs).unwrap();

        assert_eq!(lhs.capacity, 2);
        assert_eq!(lhs.values.len(), 2);
        assert_eq!(lhs.get(&uint_scalar(1)), Some(101));
        assert_eq!(lhs.get(&uint_scalar(2)), Some(105));
        assert_eq!(lhs.get(&uint_scalar(3)), None);
    }

    #[test]
    fn column_top_n_adds_scalar_refs() {
        let mut top_n = ColumnTopN::with_capacity(2);
        top_n.add_ref_for_test(ScalarRef::String("b"), 1, 0);
        top_n.add_ref_for_test(ScalarRef::String("a"), 1, 0);
        top_n.add_ref_for_test(ScalarRef::String("a"), 4, 0);
        top_n.add_ref_for_test(ScalarRef::String("c"), 1, 0);

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

    #[test]
    fn column_top_n_rescans_min_after_cached_min_updates() {
        let mut top_n = ColumnTopN::with_capacity(4);
        top_n.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(1),
            count: 1,
            error: 0,
        });
        top_n.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(2),
            count: 5,
            error: 0,
        });
        top_n.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(1),
            count: 10,
            error: 0,
        });
        top_n.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(3),
            count: 7,
            error: 0,
        });
        top_n.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(4),
            count: 8,
            error: 0,
        });
        top_n.add_entry_for_test(ColumnTopNEntry {
            scalar: uint_scalar(5),
            count: 1,
            error: 0,
        });

        assert_eq!(top_n.values.len(), 4);
        assert_eq!(top_n.get(&uint_scalar(1)), Some(11));
        assert_eq!(top_n.get(&uint_scalar(2)), None);
        assert_eq!(top_n.get(&uint_scalar(3)), Some(7));
        let entry = top_n.get_entry(&uint_scalar(5)).unwrap();
        assert_eq!(entry.count, 6);
        assert_eq!(entry.error, 5);
    }

    #[test]
    fn count_min_sketch_estimates_frequency_and_merges() {
        let mut lhs = ColumnCountMinSketch::new(64, 4);
        lhs.add_with_count(uint_scalar(7).as_ref(), 10);
        lhs.add_with_count(uint_scalar(9).as_ref(), 2);

        let mut rhs = ColumnCountMinSketch::new(64, 4);
        rhs.add_with_count(uint_scalar(7).as_ref(), 5);
        rhs.add_with_count(ScalarRef::Null, 100);

        lhs.merge(&rhs);

        assert_eq!(lhs.estimate(&uint_scalar(7)), Some(15));
        assert!(
            lhs.estimate(&uint_scalar(9))
                .is_some_and(|count| count >= 2)
        );
        assert_eq!(lhs.estimate(&Scalar::Null), None);
        assert!(!lhs.is_empty());
        assert_eq!(lhs.error_bound(1_000), 32);
    }

    #[test]
    fn count_min_sketch_hash_v1_is_stable() {
        let sketch = ColumnCountMinSketch::new(64, 4);
        let scalar = uint_scalar(7);
        let scalar_ref = scalar.as_ref();

        assert_eq!(
            sketch.hash_scalar(&scalar_ref),
            (14425906419808812959, 13059174909041102891)
        );
        let hashes = sketch.hash_scalar(&scalar_ref);
        assert_eq!(
            (0..sketch.depth)
                .map(|row| sketch.offset_with_hashes(hashes, row))
                .collect::<Vec<_>>(),
            vec![31, 74, 181, 224]
        );
    }

    #[test]
    fn count_min_sketch_batched_update_does_not_underestimate() {
        let mut sketch = ColumnCountMinSketch::new(64, 4);
        let scalar = uint_scalar(7);
        let scalar_ref = scalar.as_ref();
        let hashes = sketch.hash_scalar(&scalar_ref);
        let offsets = (0..sketch.depth)
            .map(|row| sketch.offset_with_hashes(hashes, row))
            .collect::<Vec<_>>();

        for (idx, offset) in offsets.iter().enumerate() {
            sketch.counters[*offset] = if idx == 1 { 101 } else { 100 };
        }

        sketch.add_with_count(scalar_ref, 10);

        assert_eq!(sketch.estimate(&scalar), Some(110));
    }

    fn decimal_scalar(value: i64, precision: u8, scale: u8) -> Scalar {
        Scalar::Decimal(DecimalScalar::Decimal64(
            value,
            DecimalSize::new(precision, scale).unwrap(),
        ))
    }

    // A metadata-only decimal precision widening leaves one side tagged with the previous
    // `DecimalSize`. Raw comparison collapses that to `Equal`, which would report every pair of
    // ranges as overlapping and stop recluster from making progress.
    #[test]
    fn cluster_stats_scalar_overlap_across_widened_precision() {
        let stats = |min: Scalar, max: Scalar| ClusterStatistics::new(0, vec![min], vec![max], 0);

        let stale = stats(decimal_scalar(100, 10, 2), decimal_scalar(200, 10, 2));
        let current = stats(decimal_scalar(700, 15, 2), decimal_scalar(800, 15, 2));
        assert!(!cluster_stats_scalar_overlap(&stale, &current));

        let wide = stats(decimal_scalar(100, 10, 2), decimal_scalar(900, 10, 2));
        assert!(cluster_stats_scalar_overlap(&wide, &current));

        // A scale change cannot be reconciled, so the check must stay conservative.
        let other_scale = stats(decimal_scalar(100, 10, 4), decimal_scalar(200, 10, 4));
        assert!(cluster_stats_scalar_overlap(&other_scale, &current));
    }

    #[test]
    fn reduce_cluster_min_max_across_widened_precision() {
        let stale_min = vec![decimal_scalar(900, 10, 2)];
        let stale_max = vec![decimal_scalar(950, 10, 2)];
        let current_min = vec![decimal_scalar(100, 15, 2)];
        let current_max = vec![decimal_scalar(200, 15, 2)];

        let (min, max) = reduce_cluster_min_max(
            &[stale_min.as_slice(), current_min.as_slice()],
            &[stale_max.as_slice(), current_max.as_slice()],
            ClusterType::Linear,
        )
        .expect("bounds are comparable");
        assert_eq!(min, current_min);
        assert_eq!(max, stale_max);

        // Mixed scales have no sound reduction; bail out rather than take each end from a
        // different input.
        let other_scale_min = vec![decimal_scalar(100, 10, 4)];
        let other_scale_max = vec![decimal_scalar(200, 10, 4)];
        assert!(
            reduce_cluster_min_max(
                &[stale_min.as_slice(), other_scale_min.as_slice()],
                &[stale_max.as_slice(), other_scale_max.as_slice()],
                ClusterType::Linear,
            )
            .is_none()
        );
    }

    // `ColumnTopN` keeps its values sorted and looks them up by binary search, which needs a
    // consistent order. A stale precision must not make an existing value unfindable or corrupt
    // the ordering.
    #[test]
    fn column_top_n_finds_values_across_widened_precision() {
        let mut top_n = ColumnTopN::with_capacity(8);
        top_n.add(decimal_scalar(100, 10, 2).as_ref(), 3);
        top_n.add(decimal_scalar(300, 10, 2).as_ref(), 5);

        // The same value tagged with the widened precision must hit the existing entry rather
        // than insert a duplicate.
        top_n.add(decimal_scalar(100, 15, 2).as_ref(), 4);
        assert_eq!(top_n.values.len(), 2);
        assert_eq!(
            top_n
                .get_entry(&decimal_scalar(100, 15, 2))
                .map(|e| e.count),
            Some(7)
        );

        // Values stay sorted by raw value regardless of the precision they carry.
        let scalars = top_n
            .values
            .iter()
            .map(|entry| entry.scalar.clone())
            .collect::<Vec<_>>();
        assert_eq!(scalars, vec![
            decimal_scalar(100, 10, 2),
            decimal_scalar(300, 10, 2)
        ]);
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
