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

use std::any::Any;
use std::borrow::Borrow;
use std::cmp::Ordering as CmpOrdering;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Range;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::runtime_filter_info::RuntimeTopNFilter;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::GranuleIndexLayout;
use databend_storages_common_table_meta::meta::Location;

/// Fuse table partition information.
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct FuseBlockPartInfo {
    pub location: String,

    pub bloom_filter_index_location: Option<Location>,
    pub bloom_filter_index_size: u64,
    #[serde(default)]
    pub granule_index: Option<GranuleIndexLayout>,

    pub create_on: Option<DateTime<Utc>>,
    pub nums_rows: usize,
    pub columns_meta: HashMap<ColumnId, ColumnMeta>,
    pub columns_stat: Option<HashMap<ColumnId, ColumnStatistics>>,
    pub compression: Compression,

    pub sort_min_max: Option<(Scalar, Scalar)>,
    pub block_meta_index: Option<BlockMetaIndex>,
}

#[typetag::serde(name = "fuse")]
impl PartInfo for FuseBlockPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<FuseBlockPartInfo>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.location.hash(&mut s);
        s.finish()
    }

    fn part_type(&self) -> PartInfoType {
        PartInfoType::BlockLevel
    }
}

impl FuseBlockPartInfo {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        location: String,
        bloom_filter_index_location: Option<Location>,
        bloom_filter_index_size: u64,
        granule_index: Option<GranuleIndexLayout>,
        rows_count: u64,
        columns_meta: HashMap<ColumnId, ColumnMeta>,
        columns_stat: Option<HashMap<ColumnId, ColumnStatistics>>,
        compression: Compression,
        sort_min_max: Option<(Scalar, Scalar)>,
        block_meta_index: Option<BlockMetaIndex>,
        create_on: Option<DateTime<Utc>>,
    ) -> Arc<Box<dyn PartInfo>> {
        Arc::new(Box::new(FuseBlockPartInfo {
            location,
            bloom_filter_index_location,
            bloom_filter_index_size,
            granule_index,
            create_on,
            columns_meta,
            nums_rows: rows_count as usize,
            compression,
            sort_min_max,
            block_meta_index,
            columns_stat,
        }))
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&FuseBlockPartInfo> {
        info.as_any()
            .downcast_ref::<FuseBlockPartInfo>()
            .ok_or_else(|| {
                ErrorCode::Internal("Cannot downcast from PartInfo to FuseBlockPartInfo.")
            })
    }

    pub fn range(&self) -> Option<&Range<usize>> {
        self.block_meta_index
            .as_ref()
            .and_then(|meta| meta.range.as_ref())
    }

    pub fn block_meta_index(&self) -> Option<&BlockMetaIndex> {
        self.block_meta_index.as_ref()
    }

    pub fn should_prune_by_runtime_top_n(&self, filters: &[Arc<RuntimeTopNFilter>]) -> bool {
        should_prune_by_runtime_top_n(self.columns_stat.as_ref(), filters)
    }
}

pub(crate) fn should_prune_by_runtime_top_n(
    columns_stat: Option<&HashMap<ColumnId, ColumnStatistics>>,
    filters: &[Arc<RuntimeTopNFilter>],
) -> bool {
    let Some(columns_stat) = columns_stat else {
        return false;
    };

    filters.iter().any(|filter| {
        columns_stat
            .get(&filter.column_id())
            .is_some_and(|stat| filter.should_prune(stat.min(), stat.max(), stat.null_count))
    })
}

/// The scheduling rank of a block for runtime TopN reads: `Best` holds rows
/// sorting before every value (nulls under NULLS FIRST — never prunable, read
/// first), `Value` ranks by the best possible sort value (min for ASC, max
/// for DESC), `Unknown` (no usable statistics) schedules last.
pub(crate) enum RuntimeTopNRank<S> {
    Best,
    Value(S),
    Unknown,
}

impl RuntimeTopNRank<&Scalar> {
    pub(crate) fn cloned(self) -> RuntimeTopNRank<Scalar> {
        match self {
            RuntimeTopNRank::Best => RuntimeTopNRank::Best,
            RuntimeTopNRank::Value(value) => RuntimeTopNRank::Value(value.clone()),
            RuntimeTopNRank::Unknown => RuntimeTopNRank::Unknown,
        }
    }
}

pub(crate) fn runtime_top_n_rank<'a>(
    columns_stat: Option<&'a HashMap<ColumnId, ColumnStatistics>>,
    filter: &RuntimeTopNFilter,
) -> RuntimeTopNRank<&'a Scalar> {
    runtime_top_n_rank_from_stat(
        columns_stat.and_then(|stats| stats.get(&filter.column_id())),
        filter,
    )
}

pub(crate) fn runtime_top_n_rank_from_stat<'a>(
    stat: Option<&'a ColumnStatistics>,
    filter: &RuntimeTopNFilter,
) -> RuntimeTopNRank<&'a Scalar> {
    let Some(stat) = stat else {
        return RuntimeTopNRank::Unknown;
    };
    // Under NULLS FIRST null rows sort before every value: blocks holding
    // nulls are the most promising ones (and are never prunable).
    if filter.nulls_first() && stat.null_count > 0 {
        return RuntimeTopNRank::Best;
    }
    let key = if filter.asc() { stat.min() } else { stat.max() };
    if matches!(key, Scalar::Null) {
        return RuntimeTopNRank::Unknown;
    }
    RuntimeTopNRank::Value(key)
}

/// Compare two scheduling ranks: better-ranked blocks (more likely to hold
/// top rows) first.
pub(crate) fn compare_runtime_top_n_ranks<S: Borrow<Scalar>>(
    a: &RuntimeTopNRank<S>,
    b: &RuntimeTopNRank<S>,
    asc: bool,
) -> CmpOrdering {
    match (a, b) {
        (RuntimeTopNRank::Best, RuntimeTopNRank::Best) => CmpOrdering::Equal,
        (RuntimeTopNRank::Best, _) => CmpOrdering::Less,
        (_, RuntimeTopNRank::Best) => CmpOrdering::Greater,
        (RuntimeTopNRank::Value(a), RuntimeTopNRank::Value(b)) => {
            if asc {
                a.borrow().cmp(b.borrow())
            } else {
                b.borrow().cmp(a.borrow())
            }
        }
        (RuntimeTopNRank::Value(_), RuntimeTopNRank::Unknown) => CmpOrdering::Less,
        (RuntimeTopNRank::Unknown, RuntimeTopNRank::Value(_)) => CmpOrdering::Greater,
        (RuntimeTopNRank::Unknown, RuntimeTopNRank::Unknown) => CmpOrdering::Equal,
    }
}

/// Reorder items so the blocks most likely to contain top rows are read
/// first. Stable: items with equal or unknown ranks keep their relative
/// order. Meant for bounded batches (e.g. one segment's surviving blocks);
/// unbounded part lists should use [`front_load_parts_for_runtime_top_n`].
pub(crate) fn sort_by_runtime_top_n_order<T>(
    items: &mut Vec<T>,
    filter: &RuntimeTopNFilter,
    key: impl Fn(&T) -> RuntimeTopNRank<Scalar>,
) {
    let asc = filter.asc();
    let mut decorated: Vec<(RuntimeTopNRank<Scalar>, T)> = std::mem::take(items)
        .into_iter()
        .map(|item| (key(&item), item))
        .collect();
    decorated.sort_by(|(a, _), (b, _)| compare_runtime_top_n_ranks(a, b, asc));
    items.extend(decorated.into_iter().map(|(_, item)| item));
}

fn runtime_top_n_part_rank<'a>(
    part: &'a PartInfoPtr,
    filter: &RuntimeTopNFilter,
) -> RuntimeTopNRank<&'a Scalar> {
    let columns_stat = FuseBlockPartInfo::from_part(part)
        .ok()
        .and_then(|info| info.columns_stat.as_ref());
    runtime_top_n_rank(columns_stat, filter)
}

/// Move the `head` most promising parts to the front (sorted) so they are
/// read first. The tail is left unordered on purpose: once the head blocks
/// tighten the shared boundary, tail blocks are pruned at read time anyway,
/// so an O(n log n) sort over a potentially huge part list is avoided.
pub(crate) fn front_load_parts_for_runtime_top_n(
    parts: &mut [PartInfoPtr],
    filters: &[Arc<RuntimeTopNFilter>],
    head: usize,
) {
    let Some(filter) = filters.first() else {
        return;
    };
    let head = head.max(1);
    let compare = |a: &PartInfoPtr, b: &PartInfoPtr| {
        compare_runtime_top_n_ranks(
            &runtime_top_n_part_rank(a, filter),
            &runtime_top_n_part_rank(b, filter),
            filter.asc(),
        )
    };

    if parts.len() > head {
        parts.select_nth_unstable_by(head - 1, compare);
        parts[..head].sort_unstable_by(compare);
    } else {
        parts.sort_unstable_by(compare);
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::NumberScalar;

    use super::*;

    fn int64(value: i64) -> Scalar {
        Scalar::Number(NumberScalar::Int64(value))
    }

    fn stats(min: i64, max: i64, null_count: u64) -> ColumnStatistics {
        ColumnStatistics::new(int64(min), int64(max), null_count, 0, None)
    }

    #[test]
    fn test_runtime_top_n_prunes_only_strictly_worse_blocks() {
        let asc = Arc::new(RuntimeTopNFilter::new(3, true, false));
        asc.update(&int64(10));

        let mut columns = HashMap::from([(3, stats(11, 20, 0))]);
        assert!(should_prune_by_runtime_top_n(
            Some(&columns),
            std::slice::from_ref(&asc)
        ));

        // Boundary ties must be retained.
        columns.insert(3, stats(10, 20, 0));
        assert!(!should_prune_by_runtime_top_n(
            Some(&columns),
            std::slice::from_ref(&asc)
        ));
        // Under NULLS LAST the null rows of a strictly worse block are worse too.
        columns.insert(3, stats(11, 20, 1));
        assert!(should_prune_by_runtime_top_n(
            Some(&columns),
            std::slice::from_ref(&asc)
        ));
        assert!(!should_prune_by_runtime_top_n(
            None,
            std::slice::from_ref(&asc)
        ));

        // Under NULLS FIRST null rows are always candidates.
        let nulls_first = Arc::new(RuntimeTopNFilter::new(3, true, true));
        nulls_first.update(&int64(10));
        assert!(!should_prune_by_runtime_top_n(
            Some(&columns),
            std::slice::from_ref(&nulls_first)
        ));

        let desc = Arc::new(RuntimeTopNFilter::new(3, false, false));
        desc.update(&int64(10));
        columns.insert(3, stats(1, 9, 0));
        assert!(should_prune_by_runtime_top_n(Some(&columns), &[desc]));
    }

    fn part_with_stats(location: &str, min_max: Option<(i64, i64)>) -> PartInfoPtr {
        part_with_nullable_stats(location, min_max.map(|(min, max)| (min, max, 0)))
    }

    fn part_with_nullable_stats(
        location: &str,
        min_max_nulls: Option<(i64, i64, u64)>,
    ) -> PartInfoPtr {
        FuseBlockPartInfo::create(
            location.to_string(),
            None,
            0,
            None,
            1,
            HashMap::new(),
            min_max_nulls.map(|(min, max, nulls)| HashMap::from([(3, stats(min, max, nulls))])),
            Compression::Lz4Raw,
            None,
            None,
            None,
        )
    }

    fn part_locations(parts: &[PartInfoPtr]) -> Vec<&str> {
        parts
            .iter()
            .map(|part| {
                FuseBlockPartInfo::from_part(part)
                    .unwrap()
                    .location
                    .as_str()
            })
            .collect()
    }

    #[test]
    fn test_front_load_parts_schedules_promising_blocks_first() {
        let mut parts = vec![
            part_with_stats("mid", Some((4, 40))),
            part_with_stats("no_stats", None),
            part_with_stats("high", Some((7, 70))),
            part_with_stats("low", Some((1, 10))),
        ];

        // Without a filter the order is untouched.
        front_load_parts_for_runtime_top_n(&mut parts, &[], 1024);
        assert_eq!(part_locations(&parts), vec![
            "mid", "no_stats", "high", "low"
        ]);

        // ASC reads the smallest mins first; unknown statistics go last.
        let asc = Arc::new(RuntimeTopNFilter::new(3, true, false));
        front_load_parts_for_runtime_top_n(&mut parts, std::slice::from_ref(&asc), 1024);
        assert_eq!(part_locations(&parts), vec![
            "low", "mid", "high", "no_stats"
        ]);

        // DESC reads the largest maxes first.
        let desc = Arc::new(RuntimeTopNFilter::new(3, false, false));
        front_load_parts_for_runtime_top_n(&mut parts, std::slice::from_ref(&desc), 1024);
        assert_eq!(part_locations(&parts), vec![
            "high", "mid", "low", "no_stats"
        ]);

        // A bounded head only orders the front; the tail keeps all remaining
        // parts in some order.
        front_load_parts_for_runtime_top_n(&mut parts, std::slice::from_ref(&asc), 2);
        assert_eq!(part_locations(&parts)[..2], ["low", "mid"]);
        let mut tail = part_locations(&parts)[2..].to_vec();
        tail.sort_unstable();
        assert_eq!(tail, vec!["high", "no_stats"]);
    }

    #[test]
    fn test_front_load_ranks_null_bearing_blocks_best_under_nulls_first() {
        let nulls_first = Arc::new(RuntimeTopNFilter::new(3, true, true));
        let mut parts = vec![
            part_with_stats("low", Some((1, 10))),
            part_with_nullable_stats("with_nulls", Some((50, 60, 2))),
            part_with_stats("no_stats", None),
        ];

        // Null-bearing blocks hold the globally best rows under NULLS FIRST
        // and can never be pruned: schedule them before any value rank.
        front_load_parts_for_runtime_top_n(&mut parts, std::slice::from_ref(&nulls_first), 1024);
        assert_eq!(part_locations(&parts), vec![
            "with_nulls",
            "low",
            "no_stats"
        ]);

        // NULLS LAST keeps the plain value ranking.
        let nulls_last = Arc::new(RuntimeTopNFilter::new(3, true, false));
        front_load_parts_for_runtime_top_n(&mut parts, std::slice::from_ref(&nulls_last), 1024);
        assert_eq!(part_locations(&parts), vec![
            "low",
            "with_nulls",
            "no_stats"
        ]);
    }
}

/// Fuse table lazy partition information.
/// Lazy partition is a partition that only contains the partition location.
/// The partition data will be loaded when the partition is used.
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct FuseLazyPartInfo {
    pub segment_index: usize,
    pub segment_location: Location,
}

#[typetag::serde(name = "fuse_lazy")]
impl PartInfo for FuseLazyPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<FuseLazyPartInfo>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.segment_location.0.hash(&mut s);
        s.finish()
    }

    fn part_type(&self) -> PartInfoType {
        PartInfoType::LazyLevel
    }
}

impl FuseLazyPartInfo {
    pub fn create(idx: usize, segment_location: Location) -> PartInfoPtr {
        Arc::new(Box::new(FuseLazyPartInfo {
            segment_index: idx,
            segment_location,
        }))
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&FuseLazyPartInfo> {
        info.as_any()
            .downcast_ref::<FuseLazyPartInfo>()
            .ok_or_else(|| {
                ErrorCode::Internal("Cannot downcast from PartInfo to FuseLazyPartInfo.")
            })
    }
}
