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
            .is_some_and(|stat| stat.null_count == 0 && filter.should_prune(stat.min(), stat.max()))
    })
}

/// The scheduling rank of a block for runtime TopN reads: its best possible
/// sort value (min for ASC, max for DESC), or `None` when unknown.
pub(crate) fn runtime_top_n_order_key(
    columns_stat: Option<&HashMap<ColumnId, ColumnStatistics>>,
    filter: &RuntimeTopNFilter,
) -> Option<Scalar> {
    let stat = columns_stat?.get(&filter.column_id())?;
    let key = if filter.asc() { stat.min() } else { stat.max() };
    if matches!(key, Scalar::Null) {
        return None;
    }
    Some(key.clone())
}

/// Reorder items so the blocks most likely to contain top rows are read
/// first: the shared TopN boundary then converges early and the remaining
/// blocks are pruned by `should_prune_by_runtime_top_n`. Items with unknown
/// statistics keep their relative order at the end.
pub(crate) fn sort_by_runtime_top_n_order<T>(
    items: &mut Vec<T>,
    filter: &RuntimeTopNFilter,
    key: impl Fn(&T) -> Option<Scalar>,
) {
    let asc = filter.asc();
    let mut decorated: Vec<(Option<Scalar>, T)> = std::mem::take(items)
        .into_iter()
        .map(|item| (key(&item), item))
        .collect();
    decorated.sort_by(|(a, _), (b, _)| match (a, b) {
        (Some(a), Some(b)) => {
            if asc {
                a.cmp(b)
            } else {
                b.cmp(a)
            }
        }
        (Some(_), None) => CmpOrdering::Less,
        (None, Some(_)) => CmpOrdering::Greater,
        (None, None) => CmpOrdering::Equal,
    });
    items.extend(decorated.into_iter().map(|(_, item)| item));
}

/// Node-local ordering of fuse parts for runtime TopN scheduling.
pub(crate) fn sort_parts_by_runtime_top_n(
    parts: &mut Vec<PartInfoPtr>,
    filters: &[Arc<RuntimeTopNFilter>],
) {
    let Some(filter) = filters.first() else {
        return;
    };
    sort_by_runtime_top_n_order(parts, filter, |part| {
        FuseBlockPartInfo::from_part(part)
            .ok()
            .and_then(|info| runtime_top_n_order_key(info.columns_stat.as_ref(), filter))
    });
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
    fn test_runtime_top_n_prunes_only_strictly_worse_non_null_blocks() {
        let asc = Arc::new(RuntimeTopNFilter::new(3, true));
        asc.update(&int64(10));

        let mut columns = HashMap::from([(3, stats(11, 20, 0))]);
        assert!(should_prune_by_runtime_top_n(
            Some(&columns),
            std::slice::from_ref(&asc)
        ));

        // Boundary ties and nullable statistics must be retained.
        columns.insert(3, stats(10, 20, 0));
        assert!(!should_prune_by_runtime_top_n(
            Some(&columns),
            std::slice::from_ref(&asc)
        ));
        columns.insert(3, stats(11, 20, 1));
        assert!(!should_prune_by_runtime_top_n(
            Some(&columns),
            std::slice::from_ref(&asc)
        ));
        assert!(!should_prune_by_runtime_top_n(None, &[asc]));

        let desc = Arc::new(RuntimeTopNFilter::new(3, false));
        desc.update(&int64(10));
        columns.insert(3, stats(1, 9, 0));
        assert!(should_prune_by_runtime_top_n(Some(&columns), &[desc]));
    }

    fn part_with_stats(location: &str, min_max: Option<(i64, i64)>) -> PartInfoPtr {
        FuseBlockPartInfo::create(
            location.to_string(),
            None,
            0,
            None,
            1,
            HashMap::new(),
            min_max.map(|(min, max)| HashMap::from([(3, stats(min, max, 0))])),
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
    fn test_sort_parts_by_runtime_top_n_schedules_promising_blocks_first() {
        let mut parts = vec![
            part_with_stats("mid", Some((4, 40))),
            part_with_stats("no_stats", None),
            part_with_stats("high", Some((7, 70))),
            part_with_stats("low", Some((1, 10))),
        ];

        // Without a filter the order is untouched.
        sort_parts_by_runtime_top_n(&mut parts, &[]);
        assert_eq!(part_locations(&parts), vec![
            "mid", "no_stats", "high", "low"
        ]);

        // ASC reads the smallest mins first; unknown statistics go last.
        let asc = Arc::new(RuntimeTopNFilter::new(3, true));
        sort_parts_by_runtime_top_n(&mut parts, std::slice::from_ref(&asc));
        assert_eq!(part_locations(&parts), vec![
            "low", "mid", "high", "no_stats"
        ]);

        // DESC reads the largest maxes first.
        let desc = Arc::new(RuntimeTopNFilter::new(3, false));
        sort_parts_by_runtime_top_n(&mut parts, std::slice::from_ref(&desc));
        assert_eq!(part_locations(&parts), vec![
            "high", "mid", "low", "no_stats"
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
