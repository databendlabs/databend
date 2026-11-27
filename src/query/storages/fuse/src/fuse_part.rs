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
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Range;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_catalog::plan::InternalColumnType;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::compute_row_id;
use databend_common_catalog::plan::compute_row_id_prefix;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::types::F32;
use databend_storages_common_cache::size::btree_map;
use databend_storages_common_cache::size::column_statistics;
use databend_storages_common_cache::size::hash_map;
use databend_storages_common_cache::size::location;
use databend_storages_common_cache::size::none;
use databend_storages_common_cache::size::option;
use databend_storages_common_cache::size::scalar;
use databend_storages_common_cache::size::vec;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_pruner::IndexMatches;
use databend_storages_common_table_meta::meta::BlockDataFile;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnGroupFileMeta;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::GranuleIndexLayout;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::Versioned;

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

    /// The group files of a split block, holding the chunks this part reads. Empty for a
    /// legacy block, whose chunks are all in the file at `location`. For a split block,
    /// `columns_meta` holds the chunks of all groups, and `location` only identifies the block.
    #[serde(default)]
    pub column_groups: Vec<PartColumnGroup>,
}

/// A group file of a split block, with the chunks a part reads from it.
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
pub struct PartColumnGroup {
    pub location: String,
    pub columns_meta: HashMap<ColumnId, ColumnMeta>,
    pub compression: Compression,
}

impl PartColumnGroup {
    /// The files to read: the legacy file at `location`, or the `groups` of a split block.
    pub fn data_files<'a>(
        location: &'a str,
        columns_meta: &'a HashMap<ColumnId, ColumnMeta>,
        compression: Compression,
        groups: &'a [PartColumnGroup],
    ) -> Vec<BlockDataFile<'a>> {
        if groups.is_empty() {
            return vec![BlockDataFile {
                location,
                col_metas: columns_meta,
                compression,
            }];
        }
        groups
            .iter()
            .map(|group| BlockDataFile {
                location: &group.location,
                col_metas: &group.columns_meta,
                compression: group.compression,
            })
            .collect()
    }

    /// The group as block metadata a reader can read the group from. The file size and the
    /// Bloom index are not known from a part.
    pub fn to_file_meta(&self) -> ColumnGroupFileMeta {
        ColumnGroupFileMeta {
            location: (self.location.clone(), DataBlock::VERSION),
            file_size: 0,
            leaf_column_metas: self.columns_meta.clone(),
            bloom: None,
            compression: self.compression,
        }
    }

    /// The groups of `meta` that store a column `keep` accepts, with only those columns.
    pub fn from_block(meta: &BlockMeta, keep: impl Fn(&ColumnId) -> bool) -> Vec<Self> {
        meta.column_groups
            .iter()
            .filter_map(|group| {
                let columns_meta: HashMap<_, _> = group
                    .leaf_column_metas
                    .iter()
                    .filter(|(id, _)| keep(id))
                    .map(|(id, meta)| (*id, meta.clone()))
                    .collect();
                (!columns_meta.is_empty()).then(|| PartColumnGroup {
                    location: group.location.0.clone(),
                    columns_meta,
                    compression: group.compression,
                })
            })
            .collect()
    }
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

    /// Column metas own no heap memory, as in `BlockMeta`'s estimate.
    fn mem_bytes(&self) -> usize {
        size_of::<Self>()
            + self.location.capacity()
            + option(&self.bloom_filter_index_location, location)
            + hash_map(&self.columns_meta, |_, _| 0)
            + option(&self.columns_stat, |stats| {
                hash_map(stats, |_, s| column_statistics(s))
            })
            + option(&self.sort_min_max, |(min, max)| scalar(min) + scalar(max))
            + option(&self.block_meta_index, block_meta_index)
            + vec(&self.column_groups, |g| {
                g.location.capacity() + hash_map(&g.columns_meta, |_, _| 0)
            })
    }
}

fn block_meta_index(b: &BlockMetaIndex) -> usize {
    b.block_location.capacity()
        + b.segment_location.capacity()
        + option(&b.snapshot_location, |s| s.capacity())
        + option(&b.inverted_index_matches, index_matches)
        + option(&b.vector_index_matches, index_matches)
        + option(&b.virtual_block_meta, |v| {
            v.virtual_block_location.capacity()
                + btree_map(&v.fields, |_, _| 0)
                + vec(&v.read_slots, none)
                + hash_map(&v.virtual_column_stats, |_, s| column_statistics(s))
        })
}

fn index_matches(m: &IndexMatches) -> usize {
    vec(&m.rows, none) + option(&m.scores, |v| vec(v, none))
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
        column_groups: Vec<PartColumnGroup>,
    ) -> Arc<Box<dyn PartInfo>> {
        Arc::new(Box::new(FuseBlockPartInfo {
            column_groups,
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

    /// Covering index entries for this partition, only if they describe *every* row
    /// the scan would emit. A vector index can attach scores without restricting the
    /// scan to those rows (the other rows then have a placeholder score).
    pub(crate) fn covering_matches(&self, key: &InternalColumnType) -> Option<&IndexMatches> {
        let meta = self.block_meta_index.as_ref()?;
        if meta.range.is_some() {
            return None;
        }
        let entries = match (
            key,
            &meta.inverted_index_matches,
            &meta.vector_index_matches,
        ) {
            (InternalColumnType::SearchScore, Some(entries), None)
            | (InternalColumnType::VectorScore, None, Some(entries)) => entries,
            _ => return None,
        };
        let scores = entries.scores.as_ref()?;
        if !entries.rows_only
            || entries.rows.len() != scores.len()
            || entries
                .rows
                .iter()
                .any(|&row| row as usize >= self.nums_rows)
        {
            return None;
        }
        Some(entries)
    }

    pub fn scored_rows(
        &self,
        key: &InternalColumnType,
    ) -> Option<impl ExactSizeIterator<Item = (F32, u64)> + '_> {
        let entries = self.covering_matches(key)?;
        let scores = entries.scores.as_ref()?;
        let meta = self.block_meta_index.as_ref()?;
        let prefix = compute_row_id_prefix(meta.segment_idx as u64, meta.block_id as u64);
        Some(
            scores
                .iter()
                .zip(&entries.rows)
                .map(move |(&score, &row)| (score, compute_row_id(prefix, u64::from(row)))),
        )
    }

    pub fn range(&self) -> Option<&Range<usize>> {
        self.block_meta_index
            .as_ref()
            .and_then(|meta| meta.range.as_ref())
    }

    pub fn block_meta_index(&self) -> Option<&BlockMetaIndex> {
        self.block_meta_index.as_ref()
    }

    /// The files this part reads its column chunks from.
    pub fn data_files(&self) -> Vec<BlockDataFile<'_>> {
        PartColumnGroup::data_files(
            &self.location,
            &self.columns_meta,
            self.compression,
            &self.column_groups,
        )
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

    fn mem_bytes(&self) -> usize {
        size_of::<Self>() + location(&self.segment_location)
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

#[cfg(test)]
mod tests {
    use databend_common_catalog::plan::PartStatistics;
    use databend_common_catalog::plan::Partitions;
    use databend_common_catalog::plan::PartitionsShuffleKind;
    use databend_storages_common_cache::CacheKind;
    use databend_storages_common_cache::CachedItem;
    use databend_storages_common_cache::MemCache;
    use databend_storages_common_cache::PoolCaps;
    use databend_storages_common_cache::backend::KindStore;
    use databend_storages_common_cache::kinds::table;
    use databend_storages_common_cache::shared::SharedStore;
    use databend_storages_common_table_meta::meta::SingleColumnMeta;

    use super::*;

    fn part(columns: u32) -> PartInfoPtr {
        let meta = SingleColumnMeta {
            offset: 0,
            len: 0,
            num_values: 0,
        };
        let stat = || {
            let (min, max) = ("a".repeat(100), "b".repeat(100));
            ColumnStatistics::new(Scalar::String(min), Scalar::String(max), 0, 0, None)
        };
        FuseBlockPartInfo::create(
            "1/2/_b/block.parquet".to_string(),
            None,
            0,
            None,
            0,
            (0..columns)
                .map(|id| (id, ColumnMeta::Parquet(meta.clone())))
                .collect(),
            Some((0..columns).map(|id| (id, stat())).collect()),
            Compression::Lz4Raw,
            None,
            None,
            None,
            vec![],
        )
    }

    /// Columns make most of a part: their metas and statistics, with the strings of the
    /// statistics, are counted.
    #[test]
    fn test_part_mem_bytes_counts_columns() {
        let per_column =
            size_of::<(ColumnId, ColumnMeta)>() + size_of::<(ColumnId, ColumnStatistics)>() + 200;
        let (one, many) = (part(1).mem_bytes(), part(101).mem_bytes());
        assert!(many - one >= 100 * per_column, "{one} {many}");
        assert!(many - one <= 300 * per_column, "{one} {many}");
    }

    #[test]
    fn test_nonempty_prune_result_is_charged_until_last_reader() {
        let parts = (0..4).map(|_| part(101)).collect::<Vec<_>>();
        let part_bytes: usize = parts.iter().map(|p| p.mem_bytes()).sum();
        let value = (
            PartStatistics::default(),
            Partitions::create(PartitionsShuffleKind::Seq, parts),
        );
        let bytes = value.mem_bytes();
        assert!(bytes >= part_bytes);
        assert_eq!(value.legacy_bytes(), 0);
        let store = Arc::new(SharedStore::new(
            PoolCaps {
                meta: bytes * 4,
                index: 0,
                data: 0,
            },
            [],
            1,
        ));
        let kind = CacheKind::of::<table::PruneResult>();
        let cache = MemCache::new(store.clone(), kind);
        let held = cache.put("prune", value);
        assert!(held.is_charged());
        assert_eq!(store.kind_stats(kind).resident, bytes);
        assert!(cache.evict("prune"));
        let stats = store.kind_stats(kind);
        assert_eq!((stats.in_cache, stats.resident), (0, bytes));
        drop(held);
        assert_eq!(store.kind_stats(kind).resident, 0);
    }

    fn scored_part(
        rows: u64,
        fts: Option<IndexMatches>,
        vector: Option<IndexMatches>,
        range: Option<Range<usize>>,
    ) -> PartInfoPtr {
        FuseBlockPartInfo::create(
            "1/2/_b/block.parquet".to_string(),
            None,
            0,
            None,
            rows,
            HashMap::new(),
            None,
            Compression::Lz4Raw,
            None,
            Some(BlockMetaIndex {
                segment_idx: 3,
                block_id: 5,
                range,
                inverted_index_matches: fts,
                vector_index_matches: vector,
                ..Default::default()
            }),
            None,
            vec![],
        )
    }

    fn matches(rows: &[u32], scores: Option<&[f32]>, rows_only: bool) -> IndexMatches {
        IndexMatches {
            rows: rows.to_vec(),
            scores: scores.map(|s| s.iter().map(|&v| F32::from(v)).collect()),
            rows_only,
        }
    }

    /// Index matches replace the scan of a block only when they are all the rows it
    /// outputs, each with its score.
    #[test]
    fn test_covering_matches_require_every_output_row_scored() {
        let fts = InternalColumnType::SearchScore;
        let vector = InternalColumnType::VectorScore;
        let covered = |part: PartInfoPtr, key: &InternalColumnType| {
            FuseBlockPartInfo::from_part(&part)
                .unwrap()
                .covering_matches(key)
                .is_some()
        };
        let hits = || Some(matches(&[7, 2], Some(&[0.5, 0.9]), true));

        assert!(covered(scored_part(10, hits(), None, None), &fts));
        assert!(covered(scored_part(10, None, hits(), None), &vector));
        // The other index, or both indexes on one block.
        assert!(!covered(scored_part(10, hits(), None, None), &vector));
        assert!(!covered(scored_part(10, hits(), hits(), None), &fts));
        // A vector block scored without restricting the scan (the TopN is not full).
        let unrestricted = Some(matches(&[7, 2], Some(&[0.5, 0.9]), false));
        assert!(!covered(scored_part(10, None, unrestricted, None), &vector));
        // No scores, a score per row missing, a row past the block, a row range.
        assert!(!covered(
            scored_part(10, Some(matches(&[7], None, true)), None, None),
            &fts
        ));
        let short = Some(matches(&[7, 2], Some(&[0.5]), true));
        assert!(!covered(scored_part(10, short, None, None), &fts));
        assert!(!covered(scored_part(7, hits(), None, None), &fts));
        assert!(!covered(scored_part(10, hits(), None, Some(0..5)), &fts));
        // No index metadata at all.
        assert!(!covered(part(1), &fts));

        let part = scored_part(10, hits(), None, None);
        let prefix = compute_row_id_prefix(3, 5);
        let rows: Vec<_> = FuseBlockPartInfo::from_part(&part)
            .unwrap()
            .scored_rows(&fts)
            .unwrap()
            .collect();
        assert_eq!(rows, vec![
            (F32::from(0.5), compute_row_id(prefix, 7)),
            (F32::from(0.9), compute_row_id(prefix, 2)),
        ]);
    }
}
