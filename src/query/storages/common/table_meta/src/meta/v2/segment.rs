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

use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::ColumnId;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::VariantDataType;
use databend_common_expression::types::NumberDataType;
use databend_common_frozen_api::FrozenAPI;
use databend_common_frozen_api::frozen_api;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::BlockHLLState;
use crate::meta::BlockTopN;
use crate::meta::ClusterStatistics;
use crate::meta::ColumnStatistics;
use crate::meta::Compression;
use crate::meta::FormatVersion;
use crate::meta::Location;
use crate::meta::SpatialStatistics;
use crate::meta::Statistics;
use crate::meta::StatisticsOfVectorColumns;
use crate::meta::Versioned;
use crate::meta::v0;
use crate::meta::v1;

/// A segment comprises one or more blocks
#[frozen_api("e19aba63")]
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, FrozenAPI)]
pub struct SegmentInfo {
    /// format version
    pub format_version: FormatVersion,
    /// blocks belong to this segment
    pub blocks: Vec<Arc<BlockMeta>>,
    /// summary statistics
    pub summary: Statistics,
}

impl SegmentInfo {
    // for test.
    pub fn new(blocks: Vec<Arc<BlockMeta>>, summary: Statistics) -> Self {
        Self {
            format_version: SegmentInfo::VERSION,
            blocks,
            summary,
        }
    }
}

// The virtual column variant types
const VIRTUAL_COLUMN_JSONB_TYPE: u8 = 0;
const VIRTUAL_COLUMN_BOOL_TYPE: u8 = 1;
const VIRTUAL_COLUMN_UINT64_TYPE: u8 = 2;
const VIRTUAL_COLUMN_INT64_TYPE: u8 = 3;
const VIRTUAL_COLUMN_FLOAT64_TYPE: u8 = 4;
const VIRTUAL_COLUMN_STRING_TYPE: u8 = 5;

/// The column meta of virtual columns.
/// Virtual column is the internal field values extracted from variant type values,
/// used to speed up the reading of internal fields of variant data.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct VirtualColumnMeta {
    /// where the data of column start
    pub offset: u64,
    /// the length of the column
    pub len: u64,
    /// num of "rows"
    pub num_values: u64,
    /// the type of virtual column in a block
    // To make BlockMeta more compatible, use numbers to represent variant types
    // 0 => jsonb
    // 1 => bool
    // 2 => uint64
    // 3 => int64
    // 4 => float64
    // 5 => string
    pub data_type: u8,
    /// virtual column statistics.
    pub column_stat: Option<ColumnStatistics>,
}

impl VirtualColumnMeta {
    pub fn total_rows(&self) -> usize {
        self.num_values as usize
    }

    pub fn offset_length(&self) -> (u64, u64) {
        (self.offset, self.len)
    }

    pub fn data_type(&self) -> TableDataType {
        match self.data_type {
            VIRTUAL_COLUMN_JSONB_TYPE => TableDataType::Nullable(Box::new(TableDataType::Variant)),
            VIRTUAL_COLUMN_BOOL_TYPE => TableDataType::Nullable(Box::new(TableDataType::Boolean)),
            VIRTUAL_COLUMN_UINT64_TYPE => {
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64)))
            }
            VIRTUAL_COLUMN_INT64_TYPE => {
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int64)))
            }
            VIRTUAL_COLUMN_FLOAT64_TYPE => {
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Float64)))
            }
            VIRTUAL_COLUMN_STRING_TYPE => TableDataType::Nullable(Box::new(TableDataType::String)),
            _ => unreachable!(),
        }
    }

    pub fn data_type_code(variant_type: &VariantDataType) -> u8 {
        match variant_type {
            VariantDataType::Jsonb => VIRTUAL_COLUMN_JSONB_TYPE,
            VariantDataType::Boolean => VIRTUAL_COLUMN_BOOL_TYPE,
            VariantDataType::UInt64 => VIRTUAL_COLUMN_UINT64_TYPE,
            VariantDataType::Int64 => VIRTUAL_COLUMN_INT64_TYPE,
            VariantDataType::Float64 => VIRTUAL_COLUMN_FLOAT64_TYPE,
            VariantDataType::String => VIRTUAL_COLUMN_STRING_TYPE,
            _ => unreachable!(),
        }
    }
}

/// The block meta of virtual columns.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct VirtualBlockMeta {
    /// key is virtual columnId, value is VirtualColumnMeta
    pub virtual_column_metas: HashMap<ColumnId, VirtualColumnMeta>,
    /// The file size of virtual columns.
    pub virtual_column_size: u64,
    /// The file location of virtual columns.
    pub virtual_location: Location,
}

/// The draft column meta of virtual columns, virtual ColumnId is not set.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct DraftVirtualColumnMeta {
    pub source_column_id: ColumnId,
    pub name: String,
    pub data_type: VariantDataType,
    pub column_meta: VirtualColumnMeta,
}

/// The draft block meta of virtual columns.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct DraftVirtualBlockMeta {
    /// The draft virtual oclumn metas, virtual ColumnId needs to be set.
    pub virtual_column_metas: Vec<DraftVirtualColumnMeta>,
    /// The file size of virtual columns.
    pub virtual_column_size: u64,
    /// The file location of virtual columns.
    pub virtual_location: Location,
}

/// Metadata of one physical column-group file in a logical block.
///
/// A file may still contain column chunks that are no longer active. Readers must only use the
/// chunks listed in [`Self::active_column_ids`].
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct ColumnGroupFileMeta {
    pub active_column_ids: Vec<ColumnId>,
    pub location: Location,
    pub format_version: FormatVersion,
    pub file_size: u64,
    pub uncompressed_size: u64,
    pub leaf_column_metas: HashMap<ColumnId, ColumnMeta>,
    /// Ordinary Bloom file paired with this data file. Its location is derived from `location`.
    #[serde(default)]
    pub bloom: Option<ColumnGroupBloomMeta>,
}

impl ColumnGroupFileMeta {
    /// Active leaf metadata in this physical file.
    pub fn active_leaf_column_metas(&self) -> impl Iterator<Item = (ColumnId, &ColumnMeta)> {
        self.active_column_ids.iter().filter_map(|column_id| {
            self.leaf_column_metas
                .get(column_id)
                .map(|column_meta| (*column_id, column_meta))
        })
    }
}

/// Metadata of the ordinary Bloom file paired with a physical column-group file.
///
/// The Bloom file is self-describing. A stored filter is active only while its column id remains in
/// the owning [`ColumnGroupFileMeta::active_column_ids`].
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct ColumnGroupBloomMeta {
    pub format_version: FormatVersion,
    pub file_size: u64,
}

/// Meta information of a block
/// Part of and kept inside the [SegmentInfo]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct BlockMeta {
    pub row_count: u64,
    pub block_size: u64,
    pub file_size: u64,
    #[serde(deserialize_with = "crate::meta::v2::statistics::deserialize_col_stats")]
    pub col_stats: HashMap<ColumnId, ColumnStatistics>,
    pub col_metas: HashMap<ColumnId, ColumnMeta>,
    /// Physical files that contain the active columns of this logical block.
    ///
    /// An empty vector is the legacy single-file representation described by `location`,
    /// `file_size`, `block_size`, and `col_metas`.
    #[serde(default)]
    pub column_groups: Vec<ColumnGroupFileMeta>,
    pub cluster_stats: Option<ClusterStatistics>,
    /// Compatibility anchor for this logical block's data.
    ///
    /// In the legacy layout this is the only data-file location. In a split layout it identifies
    /// the newest column-group file and does not cover the other active files; use
    /// [`Self::physical_column_groups`] or [`Self::data_file_locations`] for physical reads.
    pub location: Location,
    /// location of bloom filter index
    pub bloom_filter_index_location: Option<Location>,

    #[serde(default)]
    pub bloom_filter_index_size: u64,
    pub inverted_index_size: Option<u64>,
    pub ngram_filter_index_size: Option<u64>,
    pub vector_index_size: Option<u64>,
    pub vector_index_location: Option<Location>,
    pub spatial_index_size: Option<u64>,
    pub spatial_index_location: Option<Location>,
    pub spatial_stats: Option<HashMap<ColumnId, SpatialStatistics>>,
    pub vector_stats: Option<StatisticsOfVectorColumns>,
    /// The block meta of virtual columns.
    pub virtual_block_meta: Option<VirtualBlockMeta>,
    pub compression: Compression,

    // block create_on
    pub create_on: Option<DateTime<Utc>>,
}

impl BlockMeta {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        row_count: u64,
        block_size: u64,
        file_size: u64,
        col_stats: HashMap<ColumnId, ColumnStatistics>,
        col_metas: HashMap<ColumnId, ColumnMeta>,
        cluster_stats: Option<ClusterStatistics>,
        location: Location,
        bloom_filter_index_location: Option<Location>,
        bloom_filter_index_size: u64,
        inverted_index_size: Option<u64>,
        ngram_filter_index_size: Option<u64>,
        vector_index_size: Option<u64>,
        vector_index_location: Option<Location>,
        spatial_index_size: Option<u64>,
        spatial_index_location: Option<Location>,
        spatial_stats: Option<HashMap<ColumnId, SpatialStatistics>>,
        virtual_block_meta: Option<VirtualBlockMeta>,
        compression: Compression,
        create_on: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            row_count,
            block_size,
            file_size,
            col_stats,
            col_metas,
            column_groups: vec![],
            cluster_stats,
            location,
            bloom_filter_index_location,
            bloom_filter_index_size,
            inverted_index_size,
            ngram_filter_index_size,
            vector_index_size,
            vector_index_location,
            spatial_index_size,
            spatial_index_location,
            spatial_stats,
            vector_stats: None,
            virtual_block_meta,
            compression,
            create_on,
        }
    }

    pub fn compression(&self) -> Compression {
        self.compression
    }
    /// Active physical data files referenced by this logical block.
    pub fn data_file_locations(&self) -> impl Iterator<Item = &Location> {
        self.column_groups
            .is_empty()
            .then_some(&self.location)
            .into_iter()
            .chain(self.column_groups.iter().map(|group| &group.location))
    }

    fn legacy_column_group(
        &self,
        projected_column_ids: Option<&HashSet<ColumnId>>,
    ) -> ColumnGroupFileMeta {
        let mut active_column_ids = self
            .col_metas
            .keys()
            .filter(|column_id| {
                projected_column_ids.is_none_or(|projected| projected.contains(column_id))
            })
            .copied()
            .collect::<Vec<_>>();
        active_column_ids.sort_unstable();
        let leaf_column_metas = active_column_ids
            .iter()
            .map(|column_id| (*column_id, self.col_metas[column_id].clone()))
            .collect();
        ColumnGroupFileMeta {
            active_column_ids,
            location: self.location.clone(),
            format_version: self.location.1,
            file_size: self.file_size,
            uncompressed_size: self.block_size,
            leaf_column_metas,
            bloom: self
                .bloom_filter_index_location
                .as_ref()
                .map(|location| ColumnGroupBloomMeta {
                    format_version: location.1,
                    file_size: self.bloom_filter_index_size,
                }),
        }
    }

    /// Normalize legacy and split layouts to the active physical data-file view.
    pub fn physical_column_groups(&self) -> Cow<'_, [ColumnGroupFileMeta]> {
        if !self.column_groups.is_empty() {
            return Cow::Borrowed(&self.column_groups);
        }

        Cow::Owned(vec![self.legacy_column_group(None)])
    }

    /// Project active leaf metadata while preserving each owning physical file.
    pub fn project_column_groups(
        &self,
        projected_column_ids: &HashSet<ColumnId>,
    ) -> Vec<ColumnGroupFileMeta> {
        let project_group =
            |active_column_ids: &[ColumnId],
             location: &Location,
             format_version: FormatVersion,
             file_size: u64,
             uncompressed_size: u64,
             leaf_column_metas: &HashMap<ColumnId, ColumnMeta>| {
                let active_column_ids = active_column_ids
                    .iter()
                    .filter(|column_id| projected_column_ids.contains(column_id))
                    .filter(|column_id| leaf_column_metas.contains_key(column_id))
                    .copied()
                    .collect::<Vec<_>>();
                if active_column_ids.is_empty() {
                    return None;
                }
                let leaf_column_metas = active_column_ids
                    .iter()
                    .map(|column_id| (*column_id, leaf_column_metas[column_id].clone()))
                    .collect();
                Some(ColumnGroupFileMeta {
                    active_column_ids,
                    location: location.clone(),
                    format_version,
                    file_size,
                    uncompressed_size,
                    leaf_column_metas,
                    bloom: None,
                })
            };

        if self.column_groups.is_empty() {
            let group = self.legacy_column_group(Some(projected_column_ids));
            return (!group.active_column_ids.is_empty())
                .then_some(group)
                .into_iter()
                .collect();
        }

        self.column_groups
            .iter()
            .filter_map(|group| {
                project_group(
                    &group.active_column_ids,
                    &group.location,
                    group.format_version,
                    group.file_size,
                    group.uncompressed_size,
                    &group.leaf_column_metas,
                )
                .map(|mut projected| {
                    projected.bloom = group.bloom.clone();
                    projected
                })
            })
            .collect()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct ExtendedBlockMeta {
    pub block_meta: BlockMeta,
    pub draft_virtual_block_meta: Option<DraftVirtualBlockMeta>,
    pub column_hlls: Option<BlockHLLState>,
    #[serde(default)]
    pub column_top_n: Option<BlockTopN>,
}

#[typetag::serde(name = "extended_block_meta")]
impl BlockMetaInfo for ExtendedBlockMeta {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        ExtendedBlockMeta::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

#[typetag::serde(name = "blockmeta")]
impl BlockMetaInfo for BlockMeta {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        BlockMeta::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl SegmentInfo {
    pub fn from_v0(s: v0::SegmentInfo, fields: &[TableField]) -> Self {
        let summary = Statistics::from_v0(s.summary, fields);
        Self {
            // the is no version before v0, and no versions other then 0 can be converted into v0
            format_version: v0::SegmentInfo::VERSION,
            blocks: s
                .blocks
                .into_iter()
                .map(|b| Arc::new(BlockMeta::from_v0(&b, fields)))
                .collect::<_>(),
            summary,
        }
    }

    pub fn from_v1(s: v1::SegmentInfo, fields: &[TableField]) -> Self {
        let summary = Statistics::from_v0(s.summary, fields);
        Self {
            // NOTE: it is important to let the format_version return from here
            // carries the format_version of segment info being converted.
            format_version: s.format_version,
            blocks: s
                .blocks
                .into_iter()
                .map(|b| Arc::new(BlockMeta::from_v1(b.as_ref(), fields)))
                .collect::<_>(),
            summary,
        }
    }
}

#[derive(
    serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, EnumAsInner, FrozenAPI,
)]
pub enum ColumnMeta {
    Parquet(v0::ColumnMeta),
}

impl ColumnMeta {
    pub fn total_rows(&self) -> usize {
        match self {
            ColumnMeta::Parquet(v) => v.num_values as usize,
        }
    }

    pub fn offset_length(&self) -> (u64, u64) {
        match self {
            ColumnMeta::Parquet(v) => (v.offset, v.len),
        }
    }

    pub fn read_rows(&self, _range: Option<&Range<usize>>) -> u64 {
        match self {
            ColumnMeta::Parquet(v) => v.num_values,
        }
    }

    pub fn read_bytes(&self, _range: &Option<Range<usize>>) -> u64 {
        match self {
            ColumnMeta::Parquet(v) => v.len,
        }
    }
}

impl BlockMeta {
    pub fn from_v0(s: &v0::BlockMeta, fields: &[TableField]) -> Self {
        let col_stats = Statistics::convert_column_stats(&s.col_stats, fields);

        let col_metas = s
            .col_metas
            .iter()
            .map(|(k, v)| (*k, ColumnMeta::Parquet(v.clone())))
            .collect();

        Self {
            row_count: s.row_count,
            block_size: s.block_size,
            file_size: s.file_size,
            col_stats,
            col_metas,
            column_groups: vec![],
            cluster_stats: None,
            location: (s.location.path.clone(), 0),
            bloom_filter_index_location: None,
            bloom_filter_index_size: 0,
            compression: Compression::Lz4,
            inverted_index_size: None,
            vector_index_size: None,
            vector_index_location: None,
            spatial_index_size: None,
            spatial_index_location: None,
            spatial_stats: None,
            vector_stats: None,
            virtual_block_meta: None,
            create_on: None,
            ngram_filter_index_size: None,
        }
    }

    pub fn from_v1(s: &v1::BlockMeta, fields: &[TableField]) -> Self {
        let col_stats = Statistics::convert_column_stats(&s.col_stats, fields);
        let col_metas = s
            .col_metas
            .iter()
            .map(|(k, v)| (*k, ColumnMeta::Parquet(v.clone())))
            .collect();

        Self {
            row_count: s.row_count,
            block_size: s.block_size,
            file_size: s.file_size,
            col_stats,
            col_metas,
            column_groups: vec![],
            cluster_stats: None,
            location: s.location.clone(),
            bloom_filter_index_location: s.bloom_filter_index_location.clone(),
            bloom_filter_index_size: s.bloom_filter_index_size,
            compression: s.compression,
            inverted_index_size: None,
            vector_index_size: None,
            vector_index_location: None,
            spatial_index_size: None,
            spatial_index_location: None,
            spatial_stats: None,
            vector_stats: None,
            virtual_block_meta: None,
            create_on: None,
            ngram_filter_index_size: None,
        }
    }
}

impl From<(v1::SegmentInfo, &[TableField])> for SegmentInfo {
    fn from((v, fields): (v1::SegmentInfo, &[TableField])) -> Self {
        SegmentInfo::from_v1(v, fields)
    }
}

impl From<(v0::SegmentInfo, &[TableField])> for SegmentInfo {
    fn from((v, fields): (v0::SegmentInfo, &[TableField])) -> Self {
        SegmentInfo::from_v0(v, fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_active_leaf_column_metas_excludes_inactive_and_missing_columns() {
        let group = ColumnGroupFileMeta {
            active_column_ids: vec![1, 3],
            location: ("group.parquet".to_string(), 4),
            format_version: 4,
            file_size: 20,
            uncompressed_size: 40,
            leaf_column_metas: HashMap::from([
                (
                    1,
                    ColumnMeta::Parquet(v0::ColumnMeta {
                        offset: 10,
                        len: 11,
                        num_values: 12,
                    }),
                ),
                (
                    2,
                    ColumnMeta::Parquet(v0::ColumnMeta {
                        offset: 20,
                        len: 21,
                        num_values: 22,
                    }),
                ),
            ]),
            bloom: None,
        };

        let active_metas = group
            .active_leaf_column_metas()
            .map(|(column_id, column_meta)| (column_id, column_meta.offset_length()))
            .collect::<Vec<_>>();
        assert_eq!(active_metas, vec![(1, (10, 11))]);
    }

    #[test]
    fn test_deserialize_legacy_block_meta_without_column_groups() {
        let block_meta = BlockMeta::new(
            10,
            300,
            30,
            HashMap::new(),
            HashMap::new(),
            None,
            ("old.parquet".to_string(), 2),
            None,
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Compression::Zstd,
            None,
        );
        let location = block_meta.location.clone();
        let mut value = serde_json::to_value(block_meta).unwrap();
        assert!(
            value
                .as_object_mut()
                .unwrap()
                .remove("column_groups")
                .is_some()
        );
        let decoded: BlockMeta = serde_json::from_value(value).unwrap();
        assert!(decoded.column_groups.is_empty());
        assert_eq!(decoded.location, location);
    }
}
