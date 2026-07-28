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
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnGroupFileMeta;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::Location;

use crate::io::TableMetaLocationGenerator;

/// Projected column chunks to read from one physical column-group file.
#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct FuseColumnGroupPartInfo {
    pub location: String,
    pub columns_meta: HashMap<ColumnId, ColumnMeta>,
}

/// Runtime description of a Bloom file paired with one physical data group.
#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct FuseBloomIndexFileInfo {
    pub active_column_ids: Vec<ColumnId>,
    pub location: Location,
    pub file_size: u64,
}

#[derive(Clone, Debug)]
pub enum BloomIndexLayout<'a> {
    Legacy {
        location: &'a Location,
        file_size: u64,
    },
    ColumnGroups {
        files: Cow<'a, [FuseBloomIndexFileInfo]>,
    },
}

impl<'a> BloomIndexLayout<'a> {
    fn from_metadata(
        legacy_location: Option<&'a Location>,
        legacy_file_size: u64,
        column_group_files: Cow<'a, [FuseBloomIndexFileInfo]>,
    ) -> Option<Self> {
        if !column_group_files.is_empty() {
            return Some(Self::ColumnGroups {
                files: column_group_files,
            });
        }

        legacy_location.map(|location| Self::Legacy {
            location,
            file_size: legacy_file_size,
        })
    }
}

fn column_group_bloom_location(group: &ColumnGroupFileMeta) -> Option<Location> {
    group.bloom.as_ref().map(|bloom| {
        (
            TableMetaLocationGenerator::gen_bloom_index_location_with_version(
                &group.location.0,
                bloom.format_version,
            ),
            bloom.format_version,
        )
    })
}

pub(crate) fn column_group_bloom_files(meta: &BlockMeta) -> Vec<FuseBloomIndexFileInfo> {
    meta.column_groups
        .iter()
        .filter_map(|group| {
            let bloom = group.bloom.as_ref()?;
            Some(FuseBloomIndexFileInfo {
                active_column_ids: group.active_column_ids.clone(),
                location: column_group_bloom_location(group)?,
                file_size: bloom.file_size,
            })
        })
        .collect()
}

pub(crate) fn legacy_bloom_index_location(meta: &BlockMeta) -> Option<&Location> {
    meta.column_groups
        .is_empty()
        .then_some(meta.bloom_filter_index_location.as_ref())
        .flatten()
}

/// Physical ordinary Bloom files referenced by a logical block.
pub fn block_bloom_index_locations(meta: &BlockMeta) -> impl Iterator<Item = Location> + '_ {
    let legacy = legacy_bloom_index_location(meta).cloned();
    legacy.into_iter().chain(
        meta.column_groups
            .iter()
            .filter_map(column_group_bloom_location),
    )
}

pub(crate) fn bloom_index_layout(meta: &BlockMeta) -> Option<BloomIndexLayout<'_>> {
    let files = column_group_bloom_files(meta);
    BloomIndexLayout::from_metadata(
        legacy_bloom_index_location(meta),
        meta.bloom_filter_index_size,
        Cow::Owned(files),
    )
}

pub(crate) fn project_column_groups(
    meta: &BlockMeta,
    projected_column_ids: &HashSet<ColumnId>,
) -> Vec<FuseColumnGroupPartInfo> {
    meta.project_column_groups(projected_column_ids)
        .into_iter()
        .map(|group| FuseColumnGroupPartInfo {
            location: group.location.0,
            columns_meta: group.leaf_column_metas,
        })
        .collect()
}

/// Fuse table partition information.
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct FuseBlockPartInfo {
    pub location: String,

    pub bloom_filter_index_location: Option<Location>,
    pub bloom_filter_index_size: u64,
    #[serde(default)]
    pub column_group_bloom_files: Vec<FuseBloomIndexFileInfo>,

    pub create_on: Option<DateTime<Utc>>,
    pub nums_rows: usize,
    pub column_groups: Vec<FuseColumnGroupPartInfo>,
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
    /// Normalize optional legacy and column-group Bloom metadata into one physical-layout view.
    pub fn bloom_index_layout(&self) -> Option<BloomIndexLayout<'_>> {
        BloomIndexLayout::from_metadata(
            self.bloom_filter_index_location.as_ref(),
            self.bloom_filter_index_size,
            Cow::Borrowed(&self.column_group_bloom_files),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create(
        location: String,
        bloom_filter_index_location: Option<Location>,
        bloom_filter_index_size: u64,
        column_group_bloom_files: Vec<FuseBloomIndexFileInfo>,
        rows_count: u64,
        column_groups: Vec<FuseColumnGroupPartInfo>,
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
            column_group_bloom_files,
            create_on,
            column_groups,
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
