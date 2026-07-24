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
use databend_storages_common_table_meta::meta::BloomIndexFileMeta;
use databend_storages_common_table_meta::meta::ColumnGroupFileMeta;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::Location;

/// Projected column chunks to read from one physical column-group file.
#[derive(Clone, serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct FuseColumnGroupPartInfo {
    pub location: String,
    pub columns_meta: HashMap<ColumnId, ColumnMeta>,
}

/// Normalize a legacy single-file block and a column-group block to the same physical-file view.
pub(crate) fn normalized_column_group_files(meta: &BlockMeta) -> Cow<'_, [ColumnGroupFileMeta]> {
    if !meta.column_groups.is_empty() {
        return Cow::Borrowed(&meta.column_groups);
    }

    let mut active_column_ids = meta.col_metas.keys().copied().collect::<Vec<_>>();
    active_column_ids.sort_unstable();
    Cow::Owned(vec![ColumnGroupFileMeta {
        active_column_ids,
        location: meta.location.clone(),
        format_version: meta.location.1,
        file_size: meta.file_size,
        uncompressed_size: meta.block_size,
        leaf_column_metas: meta.col_metas.clone(),
    }])
}

pub(crate) fn project_column_groups(
    meta: &BlockMeta,
    projected_column_ids: &HashSet<ColumnId>,
) -> Vec<FuseColumnGroupPartInfo> {
    if meta.column_groups.is_empty() {
        let columns_meta = meta
            .col_metas
            .iter()
            .filter(|(column_id, _)| projected_column_ids.contains(column_id))
            .map(|(column_id, column_meta)| (*column_id, column_meta.clone()))
            .collect::<HashMap<_, _>>();
        return if columns_meta.is_empty() {
            vec![]
        } else {
            vec![FuseColumnGroupPartInfo {
                location: meta.location.0.clone(),
                columns_meta,
            }]
        };
    }

    normalized_column_group_files(meta)
        .iter()
        .filter_map(|group| {
            let columns_meta = group
                .active_column_ids
                .iter()
                .filter(|column_id| projected_column_ids.contains(column_id))
                .filter_map(|column_id| {
                    group
                        .leaf_column_metas
                        .get(column_id)
                        .map(|column_meta| (*column_id, column_meta.clone()))
                })
                .collect::<HashMap<_, _>>();
            (!columns_meta.is_empty()).then(|| FuseColumnGroupPartInfo {
                location: group.location.0.clone(),
                columns_meta,
            })
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
    pub bloom_index_files: Vec<BloomIndexFileMeta>,

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
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        location: String,
        bloom_filter_index_location: Option<Location>,
        bloom_filter_index_size: u64,
        bloom_index_files: Vec<BloomIndexFileMeta>,
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
            bloom_index_files,
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

    pub fn page_size(&self) -> usize {
        self.block_meta_index
            .as_ref()
            .map(|meta| meta.page_size)
            .unwrap_or(self.nums_rows)
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
