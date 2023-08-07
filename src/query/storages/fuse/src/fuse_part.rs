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
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Range;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common_arrow::parquet::metadata::ColumnDescriptor;
use common_catalog::plan::PartInfo;
use common_catalog::plan::PartInfoPtr;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ColumnId;
use common_expression::Scalar;
use storages_common_pruner::BlockMetaIndex;
use storages_common_table_meta::meta::ColumnMeta;
use storages_common_table_meta::meta::Compression;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub struct FusePartInfo {
    pub location: String,

    pub create_on: Option<DateTime<Utc>>,
    pub nums_rows: usize,
    pub columns_meta: HashMap<ColumnId, ColumnMeta>,
    pub virtual_columns_meta: Option<HashMap<String, VirtualColumnMeta>>,
    pub compression: Compression,

    pub sort_min_max: Option<(Scalar, Scalar)>,
    pub block_meta_index: Option<BlockMetaIndex>,
}

#[typetag::serde(name = "fuse")]
impl PartInfo for FusePartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<FusePartInfo>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.location.hash(&mut s);
        s.finish()
    }
}

impl FusePartInfo {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        location: String,
        rows_count: u64,
        columns_meta: HashMap<ColumnId, ColumnMeta>,
        virtual_columns_meta: Option<HashMap<String, VirtualColumnMeta>>,
        compression: Compression,
        sort_min_max: Option<(Scalar, Scalar)>,
        block_meta_index: Option<BlockMetaIndex>,
        create_on: Option<DateTime<Utc>>,
    ) -> Arc<Box<dyn PartInfo>> {
        Arc::new(Box::new(FusePartInfo {
            location,
            create_on,
            columns_meta,
            virtual_columns_meta,
            nums_rows: rows_count as usize,
            compression,
            sort_min_max,
            block_meta_index,
        }))
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&FusePartInfo> {
        info.as_any()
            .downcast_ref::<FusePartInfo>()
            .ok_or(ErrorCode::Internal(
                "Cannot downcast from PartInfo to FusePartInfo.",
            ))
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

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
pub struct VirtualColumnMeta {
    pub index: usize,
    pub meta: ColumnMeta,
    pub desc: ColumnDescriptor,
}
