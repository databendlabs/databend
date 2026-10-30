// Copyright 2026 Datafuse Labs
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

//! Spatial indexes: the footer of each index file, and its columns.

use std::sync::Arc;

use databend_storages_common_index::SpatialIndexFile;
use databend_storages_common_index::SpatialIndexMeta;

use crate::CacheDef;
use crate::Disk;
use crate::Pool;

/// Footers of spatial index files, by index location.
pub struct Meta;

impl CacheDef for Meta {
    type Value = Arc<SpatialIndexMeta>;
    const NAME: &'static str = "memory_cache_spatial_index_file_meta_data";
    const POOL: Pool = Pool::Meta;
    const DISK: Option<Disk<Self::Value>> = Some(Disk::new("spatial_index_meta_v1", |c| {
        c.disk_cache_spatial_index_meta_size
    }));
}

/// Spatial index columns, by `{index_location}-{column}`.
pub struct Index;

impl CacheDef for Index {
    type Value = SpatialIndexFile;
    const NAME: &'static str = "memory_cache_spatial_index_file";
    const POOL: Pool = Pool::Index;
    const DISK: Option<Disk<Self::Value>> = Some(Disk::new("spatial_index_file_v1", |c| {
        c.disk_cache_spatial_index_data_size
    }));
}
