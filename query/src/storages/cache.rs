// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use async_trait::async_trait;
use common_arrow::arrow::io::ipc::read::FileMetadata;
use common_dal::DataAccessor;
use common_exception::Result;

use crate::configs::QueryConfig;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::TableSnapshot;

#[async_trait]
pub trait StorageCache: Send + Sync {
    async fn get(&self, location: &str, da: &dyn DataAccessor) -> Result<Vec<u8>>;
}

#[async_trait]
pub trait StorageCacheNew<T>: Send + Sync {
    async fn get(&self, location: &str, da: &dyn DataAccessor) -> Result<T>;
}

pub struct CacheMgr {}

impl CacheMgr {
    fn init_cache(config: &QueryConfig) -> Result<Self> {
        todo!()
    }

    fn get_table_snapshot_cache() -> Result<Arc<dyn StorageCacheNew<TableSnapshot>>> {
        todo!()
    }

    fn get_table_segment_cache() -> Result<Arc<dyn StorageCacheNew<SegmentInfo>>> {
        todo!()
    }

    fn get_block_meata_cache() -> Result<Arc<dyn StorageCacheNew<FileMetadata>>> {
        todo!()
    }
}
