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

use std::io::ErrorKind;
use std::sync::Arc;

use common_arrow::arrow::io::parquet::read::read_metadata_async;
use common_arrow::arrow::io::parquet::read::schema::FileMetaData;
use common_dal::InputStream;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing::debug_span;
use common_tracing::tracing::Instrument;
use serde::de::DeserializeOwned;

use crate::sessions::QueryContext;
use crate::storages::fuse::cache::CachedLoader;
use crate::storages::fuse::cache::Loader;
use crate::storages::fuse::cache::TableInMemCache;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::TableSnapshot;

pub trait InputStreamProvider {
    fn input_stream(&self, path: &str, len: Option<u64>) -> Result<InputStream>;
}

impl InputStreamProvider for &QueryContext {
    fn input_stream(&self, path: &str, len: Option<u64>) -> Result<InputStream> {
        let accessor = self.get_storage_accessor()?;
        accessor.get_input_stream(path, len)
    }
}

impl InputStreamProvider for Arc<QueryContext> {
    fn input_stream(&self, path: &str, len: Option<u64>) -> Result<InputStream> {
        let ctx = self.as_ref();
        ctx.input_stream(path, len)
    }
}

#[async_trait::async_trait]
impl<T, V> Loader<V> for T
where
    T: InputStreamProvider + Sync,
    V: DeserializeOwned,
{
    async fn load(&self, key: &str) -> Result<V> {
        let mut reader = self.input_stream(key, None)?; // TODO we know the length of stream
        let mut buffer = vec![];

        use futures::AsyncReadExt;
        reader.read_to_end(&mut buffer).await.map_err(|e| {
            let msg = e.to_string();
            if e.kind() == ErrorKind::NotFound {
                ErrorCode::DalPathNotFound(msg)
            } else {
                ErrorCode::DalTransportError(msg)
            }
        })?;
        let r = serde_json::from_slice::<V>(&buffer)?;
        Ok(r)
    }
}

pub struct BlockMeta(FileMetaData);

impl BlockMeta {
    pub fn inner(&self) -> &FileMetaData {
        &self.0
    }
}

#[async_trait::async_trait]
impl<T> Loader<BlockMeta> for T
where T: InputStreamProvider + Sync
{
    async fn load(&self, key: &str) -> Result<BlockMeta> {
        let mut reader = self.input_stream(key, None)?; // TODO we know the length of stream
        let meta = read_metadata_async(&mut reader)
            .instrument(debug_span!("parquet_source_read_meta"))
            .await
            .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;
        Ok(BlockMeta(meta))
    }
}

pub type SegmentInfoCache = TableInMemCache<SegmentInfo>;
pub type TableSnapshotCache = TableInMemCache<TableSnapshot>;
pub type BlockMetaCache = TableInMemCache<BlockMeta>;

pub type SegmentInfoReader<'a> = CachedLoader<SegmentInfo, &'a QueryContext>;
pub type TableSnapshotReader<'a> = CachedLoader<TableSnapshot, &'a QueryContext>;
pub type BlockMetaReader = CachedLoader<BlockMeta, Arc<QueryContext>>;

pub struct Readers;

impl Readers {
    pub fn segment_info_reader(ctx: &QueryContext) -> SegmentInfoReader {
        SegmentInfoReader::new(ctx.get_storage_cache_mgr().get_table_segment_cache(), ctx)
    }

    pub fn table_snapshot_reader(ctx: &QueryContext) -> TableSnapshotReader {
        TableSnapshotReader::new(ctx.get_storage_cache_mgr().get_table_snapshot_cache(), ctx)
    }

    pub fn block_meta_reader(ctx: Arc<QueryContext>) -> BlockMetaReader {
        BlockMetaReader::new(ctx.get_storage_cache_mgr().get_block_meta_cache(), ctx)
    }
}
