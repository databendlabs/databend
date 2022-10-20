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

use common_arrow::parquet::metadata::FileMetaData;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_fuse_meta::caches::TenantLabel;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::SegmentInfoVersion;
use common_fuse_meta::meta::SnapshotVersion;
use common_fuse_meta::meta::TableSnapshot;
use common_storages_cache::CachedReader;
use common_storages_cache::HasTenantLabel;
use common_storages_cache::Loader;
use opendal::BytesReader;
use opendal::Operator;

use super::versioned_reader::VersionedReader;

pub type SegmentInfoReader<'a> = CachedReader<SegmentInfo, LoaderWrapper<&'a dyn TableContext>>;
pub type TableSnapshotReader = CachedReader<TableSnapshot, LoaderWrapper<Arc<dyn TableContext>>>;
pub type BloomIndexFileMetaDataReader = CachedReader<FileMetaData, Arc<dyn TableContext>>;

pub struct MetaReaders;

impl MetaReaders {
    pub fn segment_info_reader(ctx: &dyn TableContext, dal: Operator) -> SegmentInfoReader {
        SegmentInfoReader::new(
            CacheManager::instance().get_table_segment_cache(),
            LoaderWrapper(ctx),
            "SEGMENT_INFO_CACHE".to_owned(),
            dal,
        )
    }

    pub fn table_snapshot_reader(ctx: Arc<dyn TableContext>, dal: Operator) -> TableSnapshotReader {
        TableSnapshotReader::new(
            CacheManager::instance().get_table_snapshot_cache(),
            LoaderWrapper(ctx),
            "SNAPSHOT_CACHE".to_owned(),
            dal,
        )
    }

    pub fn file_meta_data_reader(
        ctx: Arc<dyn TableContext>,
        dal: Operator,
    ) -> BloomIndexFileMetaDataReader {
        BloomIndexFileMetaDataReader::new(
            CacheManager::instance().get_bloom_index_meta_cache(),
            ctx,
            "BLOOM_INDEX_FILE_META_DATA_CACHE".to_owned(),
            dal,
        )
    }
}

// workaround for the orphan rules
// Loader and types of table meta data are all defined outside (of this crate)
pub struct LoaderWrapper<T>(T);

#[async_trait::async_trait]
impl<T> Loader<TableSnapshot> for LoaderWrapper<T>
where T: Sync + Send
{
    async fn load(
        &self,
        op: Operator,
        key: &str,
        length_hint: Option<u64>,
        version: u64,
    ) -> Result<TableSnapshot> {
        let version = SnapshotVersion::try_from(version)?;
        let reader = bytes_reader(op, key, length_hint).await?;
        version.read(reader).await
    }
}

#[async_trait::async_trait]
impl<T> Loader<SegmentInfo> for LoaderWrapper<T>
where T: Sync + Send
{
    async fn load(
        &self,
        op: Operator,
        key: &str,
        length_hint: Option<u64>,
        version: u64,
    ) -> Result<SegmentInfo> {
        let version = SegmentInfoVersion::try_from(version)?;
        let reader = bytes_reader(op, key, length_hint).await?;
        version.read(reader).await
    }
}

async fn bytes_reader(op: Operator, path: &str, len: Option<u64>) -> Result<BytesReader> {
    let object = op.object(path);

    let len = match len {
        Some(l) => l,
        None => {
            let meta = object.metadata().await?;

            meta.content_length()
        }
    };

    let reader = object.range_reader(..len).await?;
    Ok(Box::new(reader))
}

impl<T> HasTenantLabel for LoaderWrapper<T>
where T: HasTenantLabel
{
    fn tenant_label(&self) -> TenantLabel {
        self.0.tenant_label()
    }
}
