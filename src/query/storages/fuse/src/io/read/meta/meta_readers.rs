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

use common_arrow::parquet::read::read_metadata_async;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::TableSchemaRef;
use opendal::ObjectReader;
use opendal::Operator;
use storages_common_cache::InMemoryItemCacheReader;
use storages_common_cache::LoadParams;
use storages_common_cache::Loader;
use storages_common_cache_manager::BloomIndexMeta;
use storages_common_cache_manager::CacheManager;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::SegmentInfoVersion;
use storages_common_table_meta::meta::SnapshotVersion;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::TableSnapshotStatistics;
use storages_common_table_meta::meta::TableSnapshotStatisticsVersion;

use super::versioned_reader::VersionedReader;

pub type TableSnapshotStatisticsReader =
    InMemoryItemCacheReader<TableSnapshotStatistics, LoaderWrapper<Operator>>;
pub type BloomIndexFileMetaDataReader =
    InMemoryItemCacheReader<BloomIndexMeta, LoaderWrapper<Operator>>;
pub type TableSnapshotReader = InMemoryItemCacheReader<TableSnapshot, LoaderWrapper<Operator>>;
pub type SegmentInfoReader =
    InMemoryItemCacheReader<SegmentInfo, LoaderWrapper<(Operator, TableSchemaRef)>>;

pub struct MetaReaders;

impl MetaReaders {
    pub fn segment_info_reader(dal: Operator, schema: TableSchemaRef) -> SegmentInfoReader {
        SegmentInfoReader::new(
            CacheManager::instance().get_table_segment_cache(),
            "segment_info_cache".to_owned(),
            LoaderWrapper((dal, schema)),
        )
    }

    pub fn table_snapshot_reader(dal: Operator) -> TableSnapshotReader {
        TableSnapshotReader::new(
            CacheManager::instance().get_table_snapshot_cache(),
            "snapshot_cache".to_owned(),
            LoaderWrapper(dal),
        )
    }

    pub fn table_snapshot_statistics_reader(dal: Operator) -> TableSnapshotStatisticsReader {
        TableSnapshotStatisticsReader::new(
            CacheManager::instance().get_table_snapshot_statistics_cache(),
            "table_statistics_cache".to_owned(),
            LoaderWrapper(dal),
        )
    }

    pub fn file_meta_data_reader(dal: Operator) -> BloomIndexFileMetaDataReader {
        BloomIndexFileMetaDataReader::new(
            CacheManager::instance().get_bloom_index_meta_cache(),
            "bloom_index_file_meta_data_cache".to_owned(),
            LoaderWrapper(dal),
        )
    }
}

// workaround for the orphan rules
// Loader and types of table meta data are all defined outside (of this crate)
pub struct LoaderWrapper<T>(T);

#[async_trait::async_trait]
impl Loader<TableSnapshot> for LoaderWrapper<Operator> {
    async fn load(&self, params: &LoadParams) -> Result<TableSnapshot> {
        let reader = bytes_reader(&self.0, params.location.as_str(), params.len_hint).await?;
        let version = SnapshotVersion::try_from(params.ver)?;
        version.read(reader).await
    }
}

#[async_trait::async_trait]
impl Loader<TableSnapshotStatistics> for LoaderWrapper<Operator> {
    async fn load(&self, params: &LoadParams) -> Result<TableSnapshotStatistics> {
        let version = TableSnapshotStatisticsVersion::try_from(params.ver)?;
        let reader = bytes_reader(&self.0, params.location.as_str(), params.len_hint).await?;
        version.read(reader).await
    }
}

#[async_trait::async_trait]
impl Loader<SegmentInfo> for LoaderWrapper<(Operator, TableSchemaRef)> {
    async fn load(&self, params: &LoadParams) -> Result<SegmentInfo> {
        let version = SegmentInfoVersion::try_from(params.ver)?;
        let LoaderWrapper((operator, schema)) = &self;
        let reader = bytes_reader(operator, params.location.as_str(), params.len_hint).await?;
        (version, schema.clone()).read(reader).await
    }
}

#[async_trait::async_trait]
impl Loader<BloomIndexMeta> for LoaderWrapper<Operator> {
    async fn load(&self, params: &LoadParams) -> Result<BloomIndexMeta> {
        let object = self.0.object(&params.location);
        let mut reader = if let Some(len) = params.len_hint {
            object.range_reader(0..len).await?
        } else {
            object.reader().await?
        };
        let meta = read_metadata_async(&mut reader).await.map_err(|err| {
            ErrorCode::Internal(format!(
                "read file meta failed, {}, {:?}",
                params.location, err
            ))
        })?;
        Ok(BloomIndexMeta(meta))
    }
}

async fn bytes_reader(op: &Operator, path: &str, len: Option<u64>) -> Result<ObjectReader> {
    let object = op.object(path);

    let len = match len {
        Some(l) => l,
        None => {
            // TODO why do we need the content length (extra HEAD http req)? here we just need to read ALL the content
            let meta = object.metadata().await?;
            meta.content_length()
        }
    };

    let reader = object.range_reader(0..len).await?;
    Ok(reader)
}
