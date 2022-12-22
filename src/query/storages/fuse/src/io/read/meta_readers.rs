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

use common_arrow::parquet::metadata::FileMetaData;
use common_exception::Result;
use common_storages_cache::CachedReader;
use common_storages_cache::Loader;
use common_storages_table_meta::caches::CacheManager;
use common_storages_table_meta::meta::SegmentInfo;
use common_storages_table_meta::meta::SegmentInfoVersion;
use common_storages_table_meta::meta::SnapshotVersion;
use common_storages_table_meta::meta::TableSnapshot;
use common_storages_table_meta::meta::TableSnapshotStatistics;
use common_storages_table_meta::meta::TableSnapshotStatisticsVersion;
use opendal::raw::BytesReader;
use opendal::Operator;

use super::versioned_reader::VersionedReader;

pub type SegmentInfoReader = CachedReader<SegmentInfo, LoaderWrapper<Operator>>;
pub type TableSnapshotReader = CachedReader<TableSnapshot, LoaderWrapper<Operator>>;
pub type TableSnapshotStatisticsReader =
    CachedReader<TableSnapshotStatistics, LoaderWrapper<Operator>>;
pub type BloomIndexFileMetaDataReader = CachedReader<FileMetaData, Operator>;

pub struct MetaReaders;

impl MetaReaders {
    pub fn segment_info_reader(dal: Operator) -> SegmentInfoReader {
        SegmentInfoReader::new(
            CacheManager::instance().get_table_segment_cache(),
            "SEGMENT_INFO_CACHE".to_owned(),
            LoaderWrapper(dal),
        )
    }

    pub fn table_snapshot_reader(dal: Operator) -> TableSnapshotReader {
        TableSnapshotReader::new(
            CacheManager::instance().get_table_snapshot_cache(),
            "SNAPSHOT_CACHE".to_owned(),
            LoaderWrapper(dal),
        )
    }

    pub fn table_snapshot_statistics_reader(dal: Operator) -> TableSnapshotStatisticsReader {
        TableSnapshotStatisticsReader::new(
            CacheManager::instance().get_table_snapshot_statistics_cache(),
            "TABLE_STATISTICS_CACHE".to_owned(),
            LoaderWrapper(dal),
        )
    }

    pub fn file_meta_data_reader(dal: Operator) -> BloomIndexFileMetaDataReader {
        BloomIndexFileMetaDataReader::new(
            CacheManager::instance().get_bloom_index_meta_cache(),
            "BLOOM_INDEX_FILE_META_DATA_CACHE".to_owned(),
            dal,
        )
    }
}

// workaround for the orphan rules
// Loader and types of table meta data are all defined outside (of this crate)
pub struct LoaderWrapper<T>(T);

#[async_trait::async_trait]
impl Loader<TableSnapshot> for LoaderWrapper<Operator> {
    async fn load(
        &self,
        key: &str,
        length_hint: Option<u64>,
        version: u64,
    ) -> Result<TableSnapshot> {
        let version = SnapshotVersion::try_from(version)?;
        let reader = bytes_reader(&self.0, key, length_hint).await?;
        version.read(reader).await
    }
}

#[async_trait::async_trait]
impl Loader<TableSnapshotStatistics> for LoaderWrapper<Operator> {
    async fn load(
        &self,
        key: &str,
        length_hint: Option<u64>,
        version: u64,
    ) -> Result<TableSnapshotStatistics> {
        let version = TableSnapshotStatisticsVersion::try_from(version)?;
        let reader = bytes_reader(&self.0, key, length_hint).await?;
        version.read(reader).await
    }
}

#[async_trait::async_trait]
impl Loader<SegmentInfo> for LoaderWrapper<Operator> {
    async fn load(&self, key: &str, length_hint: Option<u64>, version: u64) -> Result<SegmentInfo> {
        let version = SegmentInfoVersion::try_from(version)?;
        let reader = bytes_reader(&self.0, key, length_hint).await?;
        version.read(reader).await
    }
}

async fn bytes_reader(op: &Operator, path: &str, len: Option<u64>) -> Result<BytesReader> {
    let object = op.object(path);

    let len = match len {
        Some(l) => l,
        None => {
            let meta = object.metadata().await?;

            meta.content_length()
        }
    };

    let reader = object.range_reader(0..len).await?;
    Ok(Box::new(reader))
}
