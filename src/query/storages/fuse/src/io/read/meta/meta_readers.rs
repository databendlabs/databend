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
use std::io::SeekFrom;
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::Buf;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_io::constants::DEFAULT_FOOTER_READ_SIZE;
use databend_storages_common_cache::Reader;
use databend_storages_common_index::BloomIndexMeta;
use databend_storages_common_index::SpatialIndexMeta;
use databend_storages_common_index::VectorIndexMeta;
use databend_storages_common_index::VirtualColumnFileMeta;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfoVersion;
use databend_storages_common_table_meta::meta::SegmentStatistics;
use databend_storages_common_table_meta::meta::SegmentStatisticsVersion;
use databend_storages_common_table_meta::meta::SnapshotVersion;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use databend_storages_common_table_meta::meta::TableSnapshotStatisticsVersion;
use databend_storages_common_table_meta::readers::VersionedReader;
use futures::AsyncSeek;
use futures_util::AsyncSeekExt;
use opendal::Buffer;
use opendal::Operator;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::metadata::ParquetMetaDataReader;

pub use self::thrift_file_meta_read::read_thrift_file_metadata;
use crate::io::SnapshotKey;

// workaround for the orphan rules
// Loader and types of table meta data are all defined outside (of this crate)
/// Reads and decodes table snapshots. No caching; pair with `SnapshotCache`.
#[derive(Clone)]
pub struct SnapshotReader {
    operator: Operator,
}

impl SnapshotReader {
    pub fn new(operator: Operator) -> Self {
        SnapshotReader { operator }
    }
}

impl Reader for SnapshotReader {
    type Key = SnapshotKey;
    type Item = Arc<TableSnapshot>;

    fn cache_key<'a>(&self, key: &'a SnapshotKey) -> Cow<'a, str> {
        Cow::Borrowed(&key.0)
    }

    #[async_backtrace::framed]
    async fn read(&self, (location, ver): &SnapshotKey) -> Result<Arc<TableSnapshot>> {
        let version = SnapshotVersion::try_from(*ver)?;
        let buffer = bytes_reader(&self.operator, location, None).await?;
        Ok(Arc::new(version.read(buffer.reader())?))
    }
}

/// Reads and decodes table snapshot statistics. No caching; pair with `SnapshotCache`.
#[derive(Clone)]
pub struct SnapshotStatsReader {
    operator: Operator,
}

impl SnapshotStatsReader {
    pub fn new(operator: Operator) -> Self {
        SnapshotStatsReader { operator }
    }
}

impl Reader for SnapshotStatsReader {
    type Key = SnapshotKey;
    type Item = Arc<TableSnapshotStatistics>;

    fn cache_key<'a>(&self, key: &'a SnapshotKey) -> Cow<'a, str> {
        Cow::Borrowed(&key.0)
    }

    #[async_backtrace::framed]
    async fn read(&self, (location, ver): &SnapshotKey) -> Result<Arc<TableSnapshotStatistics>> {
        let version = TableSnapshotStatisticsVersion::try_from(*ver)?;
        let buffer = bytes_reader(&self.operator, location, None).await?;
        Ok(Arc::new(version.read(buffer.reader())?))
    }
}

/// Reads and decodes row-oriented segments. No caching; pair with `SegmentCache`.
#[derive(Clone)]
pub struct SegmentInfoReader {
    operator: Operator,
    schema: TableSchemaRef,
}

impl SegmentInfoReader {
    pub fn new(operator: Operator, schema: TableSchemaRef) -> Self {
        SegmentInfoReader { operator, schema }
    }
}

impl Reader for SegmentInfoReader {
    type Key = Location;
    type Item = Arc<CompactSegmentInfo>;

    fn cache_key<'a>(&self, key: &'a Location) -> Cow<'a, str> {
        Cow::Borrowed(&key.0)
    }

    #[async_backtrace::framed]
    async fn read(&self, (location, ver): &Location) -> Result<Arc<CompactSegmentInfo>> {
        let version = SegmentInfoVersion::try_from(*ver)?;
        let buffer = bytes_reader(&self.operator, location, None).await?;
        Ok(Arc::new(
            (version, self.schema.clone()).read(buffer.reader())?,
        ))
    }
}

/// Reads and decodes segment statistics. No caching; pair with `SegmentCache`.
#[derive(Clone)]
pub struct SegmentStatsReader {
    operator: Operator,
}

impl SegmentStatsReader {
    pub fn new(operator: Operator) -> Self {
        SegmentStatsReader { operator }
    }
}

impl Reader for SegmentStatsReader {
    type Key = Location;
    type Item = Arc<SegmentStatistics>;

    fn cache_key<'a>(&self, key: &'a Location) -> Cow<'a, str> {
        Cow::Borrowed(&key.0)
    }

    #[async_backtrace::framed]
    async fn read(&self, (location, ver): &Location) -> Result<Arc<SegmentStatistics>> {
        let version = SegmentStatisticsVersion::try_from(*ver)?;
        let buffer = bytes_reader(&self.operator, location, None).await?;
        Ok(Arc::new(version.read(buffer.reader())?))
    }
}

/// Reads and decodes the parquet footer of an index file as `T`. No caching; pair with
/// `FuseCaches::index_metas`. The key is the path and, when known, the file length.
pub struct IndexMetaReader<T> {
    operator: Operator,
    _meta: PhantomData<fn() -> T>,
}

impl<T> IndexMetaReader<T> {
    pub fn new(operator: Operator) -> Self {
        IndexMetaReader {
            operator,
            _meta: PhantomData,
        }
    }

    async fn read_footer(&self, (path, len): &IndexMetaKey) -> Result<ParquetMetaData> {
        read_thrift_file_metadata(self.operator.clone(), path, *len)
            .await
            .map_err(|err| {
                ErrorCode::StorageOther(format!("read file meta failed, {}, {:?}", path, err))
            })
    }
}

pub type IndexMetaKey = (String, Option<u64>);

macro_rules! index_meta_reader {
    ($ty:ty, |$meta:ident| $decode:expr) => {
        impl Reader for IndexMetaReader<$ty> {
            type Key = IndexMetaKey;
            type Item = Arc<$ty>;

            fn cache_key<'a>(&self, key: &'a IndexMetaKey) -> Cow<'a, str> {
                Cow::Borrowed(&key.0)
            }

            #[async_backtrace::framed]
            async fn read(&self, key: &IndexMetaKey) -> Result<Arc<$ty>> {
                let $meta = self.read_footer(key).await?;
                Ok(Arc::new($decode))
            }
        }
    };
}

index_meta_reader!(BloomIndexMeta, |meta| BloomIndexMeta::try_from(meta)?);
index_meta_reader!(SpatialIndexMeta, |meta| SpatialIndexMeta::try_from(meta)?);
index_meta_reader!(VectorIndexMeta, |meta| VectorIndexMeta::Legacy(
    SpatialIndexMeta::try_from(meta)?
));
index_meta_reader!(VirtualColumnFileMeta, |meta| {
    VirtualColumnFileMeta::try_from(meta)?
});

pub async fn bytes_reader(op: &Operator, path: &str, len_hint: Option<u64>) -> Result<Buffer> {
    let reader = if let Some(len) = len_hint {
        op.read_with(path).range(0..len).await?
    } else {
        op.read(path).await?
    };
    Ok(reader)
}

mod thrift_file_meta_read {
    use parquet::errors::ParquetError;

    use super::*;

    const HEADER_SIZE: u64 = PARQUET_MAGIC.len() as u64;
    const FOOTER_SIZE: u64 = 8;
    const PARQUET_MAGIC: [u8; 4] = *b"PAR1";

    #[async_backtrace::framed]
    async fn stream_len(
        seek: &mut (impl AsyncSeek + std::marker::Unpin),
    ) -> std::result::Result<u64, std::io::Error> {
        let old_pos = seek.seek(SeekFrom::Current(0)).await?;
        let len = seek.seek(SeekFrom::End(0)).await?;

        // Avoid seeking a third time when we were already at the end of the
        // stream. The branch is usually way cheaper than a seek operation.
        if old_pos != len {
            seek.seek(SeekFrom::Start(old_pos)).await?;
        }

        Ok(len)
    }

    fn metadata_len(buffer: &[u8], len: usize) -> i32 {
        i32::from_le_bytes(buffer[len - 8..len - 4].try_into().unwrap())
    }

    #[async_backtrace::framed]
    pub async fn read_thrift_file_metadata(
        op: Operator,
        path: &str,
        len_hint: Option<u64>,
    ) -> Result<ParquetMetaData> {
        let file_size = if let Some(len) = len_hint {
            len
        } else {
            let meta = op
                .stat(path)
                .await
                .map_err(|err| ParquetError::General(err.to_string()))?;
            meta.content_length()
        };

        if file_size < HEADER_SIZE + FOOTER_SIZE {
            return Err(ErrorCode::ParquetFileInvalid(
                "A parquet file must contain a header and footer with at least 12 bytes",
            ));
        }

        // read and cache up to DEFAULT_FOOTER_READ_SIZE bytes from the end and process the footer
        let default_end_len = std::cmp::min(DEFAULT_FOOTER_READ_SIZE, file_size) as usize;

        // read the end of the file
        let buffer = op
            .read_with(path)
            .range(file_size - default_end_len as u64..file_size)
            .await
            .map_err(|err| ParquetError::General(err.to_string()))?
            .to_vec();

        // check this is indeed a parquet file
        if buffer[default_end_len - 4..] != PARQUET_MAGIC {
            return Err(ErrorCode::ParquetFileInvalid(
                "Invalid Parquet file. Corrupt footer",
            ));
        }

        let metadata_len = metadata_len(&buffer, default_end_len);
        let metadata_len: u64 = metadata_len.try_into()?;

        let footer_len = FOOTER_SIZE + metadata_len;
        if footer_len > file_size {
            return Err(ErrorCode::ParquetFileInvalid(
                "The footer size must be smaller or equal to the file's size",
            ));
        }

        if (footer_len as usize) < buffer.len() {
            // the whole metadata is in the bytes we already read
            let remaining = buffer.len() - footer_len as usize;
            ParquetMetaDataReader::decode_metadata(&buffer[remaining..])
                .map_err(|err| ErrorCode::ParquetFileInvalid(err.to_string()))
        } else {
            // the end of file read by default is not long enough, read again including the metadata.
            let buffer = op
                .read_with(path)
                .range(file_size - footer_len..file_size)
                .await
                .map_err(|err| ErrorCode::ParquetFileInvalid(err.to_string()))?;
            let buffer = buffer.to_vec();
            ParquetMetaDataReader::decode_metadata(&buffer)
                .map_err(|err| ErrorCode::ParquetFileInvalid(err.to_string()))
        }
    }
}
