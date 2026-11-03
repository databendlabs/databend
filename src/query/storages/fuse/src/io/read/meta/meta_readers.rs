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
use databend_storages_common_cache::CacheDef;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::DiskCachedStorage;
use databend_storages_common_cache::DiskFill;
use databend_storages_common_cache::ReadCtx;
use databend_storages_common_cache::Reader;
use databend_storages_common_cache::kinds::bloom;
use databend_storages_common_cache::kinds::segment;
use databend_storages_common_cache::kinds::spatial;
use databend_storages_common_cache::kinds::table;
use databend_storages_common_cache::kinds::vector;
use databend_storages_common_cache::kinds::virtual_column;
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

pub use self::thrift_file_meta_read::read_cached_thrift_file_metadata;
pub use self::thrift_file_meta_read::read_thrift_file_metadata;
use crate::io::SnapshotKey;

// workaround for the orphan rules
// Loader and types of table meta data are all defined outside (of this crate)
/// Reads and decodes table snapshots, through the disk cache. No memory caching; pair with
/// `SnapshotCache`.
#[derive(Clone)]
pub struct SnapshotReader {
    storage: DiskCachedStorage,
}

impl SnapshotReader {
    pub fn new(operator: Operator) -> Self {
        SnapshotReader {
            storage: DiskCachedStorage::new(operator),
        }
    }
}

impl Reader for SnapshotReader {
    type Key = SnapshotKey;
    type Item = Arc<TableSnapshot>;

    fn cache_key<'a>(&self, key: &'a SnapshotKey) -> Cow<'a, str> {
        Cow::Borrowed(&key.0)
    }

    #[async_backtrace::framed]
    async fn read(
        &self,
        (location, ver): &SnapshotKey,
        ctx: &ReadCtx,
    ) -> Result<Arc<TableSnapshot>> {
        let version = SnapshotVersion::try_from(*ver)?;
        let bytes = read_whole::<table::Snapshot>(&self.storage, location, ctx.disk).await?;
        Ok(Arc::new(version.read(bytes.reader())?))
    }
}

/// Reads and decodes table snapshot statistics. No caching; pair with `SnapshotCache`.
#[derive(Clone)]
pub struct SnapshotStatsReader {
    storage: DiskCachedStorage,
}

impl SnapshotStatsReader {
    pub fn new(operator: Operator) -> Self {
        SnapshotStatsReader {
            storage: DiskCachedStorage::new(operator),
        }
    }
}

impl Reader for SnapshotStatsReader {
    type Key = SnapshotKey;
    type Item = Arc<TableSnapshotStatistics>;

    fn cache_key<'a>(&self, key: &'a SnapshotKey) -> Cow<'a, str> {
        Cow::Borrowed(&key.0)
    }

    #[async_backtrace::framed]
    async fn read(
        &self,
        (location, ver): &SnapshotKey,
        ctx: &ReadCtx,
    ) -> Result<Arc<TableSnapshotStatistics>> {
        let version = TableSnapshotStatisticsVersion::try_from(*ver)?;
        let bytes = read_whole::<table::SnapshotStats>(&self.storage, location, ctx.disk).await?;
        Ok(Arc::new(version.read(bytes.reader())?))
    }
}

/// Reads and decodes row-oriented segments. No caching; pair with `SegmentCache`.
#[derive(Clone)]
pub struct SegmentInfoReader {
    storage: DiskCachedStorage,
    schema: TableSchemaRef,
}

impl SegmentInfoReader {
    pub fn new(operator: Operator, schema: TableSchemaRef) -> Self {
        SegmentInfoReader {
            storage: DiskCachedStorage::new(operator),
            schema,
        }
    }
}

impl Reader for SegmentInfoReader {
    type Key = Location;
    type Item = Arc<CompactSegmentInfo>;

    fn cache_key<'a>(&self, key: &'a Location) -> Cow<'a, str> {
        Cow::Borrowed(&key.0)
    }

    #[async_backtrace::framed]
    async fn read(
        &self,
        (location, ver): &Location,
        ctx: &ReadCtx,
    ) -> Result<Arc<CompactSegmentInfo>> {
        let version = SegmentInfoVersion::try_from(*ver)?;
        let bytes = read_whole::<segment::Info>(&self.storage, location, ctx.disk).await?;
        Ok(Arc::new(
            (version, self.schema.clone()).read(bytes.reader())?,
        ))
    }
}

/// Reads and decodes segment statistics. No caching; pair with `SegmentCache`.
#[derive(Clone)]
pub struct SegmentStatsReader {
    storage: DiskCachedStorage,
}

impl SegmentStatsReader {
    pub fn new(operator: Operator) -> Self {
        SegmentStatsReader {
            storage: DiskCachedStorage::new(operator),
        }
    }
}

impl Reader for SegmentStatsReader {
    type Key = Location;
    type Item = Arc<SegmentStatistics>;

    fn cache_key<'a>(&self, key: &'a Location) -> Cow<'a, str> {
        Cow::Borrowed(&key.0)
    }

    #[async_backtrace::framed]
    async fn read(
        &self,
        (location, ver): &Location,
        ctx: &ReadCtx,
    ) -> Result<Arc<SegmentStatistics>> {
        let version = SegmentStatisticsVersion::try_from(*ver)?;
        let bytes = read_whole::<segment::Stats>(&self.storage, location, ctx.disk).await?;
        Ok(Arc::new(version.read(bytes.reader())?))
    }
}

/// Reads and decodes the parquet footer of an index file as `T`. No caching; pair with
/// `FuseCaches::index_metas`. The key is the path and, when known, the file length.
pub struct IndexMetaReader<T> {
    storage: DiskCachedStorage,
    _meta: PhantomData<fn() -> T>,
}

impl<T> IndexMetaReader<T> {
    pub fn new(operator: Operator) -> Self {
        IndexMetaReader {
            storage: DiskCachedStorage::new(operator),
            _meta: PhantomData,
        }
    }

    async fn read_footer(
        &self,
        kind: CacheKind,
        (path, len): &IndexMetaKey,
        fill: DiskFill,
    ) -> Result<ParquetMetaData> {
        read_cached_thrift_file_metadata(&self.storage, kind, path, *len, fill)
            .await
            .map_err(|err| {
                ErrorCode::StorageOther(format!("read file meta failed, {}, {:?}", path, err))
            })
    }
}

pub type IndexMetaKey = (String, Option<u64>);

macro_rules! index_meta_reader {
    ($ty:ty, $kind:ty, |$meta:ident| $decode:expr) => {
        impl Reader for IndexMetaReader<$ty> {
            type Key = IndexMetaKey;
            type Item = Arc<$ty>;

            fn cache_key<'a>(&self, key: &'a IndexMetaKey) -> Cow<'a, str> {
                Cow::Borrowed(&key.0)
            }

            #[async_backtrace::framed]
            async fn read(&self, key: &IndexMetaKey, ctx: &ReadCtx) -> Result<Arc<$ty>> {
                let $meta = self
                    .read_footer(CacheKind::of::<$kind>(), key, ctx.disk)
                    .await?;
                Ok(Arc::new($decode))
            }
        }
    };
}

index_meta_reader!(BloomIndexMeta, bloom::IndexMeta, |meta| {
    BloomIndexMeta::try_from(meta)?
});
index_meta_reader!(SpatialIndexMeta, spatial::Meta, |meta| {
    SpatialIndexMeta::try_from(meta)?
});
index_meta_reader!(VectorIndexMeta, vector::Meta, |meta| {
    VectorIndexMeta::Legacy(SpatialIndexMeta::try_from(meta)?)
});
index_meta_reader!(VirtualColumnFileMeta, virtual_column::Meta, |meta| {
    VirtualColumnFileMeta::try_from(meta)?
});

/// The whole object at `path`, through the disk cache under the kind `D`.
pub async fn read_whole<D: CacheDef>(
    storage: &DiskCachedStorage,
    path: &str,
    fill: DiskFill,
) -> Result<bytes::Bytes> {
    storage.read(CacheKind::of::<D>(), path, None, fill).await
}

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
        read_footer(&op, path, len_hint, |range| {
            let op = op.clone();
            async move {
                let buffer = op
                    .read_with(path)
                    .range(range)
                    .await
                    .map_err(|err| ParquetError::General(err.to_string()))?;
                Ok(buffer.to_bytes())
            }
        })
        .await
    }

    /// `read_thrift_file_metadata` through the disk cache: the tail and the footer of the file
    /// are its units, under `kind`.
    #[async_backtrace::framed]
    pub async fn read_cached_thrift_file_metadata(
        storage: &DiskCachedStorage,
        kind: CacheKind,
        path: &str,
        len_hint: Option<u64>,
        fill: DiskFill,
    ) -> Result<ParquetMetaData> {
        read_footer(storage.operator(), path, len_hint, |range| async move {
            storage.read(kind, path, Some(range), fill).await
        })
        .await
    }

    async fn read_footer<'a, F, Fut>(
        op: &Operator,
        path: &'a str,
        len_hint: Option<u64>,
        read_range: F,
    ) -> Result<ParquetMetaData>
    where
        F: Fn(std::ops::Range<u64>) -> Fut,
        Fut: std::future::Future<Output = Result<bytes::Bytes>> + 'a,
    {
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
        let buffer = read_range(file_size - default_end_len as u64..file_size).await?;

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
            let buffer = read_range(file_size - footer_len..file_size).await?;
            ParquetMetaDataReader::decode_metadata(&buffer)
                .map_err(|err| ErrorCode::ParquetFileInvalid(err.to_string()))
        }
    }
}
