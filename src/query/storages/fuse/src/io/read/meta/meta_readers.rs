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

use std::io::Read;
use std::io::SeekFrom;
use std::sync::Arc;

use bytes::Buf;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::InMemoryItemCacheReader;
use databend_storages_common_cache::InMemoryLruCache;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_cache::Loader;
use databend_storages_common_index::BloomIndexMeta;
use databend_storages_common_index::InvertedIndexMeta;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::SegmentInfoVersion;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use databend_storages_common_table_meta::meta::SnapshotVersion;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use databend_storages_common_table_meta::meta::TableSnapshotStatisticsVersion;
use databend_storages_common_table_meta::readers::VersionedReader;
use futures::AsyncSeek;
use futures_util::AsyncSeekExt;
use opendal::Buffer;
use opendal::Operator;
use parquet::format::FileMetaData;
use parquet::thrift::TSerializable;

use self::thrift_file_meta_read::read_thrift_file_metadata;

pub type TableSnapshotStatisticsReader =
    InMemoryItemCacheReader<TableSnapshotStatistics, LoaderWrapper<Operator>>;
pub type BloomIndexMetaReader = InMemoryItemCacheReader<BloomIndexMeta, LoaderWrapper<Operator>>;
pub type TableSnapshotReader = InMemoryItemCacheReader<TableSnapshot, LoaderWrapper<Operator>>;
pub type CompactSegmentInfoReader =
    InMemoryItemCacheReader<CompactSegmentInfo, LoaderWrapper<(Operator, TableSchemaRef)>>;
pub type InvertedIndexMetaReader =
    InMemoryItemCacheReader<InvertedIndexMeta, LoaderWrapper<Operator>>;

pub struct MetaReaders;

impl MetaReaders {
    pub fn segment_info_reader(dal: Operator, schema: TableSchemaRef) -> CompactSegmentInfoReader {
        CompactSegmentInfoReader::new(
            CacheManager::instance().get_table_segment_cache(),
            LoaderWrapper((dal, schema)),
        )
    }

    pub fn segment_info_reader_without_cache(
        dal: Operator,
        schema: TableSchemaRef,
    ) -> CompactSegmentInfoReader {
        CompactSegmentInfoReader::new(
            Arc::new(None::<InMemoryLruCache<CompactSegmentInfo>>),
            LoaderWrapper((dal, schema)),
        )
    }

    pub fn table_snapshot_reader(dal: Operator) -> TableSnapshotReader {
        TableSnapshotReader::new(
            CacheManager::instance().get_table_snapshot_cache(),
            LoaderWrapper(dal),
        )
    }

    pub fn table_snapshot_reader_without_cache(dal: Operator) -> TableSnapshotReader {
        TableSnapshotReader::new(
            Arc::new(None::<InMemoryLruCache<TableSnapshot>>),
            LoaderWrapper(dal),
        )
    }

    pub fn table_snapshot_statistics_reader(dal: Operator) -> TableSnapshotStatisticsReader {
        TableSnapshotStatisticsReader::new(
            CacheManager::instance().get_table_snapshot_statistics_cache(),
            LoaderWrapper(dal),
        )
    }

    pub fn bloom_index_meta_reader(dal: Operator) -> BloomIndexMetaReader {
        BloomIndexMetaReader::new(
            CacheManager::instance().get_bloom_index_meta_cache(),
            LoaderWrapper(dal),
        )
    }

    pub fn inverted_index_meta_reader(dal: Operator) -> InvertedIndexMetaReader {
        InvertedIndexMetaReader::new(
            CacheManager::instance().get_inverted_index_meta_cache(),
            LoaderWrapper(dal),
        )
    }
}

// workaround for the orphan rules
// Loader and types of table meta data are all defined outside (of this crate)
pub struct LoaderWrapper<T>(T);

#[async_trait::async_trait]
impl Loader<TableSnapshot> for LoaderWrapper<Operator> {
    #[async_backtrace::framed]
    async fn load(&self, params: &LoadParams) -> Result<TableSnapshot> {
        let reader = bytes_reader(&self.0, params.location.as_str(), params.len_hint).await?;
        let version = SnapshotVersion::try_from(params.ver)?;
        version.read(reader.reader())
    }
}

#[async_trait::async_trait]
impl Loader<TableSnapshotStatistics> for LoaderWrapper<Operator> {
    #[async_backtrace::framed]
    async fn load(&self, params: &LoadParams) -> Result<TableSnapshotStatistics> {
        let version = TableSnapshotStatisticsVersion::try_from(params.ver)?;
        let reader = bytes_reader(&self.0, params.location.as_str(), params.len_hint).await?;
        version.read(reader.reader())
    }
}

#[async_trait::async_trait]
impl Loader<CompactSegmentInfo> for LoaderWrapper<(Operator, TableSchemaRef)> {
    #[async_backtrace::framed]
    async fn load(&self, params: &LoadParams) -> Result<CompactSegmentInfo> {
        let version = SegmentInfoVersion::try_from(params.ver)?;
        let LoaderWrapper((operator, schema)) = &self;
        let reader = bytes_reader(operator, params.location.as_str(), params.len_hint).await?;
        (version, schema.clone()).read(reader.reader())
    }
}

#[async_trait::async_trait]
impl Loader<BloomIndexMeta> for LoaderWrapper<Operator> {
    #[async_backtrace::framed]
    async fn load(&self, params: &LoadParams) -> Result<BloomIndexMeta> {
        // read the ThriftFileMetaData, omit unnecessary conversions
        let meta = read_thrift_file_metadata(self.0.clone(), &params.location, params.len_hint)
            .await
            .map_err(|err| {
                ErrorCode::StorageOther(format!(
                    "read file meta failed, {}, {:?}",
                    params.location, err
                ))
            })?;

        BloomIndexMeta::try_from(meta)
    }
}

#[async_trait::async_trait]
impl Loader<InvertedIndexMeta> for LoaderWrapper<Operator> {
    #[async_backtrace::framed]
    async fn load(&self, params: &LoadParams) -> Result<InvertedIndexMeta> {
        let operator = &self.0;
        let meta = operator.stat(&params.location).await.map_err(|err| {
            ErrorCode::StorageOther(format!(
                "read inverted index file meta failed, {}, {:?}",
                params.location, err
            ))
        })?;
        let file_size = meta.content_length();

        if file_size < 36 {
            return Err(ErrorCode::StorageOther(
                "inverted index file must contain a footer with at least 36 bytes",
            ));
        }
        let default_end_len = 36;

        // read the end of the file
        let data = operator
            .read_with(&params.location)
            .range(file_size - default_end_len as u64..file_size)
            .await
            .map_err(|err| {
                ErrorCode::StorageOther(format!(
                    "read file meta failed, {}, {:?}",
                    params.location, err
                ))
            })?;

        let mut buf = vec![0u8; 4];
        let mut reader = data.reader();
        let mut offsets = Vec::with_capacity(8);
        let fast_fields_offset = read_u32(&mut reader, buf.as_mut_slice())? as u64;
        let store_offset = read_u32(&mut reader, buf.as_mut_slice())? as u64;
        let field_norms_offset = read_u32(&mut reader, buf.as_mut_slice())? as u64;
        let positions_offset = read_u32(&mut reader, buf.as_mut_slice())? as u64;
        let postings_offset = read_u32(&mut reader, buf.as_mut_slice())? as u64;
        let terms_offset = read_u32(&mut reader, buf.as_mut_slice())? as u64;
        let meta_offset = read_u32(&mut reader, buf.as_mut_slice())? as u64;
        let managed_offset = read_u32(&mut reader, buf.as_mut_slice())? as u64;

        offsets.push(("fast".to_string(), fast_fields_offset));
        offsets.push(("store".to_string(), store_offset));
        offsets.push(("fieldnorm".to_string(), field_norms_offset));
        offsets.push(("pos".to_string(), positions_offset));
        offsets.push(("idx".to_string(), postings_offset));
        offsets.push(("term".to_string(), terms_offset));
        offsets.push(("meta.json".to_string(), meta_offset));
        offsets.push((".managed.json".to_string(), managed_offset));

        let mut prev_offset = 0;
        let mut columns = Vec::with_capacity(offsets.len());
        for (name, offset) in offsets.into_iter() {
            let column_meta = SingleColumnMeta {
                offset: prev_offset,
                len: offset - prev_offset,
                num_values: 1,
            };
            prev_offset = offset;
            columns.push((name, column_meta));
        }

        Ok(InvertedIndexMeta { columns })
    }
}

async fn bytes_reader(op: &Operator, path: &str, len_hint: Option<u64>) -> Result<Buffer> {
    let reader = if let Some(len) = len_hint {
        op.read_with(path).range(0..len).await?
    } else {
        op.read(path).await?
    };
    Ok(reader)
}

mod thrift_file_meta_read {
    use databend_common_arrow::parquet::error::Error;
    use thrift::protocol::TCompactInputProtocol;

    use super::*;

    const HEADER_SIZE: u64 = PARQUET_MAGIC.len() as u64;
    const FOOTER_SIZE: u64 = 8;
    const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];

    /// The number of bytes read at the end of the parquet file on first read
    const DEFAULT_FOOTER_READ_SIZE: u64 = 64 * 1024;

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
    ) -> Result<FileMetaData> {
        let file_size = if let Some(len) = len_hint {
            len
        } else {
            let meta = op
                .stat(path)
                .await
                .map_err(|err| Error::OutOfSpec(err.to_string()))?;
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
            .map_err(|err| Error::OutOfSpec(err.to_string()))?
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

            let mut prot = TCompactInputProtocol::new(&buffer[remaining..]);
            let meta = FileMetaData::read_from_in_protocol(&mut prot)
                .map_err(|err| ErrorCode::ParquetFileInvalid(err.to_string()))?;
            Ok(meta)
        } else {
            // the end of file read by default is not long enough, read again including the metadata.
            let buffer = op
                .read_with(path)
                .range(file_size - footer_len..file_size)
                .await
                .map_err(|err| ErrorCode::ParquetFileInvalid(err.to_string()))?;

            let mut prot = TCompactInputProtocol::new(buffer.reader());
            let meta = FileMetaData::read_from_in_protocol(&mut prot)
                .map_err(|err| ErrorCode::ParquetFileInvalid(err.to_string()))?;
            Ok(meta)
        }
    }
}

#[inline(always)]
fn read_u32<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<u32> {
    r.read_exact(buf)?;
    Ok(u32::from_le_bytes(buf.try_into().unwrap()))
}
