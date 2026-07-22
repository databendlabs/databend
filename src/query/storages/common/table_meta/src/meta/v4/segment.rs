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

use std::io::Cursor;
use std::io::Read;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_frozen_api::FrozenAPI;
use databend_common_frozen_api::frozen_api;
use serde::Deserialize;
use serde::Serialize;

use super::super::v2;
use super::super::v3;
use crate::meta::FormatVersion;
use crate::meta::MetaEncoding;
use crate::meta::Statistics;
use crate::meta::Versioned;
use crate::meta::format::MAX_SEGMENT_BLOCK_NUMBER;
use crate::meta::format::MetaCompression;
use crate::meta::format::SegmentHeader;
use crate::meta::format::compress;
use crate::meta::format::decode_segment_header;
use crate::meta::format::encode;
use crate::meta::format::read_and_deserialize;
use crate::meta::v2::BlockMeta;

/// A segment comprises one or more blocks
/// The structure of the segment is the same as that of v2, but the serialization and deserialization methods are different
#[frozen_api("2c973019")]
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, FrozenAPI)]
pub struct SegmentInfo {
    /// format version of SegmentInfo table meta data
    ///
    /// Note that:
    ///
    /// - A instance of v3::SegmentInfo may have a value of v2/v1::SegmentInfo::VERSION for this field.
    ///
    ///   That indicates this instance is converted from a v2/v1::SegmentInfo.
    ///
    /// - The meta writers are responsible for only writing down the latest version of SegmentInfo, and
    ///   the format_version being written is of the latest version.
    ///
    ///   e.g. if the current version of SegmentInfo is v3::SegmentInfo, then the format_version
    ///   that will be written down to object storage as part of SegmentInfo table meta data,
    ///   should always be v3::SegmentInfo::VERSION (which is 3)
    pub format_version: FormatVersion,
    /// blocks belong to this segment
    pub blocks: Vec<Arc<BlockMeta>>,
    /// summary statistics
    pub summary: Statistics,
}

impl SegmentInfo {
    pub fn new(blocks: Vec<Arc<BlockMeta>>, summary: Statistics) -> Self {
        assert!(
            blocks.len() <= MAX_SEGMENT_BLOCK_NUMBER,
            "number of block overflow: {},  max number allowed {}",
            blocks.len(),
            MAX_SEGMENT_BLOCK_NUMBER,
        );

        Self {
            format_version: SegmentInfo::VERSION,
            blocks,
            summary,
        }
    }

    // Total block bytes of this segment.
    pub fn total_bytes(&self) -> u64 {
        self.summary.uncompressed_byte_size
    }

    // Encode self.blocks as RawBlockMeta.
    fn block_raw_bytes(&self) -> Result<RawBlockMeta> {
        let encoding = MetaEncoding::MessagePack;
        let bytes = encode(&encoding, &self.blocks)?;

        let compression = MetaCompression::default();
        let compressed = compress(&compression, bytes)?;

        Ok(RawBlockMeta {
            bytes: compressed,
            encoding,
            compression,
        })
    }
}

// use the chain of converters, for versions before v3
impl<T> From<T> for SegmentInfo
where T: Into<v2::SegmentInfo>
{
    fn from(value: T) -> Self {
        Self::from_v2(value.into())
    }
}

impl From<v3::SegmentInfo> for SegmentInfo {
    fn from(value: v3::SegmentInfo) -> Self {
        Self::from_v3(value)
    }
}

impl SegmentInfo {
    pub fn from_v3(s: v3::SegmentInfo) -> Self {
        // NOTE: it is important to let the format_version return from here
        // carries the format_version of segment info being converted.
        Self {
            format_version: s.format_version,
            blocks: s.blocks.into_iter().map(|v| Arc::new(v.into())).collect(),
            summary: s.summary.into(),
        }
    }
    pub fn from_v2(s: v2::SegmentInfo) -> Self {
        // NOTE: it is important to let the format_version return from here
        // carries the format_version of segment info being converted.
        Self {
            format_version: s.format_version,
            blocks: s.blocks,
            summary: s.summary,
        }
    }

    /// Serializes the Segment struct to a byte vector.
    ///
    /// The byte vector contains the format version, encoding, compression, and compressed block data and
    /// summary data. The encoding and compression are set to default values. The block data and summary
    /// data are encoded and compressed, respectively.
    ///
    /// # Returns
    ///
    /// A Result containing the serialized Segment data as a byte vector. If any errors occur during
    /// encoding, compression, or writing to the byte vector, an error will be returned.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.to_bytes_with_encoding(MetaEncoding::MessagePack)
    }

    fn to_bytes_with_encoding(&self, encoding: MetaEncoding) -> Result<Vec<u8>> {
        let compression = MetaCompression::default();

        let blocks = encode(&encoding, &self.blocks)?;
        let blocks_compress = compress(&compression, blocks)?;

        let summary = encode(&encoding, &self.summary)?;
        let summary_compress = compress(&compression, summary)?;

        let data_size = self.format_version.to_le_bytes().len()
            + 2
            + blocks_compress.len().to_le_bytes().len()
            + blocks_compress.len()
            + summary_compress.len().to_le_bytes().len()
            + summary_compress.len();
        let mut buf = Vec::with_capacity(data_size);

        buf.extend_from_slice(&self.format_version.to_le_bytes());
        buf.push(encoding as u8);
        buf.push(compression as u8);
        buf.extend_from_slice(&blocks_compress.len().to_le_bytes());
        buf.extend_from_slice(&summary_compress.len().to_le_bytes());

        buf.extend(blocks_compress);
        buf.extend(summary_compress);

        Ok(buf)
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let SegmentHeader {
            version,
            encoding,
            compression,
            blocks_size,
            summary_size,
        } = decode_segment_header(&mut cursor)?;

        let blocks = read_block_metas(&mut cursor, blocks_size, &encoding, &compression)?;
        let summary = read_statistics(&mut cursor, summary_size, &encoding, &compression)?;

        let mut segment = Self::new(blocks, summary);

        // bytes may represent an encoded v[n]::SegmentInfo, where n <= self::SegmentInfo::VERSION
        // please see PR https://github.com/datafuselabs/databend/pull/11211 for the adjustment of
        // format_version`'s "semantic"
        segment.format_version = version;
        Ok(segment)
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, FrozenAPI)]
pub struct RawBlockMeta {
    pub bytes: Vec<u8>,
    pub encoding: MetaEncoding,
    pub compression: MetaCompression,
}

impl RawBlockMeta {
    pub fn block_metas(&self) -> Result<Vec<Arc<BlockMeta>>> {
        let mut reader = Cursor::new(&self.bytes);
        read_block_metas(
            &mut reader,
            self.bytes.len() as u64,
            &self.encoding,
            &self.compression,
        )
    }
}

#[frozen_api("1b3fc937")]
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone, FrozenAPI)]
pub struct CompactSegmentInfo {
    pub format_version: FormatVersion,
    pub summary: Statistics,
    pub raw_block_metas: RawBlockMeta,
}

impl CompactSegmentInfo {
    pub fn from_reader(mut r: impl Read) -> Result<Self> {
        let SegmentHeader {
            version,
            encoding,
            compression,
            blocks_size,
            summary_size,
        } = decode_segment_header(&mut r)?;

        let mut block_metas_raw_bytes = vec![0; blocks_size as usize];
        r.read_exact(&mut block_metas_raw_bytes)?;

        let summary = read_statistics(&mut r, summary_size, &encoding, &compression)?;

        let segment = CompactSegmentInfo {
            format_version: version,
            summary,
            raw_block_metas: RawBlockMeta {
                bytes: block_metas_raw_bytes,
                encoding,
                compression,
            },
        };
        Ok(segment)
    }

    pub fn block_metas(&self) -> Result<Vec<Arc<BlockMeta>>> {
        self.raw_block_metas.block_metas()
    }
}

fn read_block_metas<R>(
    reader: &mut R,
    size: u64,
    encoding: &MetaEncoding,
    compression: &MetaCompression,
) -> Result<Vec<Arc<BlockMeta>>>
where
    R: Read,
{
    if matches!(encoding, MetaEncoding::Bincode) {
        let blocks: Vec<v3::frozen::BlockMeta> =
            read_and_deserialize(reader, size, encoding, compression)?;
        return Ok(blocks
            .into_iter()
            .map(|block| Arc::new(block.into()))
            .collect());
    }

    read_and_deserialize(reader, size, encoding, compression)
}

fn read_statistics<R>(
    reader: &mut R,
    size: u64,
    encoding: &MetaEncoding,
    compression: &MetaCompression,
) -> Result<Statistics>
where
    R: Read,
{
    if matches!(encoding, MetaEncoding::Bincode) {
        let statistics: v3::frozen::Statistics =
            read_and_deserialize(reader, size, encoding, compression)?;
        return Ok(statistics.into());
    }

    read_and_deserialize(reader, size, encoding, compression)
}

impl TryFrom<Arc<CompactSegmentInfo>> for SegmentInfo {
    type Error = ErrorCode;
    fn try_from(value: Arc<CompactSegmentInfo>) -> std::result::Result<Self, Self::Error> {
        let blocks = value.block_metas()?;
        Ok(SegmentInfo {
            format_version: value.format_version,
            blocks,
            summary: value.summary.clone(),
        })
    }
}

impl TryFrom<&CompactSegmentInfo> for SegmentInfo {
    type Error = ErrorCode;
    fn try_from(value: &CompactSegmentInfo) -> std::result::Result<Self, Self::Error> {
        let blocks = value.block_metas()?;
        Ok(SegmentInfo {
            format_version: value.format_version,
            blocks,
            summary: value.summary.clone(),
        })
    }
}

impl TryFrom<&SegmentInfo> for CompactSegmentInfo {
    type Error = ErrorCode;

    fn try_from(value: &SegmentInfo) -> std::result::Result<Self, Self::Error> {
        let bytes = value.block_raw_bytes()?;
        Ok(Self {
            format_version: value.format_version,
            summary: value.summary.clone(),
            raw_block_metas: bytes,
        })
    }
}

impl TryFrom<SegmentInfo> for CompactSegmentInfo {
    type Error = ErrorCode;

    fn try_from(value: SegmentInfo) -> std::result::Result<Self, Self::Error> {
        let bytes = value.block_raw_bytes()?;
        Ok(Self {
            format_version: value.format_version,
            summary: value.summary,
            raw_block_metas: bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_column::types::timestamp_tz;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::DecimalScalar;
    use databend_common_expression::types::DecimalSize;

    use super::*;
    use crate::meta::ColumnMeta;
    use crate::meta::ColumnStatistics;
    use crate::meta::Compression;

    #[allow(dead_code)]
    #[derive(Serialize)]
    enum OldLegacyScalar {
        Null,
        EmptyArray,
        EmptyMap,
        Number(databend_common_expression::types::NumberScalar),
        Decimal(DecimalScalar),
        Timestamp(i64),
        Date(i32),
        Boolean(bool),
        String(Vec<u8>),
        Array(()),
        Map(()),
        Bitmap(Vec<u8>),
        Tuple(Vec<Scalar>),
        Variant(Vec<u8>),
    }

    #[derive(Serialize)]
    struct OldColumnStatistics {
        min: OldLegacyScalar,
        max: OldLegacyScalar,
        null_count: u64,
        in_memory_size: u64,
        distinct_of_values: Option<u64>,
    }

    #[derive(Serialize)]
    struct OldBlockMeta {
        row_count: u64,
        block_size: u64,
        file_size: u64,
        col_stats: HashMap<u32, OldColumnStatistics>,
        col_metas: HashMap<u32, v3::frozen::ColumnMeta>,
        cluster_stats: Option<()>,
        location: (String, u64),
        bloom_filter_index_location: Option<(String, u64)>,
        bloom_filter_index_size: u64,
        compression: v3::frozen::Compression,
    }

    #[derive(Serialize)]
    struct OldStatistics {
        row_count: u64,
        block_count: u64,
        perfect_block_count: u64,
        uncompressed_byte_size: u64,
        compressed_byte_size: u64,
        index_size: u64,
        col_stats: HashMap<u32, OldColumnStatistics>,
    }

    fn old_legacy_col_stats() -> HashMap<u32, OldColumnStatistics> {
        let mut col_stats = HashMap::new();
        col_stats.insert(1, OldColumnStatistics {
            min: OldLegacyScalar::String(b"aaa".to_vec()),
            max: OldLegacyScalar::String(b"zzz".to_vec()),
            null_count: 0,
            in_memory_size: 6,
            distinct_of_values: Some(2),
        });
        col_stats
    }

    fn legacy_segment_bytes() -> Result<Vec<u8>> {
        let block_col_stats = old_legacy_col_stats();
        let summary_col_stats = old_legacy_col_stats();

        let mut col_metas = HashMap::new();
        col_metas.insert(
            1,
            v3::frozen::ColumnMeta::Parquet(v3::frozen::ParquetColumnMeta {
                offset: 0,
                len: 16,
                num_values: 3,
            }),
        );

        let blocks = vec![OldBlockMeta {
            row_count: 3,
            block_size: 16,
            file_size: 16,
            col_stats: block_col_stats,
            col_metas,
            cluster_stats: None,
            location: ("block.parquet".to_string(), 0),
            bloom_filter_index_location: None,
            bloom_filter_index_size: 0,
            compression: v3::frozen::Compression::None,
        }];

        let summary = OldStatistics {
            row_count: 3,
            block_count: 1,
            perfect_block_count: 1,
            uncompressed_byte_size: 16,
            compressed_byte_size: 16,
            index_size: 0,
            col_stats: summary_col_stats,
        };

        let encoding = MetaEncoding::Bincode;
        let compression = MetaCompression::default();
        let blocks = compress(&compression, encode(&encoding, &blocks)?)?;
        let summary = compress(&compression, encode(&encoding, &summary)?)?;

        let mut buf = Vec::new();
        buf.extend_from_slice(&SegmentInfo::VERSION.to_le_bytes());
        buf.push(MetaEncoding::Bincode as u8);
        buf.push(MetaCompression::default() as u8);
        buf.extend_from_slice(&blocks.len().to_le_bytes());
        buf.extend_from_slice(&summary.len().to_le_bytes());
        buf.extend(blocks);
        buf.extend(summary);
        Ok(buf)
    }

    fn current_col_stats() -> HashMap<u32, ColumnStatistics> {
        let mut col_stats = HashMap::new();
        col_stats.insert(
            1,
            ColumnStatistics::new(
                Scalar::String("aaa".to_string()),
                Scalar::String("zzz".to_string()),
                0,
                6,
                Some(2),
            ),
        );
        col_stats.insert(
            2,
            ColumnStatistics::new(
                Scalar::TimestampTz(timestamp_tz::new(1000, 0)),
                Scalar::TimestampTz(timestamp_tz::new(2000, 0)),
                0,
                16,
                Some(2),
            ),
        );
        col_stats.insert(
            3,
            ColumnStatistics::new(
                Scalar::Decimal(DecimalScalar::Decimal64(
                    123,
                    DecimalSize::new_unchecked(10, 2),
                )),
                Scalar::Decimal(DecimalScalar::Decimal64(
                    456,
                    DecimalSize::new_unchecked(10, 2),
                )),
                0,
                16,
                Some(2),
            ),
        );
        col_stats
    }

    fn current_segment() -> SegmentInfo {
        let block_col_stats = current_col_stats();
        let summary_col_stats = current_col_stats();

        let mut col_metas = HashMap::new();
        for column_id in 1..=3 {
            col_metas.insert(
                column_id,
                ColumnMeta::Parquet(crate::meta::v0::ColumnMeta {
                    offset: 0,
                    len: 16,
                    num_values: 3,
                }),
            );
        }

        let blocks = vec![Arc::new(BlockMeta {
            row_count: 3,
            block_size: 16,
            file_size: 16,
            col_stats: block_col_stats,
            col_metas,
            cluster_stats: None,
            location: ("block.parquet".to_string(), 0),
            bloom_filter_index_location: None,
            bloom_filter_index_size: 0,
            inverted_index_size: None,
            ngram_filter_index_size: None,
            vector_index_size: None,
            vector_index_location: None,
            spatial_index_size: None,
            spatial_index_location: None,
            spatial_stats: None,
            vector_stats: None,
            virtual_block_meta: None,
            compression: Compression::None,
            create_on: None,
        })];

        SegmentInfo::new(blocks, Statistics {
            row_count: 3,
            block_count: 1,
            perfect_block_count: 1,
            uncompressed_byte_size: 16,
            compressed_byte_size: 16,
            index_size: 0,
            col_stats: summary_col_stats,
            ..Default::default()
        })
    }

    #[test]
    fn test_read_bincode_segment_with_v3_frozen_meta() -> Result<()> {
        let bytes = legacy_segment_bytes()?;
        let segment = SegmentInfo::from_slice(&bytes)?;

        assert_eq!(segment.format_version, SegmentInfo::VERSION);
        assert_eq!(segment.summary.row_count, 3);
        assert_eq!(
            segment.summary.col_stats[&1].min,
            Scalar::String("aaa".to_string())
        );
        assert_eq!(segment.blocks.len(), 1);

        let block = &segment.blocks[0];
        assert_eq!(block.row_count, 3);
        assert_eq!(block.location.0, "block.parquet");
        assert!(block.col_metas[&1].as_parquet().is_some());
        assert_eq!(block.col_stats[&1].max, Scalar::String("zzz".to_string()));
        Ok(())
    }

    #[test]
    fn test_compact_segment_reads_bincode_blocks_lazily_with_v3_frozen_meta() -> Result<()> {
        let bytes = legacy_segment_bytes()?;
        let segment = CompactSegmentInfo::from_reader(Cursor::new(bytes))?;

        assert_eq!(segment.summary.row_count, 3);
        assert_eq!(
            segment.summary.col_stats[&1].min,
            Scalar::String("aaa".to_string())
        );

        let blocks = segment.block_metas()?;
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].row_count, 3);
        assert_eq!(
            blocks[0].col_stats[&1].max,
            Scalar::String("zzz".to_string())
        );
        Ok(())
    }

    #[test]
    fn test_read_current_messagepack_segment() -> Result<()> {
        let bytes = current_segment().to_bytes()?;
        let segment = SegmentInfo::from_slice(&bytes)?;

        assert_eq!(segment.format_version, SegmentInfo::VERSION);
        assert_eq!(segment.summary.row_count, 3);
        assert_eq!(
            segment.summary.col_stats[&2].max,
            Scalar::TimestampTz(timestamp_tz::new(2000, 0))
        );
        assert_eq!(
            segment.summary.col_stats[&3].min,
            Scalar::Decimal(DecimalScalar::Decimal64(
                123,
                DecimalSize::new_unchecked(10, 2)
            ))
        );

        assert_eq!(segment.blocks.len(), 1);
        assert_eq!(
            segment.blocks[0].col_stats[&3].max,
            Scalar::Decimal(DecimalScalar::Decimal64(
                456,
                DecimalSize::new_unchecked(10, 2)
            ))
        );
        Ok(())
    }

    #[test]
    fn test_compact_segment_reads_current_messagepack_blocks_lazily() -> Result<()> {
        let bytes = current_segment().to_bytes()?;
        let segment = CompactSegmentInfo::from_reader(Cursor::new(bytes))?;

        assert_eq!(segment.raw_block_metas.encoding, MetaEncoding::MessagePack);
        assert_eq!(segment.summary.row_count, 3);
        assert_eq!(
            segment.summary.col_stats[&2].min,
            Scalar::TimestampTz(timestamp_tz::new(1000, 0))
        );

        let blocks = segment.block_metas()?;
        assert_eq!(blocks.len(), 1);
        assert_eq!(
            blocks[0].col_stats[&3].max,
            Scalar::Decimal(DecimalScalar::Decimal64(
                456,
                DecimalSize::new_unchecked(10, 2)
            ))
        );
        Ok(())
    }
}
