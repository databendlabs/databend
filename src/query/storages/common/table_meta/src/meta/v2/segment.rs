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

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::ColumnId;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::NumberDataType;
use databend_common_frozen_api::FrozenAPI;
use databend_common_frozen_api::frozen_api;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::BlockHLLState;
use crate::meta::BlockTopN;
use crate::meta::ClusterStatistics;
use crate::meta::ColumnStatistics;
use crate::meta::Compression;
use crate::meta::FormatVersion;
use crate::meta::Location;
use crate::meta::PartitionStatistics;
use crate::meta::SpatialStatistics;
use crate::meta::Statistics;
use crate::meta::StatisticsOfVectorColumns;
use crate::meta::Versioned;
use crate::meta::v0;
use crate::meta::v1;

/// A segment comprises one or more blocks
#[frozen_api("e19aba63")]
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, FrozenAPI)]
pub struct SegmentInfo {
    /// format version
    pub format_version: FormatVersion,
    /// blocks belong to this segment
    pub blocks: Vec<Arc<BlockMeta>>,
    /// summary statistics
    pub summary: Statistics,
}

impl SegmentInfo {
    // for test.
    pub fn new(blocks: Vec<Arc<BlockMeta>>, summary: Statistics) -> Self {
        Self {
            format_version: SegmentInfo::VERSION,
            blocks,
            summary,
        }
    }
}

// The virtual column variant types
const VIRTUAL_COLUMN_JSONB_TYPE: u8 = 0;
const VIRTUAL_COLUMN_BOOL_TYPE: u8 = 1;
const VIRTUAL_COLUMN_UINT64_TYPE: u8 = 2;
const VIRTUAL_COLUMN_INT64_TYPE: u8 = 3;
const VIRTUAL_COLUMN_FLOAT64_TYPE: u8 = 4;
const VIRTUAL_COLUMN_STRING_TYPE: u8 = 5;
const VIRTUAL_COLUMN_BINARY_TYPE: u8 = 6;
const VIRTUAL_COLUMN_DATE_TYPE: u8 = 7;
const VIRTUAL_COLUMN_TIMESTAMP_TYPE: u8 = 8;
const VIRTUAL_COLUMN_TIMESTAMP_TZ_TYPE: u8 = 9;
const VIRTUAL_COLUMN_INTERVAL_TYPE: u8 = 10;
const VIRTUAL_COLUMN_UINT8_TYPE: u8 = 11;
const VIRTUAL_COLUMN_UINT16_TYPE: u8 = 12;
const VIRTUAL_COLUMN_UINT32_TYPE: u8 = 13;
const VIRTUAL_COLUMN_INT8_TYPE: u8 = 14;
const VIRTUAL_COLUMN_INT16_TYPE: u8 = 15;
const VIRTUAL_COLUMN_INT32_TYPE: u8 = 16;
const VIRTUAL_COLUMN_FLOAT32_TYPE: u8 = 17;
const VIRTUAL_COLUMN_EXTENDED_TYPE: u8 = u8::MAX;

/// Physical data type of a materialized virtual column.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, FrozenAPI)]
pub enum VirtualColumnPhysicalType {
    Jsonb,
    Boolean,
    Number(NumberDataType),
    Decimal(DecimalDataType),
    String,
    Binary,
    Date,
    Timestamp,
    TimestampTz,
    Interval,
    Array(Box<VirtualColumnPhysicalType>),
}

impl VirtualColumnPhysicalType {
    pub fn try_from_data_type(
        data_type: &databend_common_expression::types::DataType,
    ) -> Result<Self> {
        use databend_common_expression::types::DataType;

        match data_type.remove_nullable() {
            DataType::Variant => Ok(Self::Jsonb),
            DataType::Boolean => Ok(Self::Boolean),
            DataType::Number(number) => Ok(Self::Number(number)),
            DataType::Decimal(size) => Ok(Self::Decimal(size.into())),
            DataType::String => Ok(Self::String),
            DataType::Binary => Ok(Self::Binary),
            DataType::Date => Ok(Self::Date),
            DataType::Timestamp => Ok(Self::Timestamp),
            DataType::TimestampTz => Ok(Self::TimestampTz),
            DataType::Interval => Ok(Self::Interval),
            DataType::Array(inner) => Ok(Self::Array(Box::new(Self::try_from_data_type(&inner)?))),
            unsupported => Err(ErrorCode::Internal(format!(
                "unsupported virtual column physical type: {unsupported:?}"
            ))),
        }
    }

    pub fn table_data_type(&self) -> TableDataType {
        match self {
            Self::Jsonb => TableDataType::Variant,
            Self::Boolean => TableDataType::Boolean,
            Self::Number(number) => TableDataType::Number(*number),
            Self::Decimal(decimal) => TableDataType::Decimal(*decimal),
            Self::String => TableDataType::String,
            Self::Binary => TableDataType::Binary,
            Self::Date => TableDataType::Date,
            Self::Timestamp => TableDataType::Timestamp,
            Self::TimestampTz => TableDataType::TimestampTz,
            Self::Interval => TableDataType::Interval,
            Self::Array(inner) => TableDataType::Array(Box::new(inner.table_data_type())),
        }
    }

    pub fn encode(&self) -> (u8, Option<Self>) {
        match self {
            Self::Jsonb => (VIRTUAL_COLUMN_JSONB_TYPE, None),
            Self::Boolean => (VIRTUAL_COLUMN_BOOL_TYPE, None),
            Self::Number(NumberDataType::UInt64) => (VIRTUAL_COLUMN_UINT64_TYPE, None),
            Self::Number(NumberDataType::UInt8) => (VIRTUAL_COLUMN_UINT8_TYPE, None),
            Self::Number(NumberDataType::UInt16) => (VIRTUAL_COLUMN_UINT16_TYPE, None),
            Self::Number(NumberDataType::UInt32) => (VIRTUAL_COLUMN_UINT32_TYPE, None),
            Self::Number(NumberDataType::Int64) => (VIRTUAL_COLUMN_INT64_TYPE, None),
            Self::Number(NumberDataType::Int8) => (VIRTUAL_COLUMN_INT8_TYPE, None),
            Self::Number(NumberDataType::Int16) => (VIRTUAL_COLUMN_INT16_TYPE, None),
            Self::Number(NumberDataType::Int32) => (VIRTUAL_COLUMN_INT32_TYPE, None),
            Self::Number(NumberDataType::Float64) => (VIRTUAL_COLUMN_FLOAT64_TYPE, None),
            Self::Number(NumberDataType::Float32) => (VIRTUAL_COLUMN_FLOAT32_TYPE, None),
            Self::String => (VIRTUAL_COLUMN_STRING_TYPE, None),
            Self::Binary => (VIRTUAL_COLUMN_BINARY_TYPE, None),
            Self::Date => (VIRTUAL_COLUMN_DATE_TYPE, None),
            Self::Timestamp => (VIRTUAL_COLUMN_TIMESTAMP_TYPE, None),
            Self::TimestampTz => (VIRTUAL_COLUMN_TIMESTAMP_TZ_TYPE, None),
            Self::Interval => (VIRTUAL_COLUMN_INTERVAL_TYPE, None),
            physical_type => (VIRTUAL_COLUMN_EXTENDED_TYPE, Some(physical_type.clone())),
        }
    }
}

/// The column meta of virtual columns.
/// Virtual column is the internal field values extracted from variant type values,
/// used to speed up the reading of internal fields of variant data.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct VirtualColumnMeta {
    /// where the data of column start
    pub offset: u64,
    /// the length of the column
    pub len: u64,
    /// num of "rows"
    pub num_values: u64,
    /// the type of virtual column in a block
    // To make BlockMeta more compatible, use numbers to represent variant types
    // 0 => jsonb
    // 1 => bool
    // 2 => uint64
    // 3 => int64
    // 4 => float64
    // 5 => string
    // 6 => binary
    // 7 => date
    // 8 => timestamp
    // 9 => timestamp_tz
    // 10 => interval
    // 11 => uint8
    // 12 => uint16
    // 13 => uint32
    // 14 => int8
    // 15 => int16
    // 16 => int32
    // 17 => float32
    // 255 => extended types(decimal, array)
    pub data_type: u8,
    /// Full type information for types that cannot be represented by
    /// the `data_type` code, such as Decimal with precision and scale.
    /// Simple types continue to use `data_type` and leave this field as `None`.
    #[serde(default)]
    pub extended_physical_type: Option<VirtualColumnPhysicalType>,
    /// virtual column statistics.
    pub column_stat: Option<ColumnStatistics>,
}

impl VirtualColumnMeta {
    pub fn total_rows(&self) -> usize {
        self.num_values as usize
    }

    pub fn offset_length(&self) -> (u64, u64) {
        (self.offset, self.len)
    }

    pub fn data_type(&self) -> TableDataType {
        let data_type = self.physical_type().table_data_type();
        TableDataType::Nullable(Box::new(data_type))
    }

    pub fn physical_type(&self) -> VirtualColumnPhysicalType {
        match self.data_type {
            VIRTUAL_COLUMN_JSONB_TYPE => VirtualColumnPhysicalType::Jsonb,
            VIRTUAL_COLUMN_BOOL_TYPE => VirtualColumnPhysicalType::Boolean,
            VIRTUAL_COLUMN_UINT64_TYPE => VirtualColumnPhysicalType::Number(NumberDataType::UInt64),
            VIRTUAL_COLUMN_UINT8_TYPE => VirtualColumnPhysicalType::Number(NumberDataType::UInt8),
            VIRTUAL_COLUMN_UINT16_TYPE => VirtualColumnPhysicalType::Number(NumberDataType::UInt16),
            VIRTUAL_COLUMN_UINT32_TYPE => VirtualColumnPhysicalType::Number(NumberDataType::UInt32),
            VIRTUAL_COLUMN_INT64_TYPE => VirtualColumnPhysicalType::Number(NumberDataType::Int64),
            VIRTUAL_COLUMN_INT8_TYPE => VirtualColumnPhysicalType::Number(NumberDataType::Int8),
            VIRTUAL_COLUMN_INT16_TYPE => VirtualColumnPhysicalType::Number(NumberDataType::Int16),
            VIRTUAL_COLUMN_INT32_TYPE => VirtualColumnPhysicalType::Number(NumberDataType::Int32),
            VIRTUAL_COLUMN_FLOAT64_TYPE => {
                VirtualColumnPhysicalType::Number(NumberDataType::Float64)
            }
            VIRTUAL_COLUMN_FLOAT32_TYPE => {
                VirtualColumnPhysicalType::Number(NumberDataType::Float32)
            }
            VIRTUAL_COLUMN_STRING_TYPE => VirtualColumnPhysicalType::String,
            VIRTUAL_COLUMN_BINARY_TYPE => VirtualColumnPhysicalType::Binary,
            VIRTUAL_COLUMN_DATE_TYPE => VirtualColumnPhysicalType::Date,
            VIRTUAL_COLUMN_TIMESTAMP_TYPE => VirtualColumnPhysicalType::Timestamp,
            VIRTUAL_COLUMN_TIMESTAMP_TZ_TYPE => VirtualColumnPhysicalType::TimestampTz,
            VIRTUAL_COLUMN_INTERVAL_TYPE => VirtualColumnPhysicalType::Interval,
            VIRTUAL_COLUMN_EXTENDED_TYPE => self
                .extended_physical_type
                .clone()
                .expect("extended virtual column type is missing"),
            _ => unreachable!(),
        }
    }
}

/// Retained path frequencies for one source variant column in a block.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct VirtualColumnPathStatistics {
    /// `(segment-local virtual path id, saturated value_count)` pairs.
    pub path_counts: Vec<(ColumnId, u32)>,
    /// Whether every observed non-direct path for this source is represented in
    /// `path_counts`. Direct paths are intentionally represented only by
    /// `VirtualBlockMeta.virtual_column_metas`; false means producer-side
    /// truncation omitted some non-direct paths.
    pub path_statistics_complete: bool,
}

/// Path frequencies for one source variant column before segment-local path ids
/// are assigned. Each pair is `(canonical_path, saturated value_count)`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DraftVirtualColumnPathStatistics {
    pub path_counts: Vec<(String, u32)>,
    /// Whether every observed non-direct path for this source is represented in
    /// `path_counts`. Direct paths are intentionally represented only by draft
    /// virtual column metadata.
    pub path_statistics_complete: bool,
}

/// The block meta of virtual columns.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct VirtualBlockMeta {
    /// Segment-local direct virtual column metadata. Simple physical types use
    /// the legacy `VirtualColumnMeta.data_type` code; extended types are stored
    /// in `VirtualColumnMeta.extended_physical_type`.
    pub virtual_column_metas: HashMap<ColumnId, VirtualColumnMeta>,
    /// The file size of virtual columns.
    pub virtual_column_size: u64,
    /// The file location of virtual columns.
    pub virtual_location: Location,
    /// Whether BlockMeta completely describes every path physically present in
    /// the sidecar. If false, readers must inspect the sidecar footer for shared
    /// paths before concluding that an unlisted path is missing.
    #[serde(default)]
    pub virtual_columns_complete: bool,
}

/// The draft column meta of virtual columns, virtual ColumnId is not set.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DraftVirtualColumnMeta {
    pub source_column_id: ColumnId,
    pub name: String,
    pub data_type: VirtualColumnPhysicalType,
    pub column_meta: VirtualColumnMeta,
}

/// Draft metadata for a generated virtual-column sidecar.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DraftVirtualColumnBlockMeta {
    /// The draft virtual column metas; segment-local virtual ColumnIds are not assigned yet.
    pub virtual_column_metas: Vec<DraftVirtualColumnMeta>,
    /// Whether every path physically present in the sidecar is represented by
    /// `virtual_column_metas`; false means the sidecar also contains shared paths.
    pub virtual_columns_complete: bool,
    /// The file size of virtual columns.
    pub virtual_column_size: u64,
    /// The file location of virtual columns.
    pub virtual_location: Location,
}

/// Independent optional payloads produced while writing a block.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DraftVirtualBlockMeta {
    pub virtual_columns: Option<DraftVirtualColumnBlockMeta>,
    pub path_statistics: Option<HashMap<ColumnId, DraftVirtualColumnPathStatistics>>,
}

/// Meta information of a block
/// Part of and kept inside the [SegmentInfo]
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct BlockMeta {
    pub row_count: u64,
    pub block_size: u64,
    pub file_size: u64,
    #[serde(deserialize_with = "crate::meta::v2::statistics::deserialize_col_stats")]
    pub col_stats: HashMap<ColumnId, ColumnStatistics>,
    pub col_metas: HashMap<ColumnId, ColumnMeta>,
    pub cluster_stats: Option<ClusterStatistics>,
    #[serde(default)]
    pub partition_stats: Option<PartitionStatistics>,
    /// location of data block
    pub location: Location,
    /// location of bloom filter index
    pub bloom_filter_index_location: Option<Location>,

    #[serde(default)]
    pub bloom_filter_index_size: u64,
    pub inverted_index_size: Option<u64>,
    pub ngram_filter_index_size: Option<u64>,
    pub vector_index_size: Option<u64>,
    pub vector_index_location: Option<Location>,
    pub spatial_index_size: Option<u64>,
    pub spatial_index_location: Option<Location>,
    pub spatial_stats: Option<HashMap<ColumnId, SpatialStatistics>>,
    pub vector_stats: Option<StatisticsOfVectorColumns>,
    /// The block meta of virtual columns.
    pub virtual_block_meta: Option<VirtualBlockMeta>,
    /// Block-local JSON path statistics keyed by source Variant column id.
    #[serde(default)]
    pub virtual_path_statistics: Option<HashMap<ColumnId, VirtualColumnPathStatistics>>,
    pub compression: Compression,

    // block create_on
    pub create_on: Option<DateTime<Utc>>,
}

impl BlockMeta {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        row_count: u64,
        block_size: u64,
        file_size: u64,
        col_stats: HashMap<ColumnId, ColumnStatistics>,
        col_metas: HashMap<ColumnId, ColumnMeta>,
        cluster_stats: Option<ClusterStatistics>,
        location: Location,
        bloom_filter_index_location: Option<Location>,
        bloom_filter_index_size: u64,
        inverted_index_size: Option<u64>,
        ngram_filter_index_size: Option<u64>,
        vector_index_size: Option<u64>,
        vector_index_location: Option<Location>,
        spatial_index_size: Option<u64>,
        spatial_index_location: Option<Location>,
        spatial_stats: Option<HashMap<ColumnId, SpatialStatistics>>,
        virtual_block_meta: Option<VirtualBlockMeta>,
        compression: Compression,
        create_on: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            row_count,
            block_size,
            file_size,
            col_stats,
            col_metas,
            cluster_stats,
            partition_stats: None,
            location,
            bloom_filter_index_location,
            bloom_filter_index_size,
            inverted_index_size,
            ngram_filter_index_size,
            vector_index_size,
            vector_index_location,
            spatial_index_size,
            spatial_index_location,
            spatial_stats,
            vector_stats: None,
            virtual_path_statistics: None,
            virtual_block_meta,
            compression,
            create_on,
        }
    }

    pub fn compression(&self) -> Compression {
        self.compression
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct ExtendedBlockMeta {
    pub block_meta: BlockMeta,
    pub draft_virtual_block_meta: Option<DraftVirtualBlockMeta>,
    pub column_hlls: Option<BlockHLLState>,
    #[serde(default)]
    pub column_top_n: Option<BlockTopN>,
}

#[typetag::serde(name = "extended_block_meta")]
impl BlockMetaInfo for ExtendedBlockMeta {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        ExtendedBlockMeta::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

#[typetag::serde(name = "blockmeta")]
impl BlockMetaInfo for BlockMeta {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        BlockMeta::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl SegmentInfo {
    pub fn from_v0(s: v0::SegmentInfo, fields: &[TableField]) -> Self {
        let summary = Statistics::from_v0(s.summary, fields);
        Self {
            // the is no version before v0, and no versions other then 0 can be converted into v0
            format_version: v0::SegmentInfo::VERSION,
            blocks: s
                .blocks
                .into_iter()
                .map(|b| Arc::new(BlockMeta::from_v0(&b, fields)))
                .collect::<_>(),
            summary,
        }
    }

    pub fn from_v1(s: v1::SegmentInfo, fields: &[TableField]) -> Self {
        let summary = Statistics::from_v0(s.summary, fields);
        Self {
            // NOTE: it is important to let the format_version return from here
            // carries the format_version of segment info being converted.
            format_version: s.format_version,
            blocks: s
                .blocks
                .into_iter()
                .map(|b| Arc::new(BlockMeta::from_v1(b.as_ref(), fields)))
                .collect::<_>(),
            summary,
        }
    }
}

#[derive(
    serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, EnumAsInner, FrozenAPI,
)]
pub enum ColumnMeta {
    Parquet(v0::ColumnMeta),
}

impl ColumnMeta {
    pub fn total_rows(&self) -> usize {
        match self {
            ColumnMeta::Parquet(v) => v.num_values as usize,
        }
    }

    pub fn offset_length(&self) -> (u64, u64) {
        match self {
            ColumnMeta::Parquet(v) => (v.offset, v.len),
        }
    }

    pub fn read_rows(&self, _range: Option<&Range<usize>>) -> u64 {
        match self {
            ColumnMeta::Parquet(v) => v.num_values,
        }
    }

    pub fn read_bytes(&self, _range: &Option<Range<usize>>) -> u64 {
        match self {
            ColumnMeta::Parquet(v) => v.len,
        }
    }
}

impl BlockMeta {
    pub fn from_v0(s: &v0::BlockMeta, fields: &[TableField]) -> Self {
        let col_stats = Statistics::convert_column_stats(&s.col_stats, fields);

        let col_metas = s
            .col_metas
            .iter()
            .map(|(k, v)| (*k, ColumnMeta::Parquet(v.clone())))
            .collect();

        Self {
            row_count: s.row_count,
            block_size: s.block_size,
            file_size: s.file_size,
            col_stats,
            col_metas,
            cluster_stats: None,
            partition_stats: None,
            location: (s.location.path.clone(), 0),
            bloom_filter_index_location: None,
            bloom_filter_index_size: 0,
            compression: Compression::Lz4,
            inverted_index_size: None,
            vector_index_size: None,
            vector_index_location: None,
            spatial_index_size: None,
            spatial_index_location: None,
            spatial_stats: None,
            vector_stats: None,
            virtual_path_statistics: None,
            virtual_block_meta: None,
            create_on: None,
            ngram_filter_index_size: None,
        }
    }

    pub fn from_v1(s: &v1::BlockMeta, fields: &[TableField]) -> Self {
        let col_stats = Statistics::convert_column_stats(&s.col_stats, fields);
        let col_metas = s
            .col_metas
            .iter()
            .map(|(k, v)| (*k, ColumnMeta::Parquet(v.clone())))
            .collect();

        Self {
            row_count: s.row_count,
            block_size: s.block_size,
            file_size: s.file_size,
            col_stats,
            col_metas,
            cluster_stats: None,
            partition_stats: None,
            location: s.location.clone(),
            bloom_filter_index_location: s.bloom_filter_index_location.clone(),
            bloom_filter_index_size: s.bloom_filter_index_size,
            compression: s.compression,
            inverted_index_size: None,
            vector_index_size: None,
            vector_index_location: None,
            spatial_index_size: None,
            spatial_index_location: None,
            spatial_stats: None,
            vector_stats: None,
            virtual_path_statistics: None,
            virtual_block_meta: None,
            create_on: None,
            ngram_filter_index_size: None,
        }
    }
}

impl From<(v1::SegmentInfo, &[TableField])> for SegmentInfo {
    fn from((v, fields): (v1::SegmentInfo, &[TableField])) -> Self {
        SegmentInfo::from_v1(v, fields)
    }
}

impl From<(v0::SegmentInfo, &[TableField])> for SegmentInfo {
    fn from((v, fields): (v0::SegmentInfo, &[TableField])) -> Self {
        SegmentInfo::from_v0(v, fields)
    }
}
