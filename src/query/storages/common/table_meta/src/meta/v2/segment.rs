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
use databend_common_expression::types::i256;
use databend_common_expression::types::Decimal;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::ColumnId;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::VariantDataType;
use databend_common_frozen_api::frozen_api;
use databend_common_frozen_api::FrozenAPI;
use databend_common_native::ColumnMeta as NativeColumnMeta;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::v0;
use crate::meta::v1;
use crate::meta::BlockHLLState;
use crate::meta::ClusterStatistics;
use crate::meta::ColumnStatistics;
use crate::meta::Compression;
use crate::meta::FormatVersion;
use crate::meta::Location;
use crate::meta::Statistics;
use crate::meta::Versioned;

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
const VIRTUAL_COLUMN_DECIMAL64_TYPE: u8 = 6;
const VIRTUAL_COLUMN_DECIMAL128_TYPE: u8 = 7;
const VIRTUAL_COLUMN_DECIMAL256_TYPE: u8 = 8;
const VIRTUAL_COLUMN_BINARY_TYPE: u8 = 9;
const VIRTUAL_COLUMN_DATE_TYPE: u8 = 10;
const VIRTUAL_COLUMN_TIMESTAMP_TYPE: u8 = 11;
const VIRTUAL_COLUMN_INTERVAL_TYPE: u8 = 12;

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
    // 6 => decimal64
    // 7 => decimal128
    // 8 => decimal256
    // 9 => binary
    // 10 => date
    // 11 => timestamp
    // 12 => interval
    pub data_type: u8,
    /// the scale size, only used for decimal type
    pub scale: Option<u8>,
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
        let scale = self.scale.unwrap_or_default();
        match self.data_type {
            VIRTUAL_COLUMN_JSONB_TYPE => TableDataType::Nullable(Box::new(TableDataType::Variant)),
            VIRTUAL_COLUMN_BOOL_TYPE => TableDataType::Nullable(Box::new(TableDataType::Boolean)),
            VIRTUAL_COLUMN_UINT64_TYPE => {
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64)))
            }
            VIRTUAL_COLUMN_INT64_TYPE => {
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int64)))
            }
            VIRTUAL_COLUMN_FLOAT64_TYPE => {
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Float64)))
            }
            VIRTUAL_COLUMN_STRING_TYPE => TableDataType::Nullable(Box::new(TableDataType::String)),
            VIRTUAL_COLUMN_DECIMAL64_TYPE => {
                let size = DecimalSize::new_unchecked(i64::MAX_PRECISION, scale);
                TableDataType::Nullable(Box::new(TableDataType::Decimal(DecimalDataType::from(
                    size,
                ))))
            }
            VIRTUAL_COLUMN_DECIMAL128_TYPE => {
                let size = DecimalSize::new_unchecked(i128::MAX_PRECISION, scale);
                TableDataType::Nullable(Box::new(TableDataType::Decimal(DecimalDataType::from(
                    size,
                ))))
            }
            VIRTUAL_COLUMN_DECIMAL256_TYPE => {
                let size = DecimalSize::new_unchecked(i256::MAX_PRECISION, scale);
                TableDataType::Nullable(Box::new(TableDataType::Decimal(DecimalDataType::from(
                    size,
                ))))
            }
            VIRTUAL_COLUMN_BINARY_TYPE => TableDataType::Nullable(Box::new(TableDataType::Binary)),
            VIRTUAL_COLUMN_DATE_TYPE => TableDataType::Nullable(Box::new(TableDataType::Date)),
            VIRTUAL_COLUMN_TIMESTAMP_TYPE => {
                TableDataType::Nullable(Box::new(TableDataType::Timestamp))
            }
            VIRTUAL_COLUMN_INTERVAL_TYPE => {
                TableDataType::Nullable(Box::new(TableDataType::Interval))
            }
            _ => unreachable!(),
        }
    }

    pub fn data_type_code(variant_type: &VariantDataType) -> (u8, Option<u8>) {
        let ty = match variant_type {
            VariantDataType::Jsonb => VIRTUAL_COLUMN_JSONB_TYPE,
            VariantDataType::Boolean => VIRTUAL_COLUMN_BOOL_TYPE,
            VariantDataType::UInt64 => VIRTUAL_COLUMN_UINT64_TYPE,
            VariantDataType::Int64 => VIRTUAL_COLUMN_INT64_TYPE,
            VariantDataType::Float64 => VIRTUAL_COLUMN_FLOAT64_TYPE,
            VariantDataType::String => VIRTUAL_COLUMN_STRING_TYPE,
            VariantDataType::Decimal(ty) => match ty {
                DecimalDataType::Decimal64(_) => VIRTUAL_COLUMN_DECIMAL64_TYPE,
                DecimalDataType::Decimal128(_) => VIRTUAL_COLUMN_DECIMAL128_TYPE,
                DecimalDataType::Decimal256(_) => VIRTUAL_COLUMN_DECIMAL256_TYPE,
            },
            VariantDataType::Binary => VIRTUAL_COLUMN_BINARY_TYPE,
            VariantDataType::Date => VIRTUAL_COLUMN_DATE_TYPE,
            VariantDataType::Timestamp => VIRTUAL_COLUMN_TIMESTAMP_TYPE,
            VariantDataType::Interval => VIRTUAL_COLUMN_INTERVAL_TYPE,
            _ => unreachable!(),
        };
        let scale = match variant_type {
            VariantDataType::Decimal(ty) => Some(ty.scale()),
            _ => None,
        };
        (ty, scale)
    }
}

/// The block meta of virtual columns.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct VirtualBlockMeta {
    /// key is virtual columnId, value is VirtualColumnMeta
    pub virtual_column_metas: HashMap<ColumnId, VirtualColumnMeta>,
    /// The file size of virtual columns.
    pub virtual_column_size: u64,
    /// The file location of virtual columns.
    pub virtual_location: Location,
}

/// The draft column meta of virtual columns, virtual ColumnId is not set.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct DraftVirtualColumnMeta {
    pub source_column_id: ColumnId,
    pub name: String,
    pub data_type: VariantDataType,
    pub column_meta: VirtualColumnMeta,
}

/// The draft block meta of virtual columns.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct DraftVirtualBlockMeta {
    /// The draft virtual oclumn metas, virtual ColumnId needs to be set.
    pub virtual_column_metas: Vec<DraftVirtualColumnMeta>,
    /// The file size of virtual columns.
    pub virtual_column_size: u64,
    /// The file location of virtual columns.
    pub virtual_location: Location,
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
    /// The block meta of virtual columns.
    pub virtual_block_meta: Option<VirtualBlockMeta>,
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
            location,
            bloom_filter_index_location,
            bloom_filter_index_size,
            inverted_index_size,
            ngram_filter_index_size,
            vector_index_size,
            vector_index_location,
            virtual_block_meta,
            compression,
            create_on,
        }
    }

    pub fn compression(&self) -> Compression {
        self.compression
    }

    /// Get the page size of the block.
    ///
    /// - If the format is parquet, its page size is its row count.
    /// - If the format is native, its page size is the row count of each page.
    ///
    /// The row count of the last page may be smaller than the page size
    pub fn page_size(&self) -> u64 {
        if let Some((_, ColumnMeta::Native(meta))) = self.col_metas.iter().next() {
            meta.pages.first().unwrap().num_values
        } else {
            self.row_count
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, FrozenAPI)]
pub struct ExtendedBlockMeta {
    pub block_meta: BlockMeta,
    pub draft_virtual_block_meta: Option<DraftVirtualBlockMeta>,
    pub column_hlls: Option<BlockHLLState>,
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
    Native(NativeColumnMeta),
}

impl ColumnMeta {
    pub fn total_rows(&self) -> usize {
        match self {
            ColumnMeta::Parquet(v) => v.num_values as usize,
            ColumnMeta::Native(v) => v.pages.iter().map(|page| page.num_values as usize).sum(),
        }
    }

    pub fn offset_length(&self) -> (u64, u64) {
        match self {
            ColumnMeta::Parquet(v) => (v.offset, v.len),
            ColumnMeta::Native(v) => (v.offset, v.pages.iter().map(|page| page.length).sum()),
        }
    }

    pub fn read_rows(&self, range: Option<&Range<usize>>) -> u64 {
        match self {
            ColumnMeta::Parquet(v) => v.num_values,
            ColumnMeta::Native(v) => match range {
                Some(range) => v
                    .pages
                    .iter()
                    .skip(range.start)
                    .take(range.end - range.start)
                    .map(|page| page.num_values)
                    .sum(),
                None => v.pages.iter().map(|page| page.num_values).sum(),
            },
        }
    }

    pub fn read_bytes(&self, range: &Option<Range<usize>>) -> u64 {
        match self {
            ColumnMeta::Parquet(v) => v.len,
            ColumnMeta::Native(v) => match range {
                Some(range) => v
                    .pages
                    .iter()
                    .skip(range.start)
                    .take(range.end - range.start)
                    .map(|page| page.length)
                    .sum(),
                None => v.pages.iter().map(|page| page.length).sum(),
            },
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
            location: (s.location.path.clone(), 0),
            bloom_filter_index_location: None,
            bloom_filter_index_size: 0,
            compression: Compression::Lz4,
            inverted_index_size: None,
            vector_index_size: None,
            vector_index_location: None,
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
            location: s.location.clone(),
            bloom_filter_index_location: s.bloom_filter_index_location.clone(),
            bloom_filter_index_size: s.bloom_filter_index_size,
            compression: s.compression,
            inverted_index_size: None,
            vector_index_size: None,
            vector_index_location: None,
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
