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

use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::decimal::DecimalScalar;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use ethnum::I256;
use parquet::data_type::AsBytes;
use parquet::data_type::ByteArray;
use parquet::data_type::FixedLenByteArray;
use parquet::data_type::Int96;
use parquet::file::page_index::index::Index;
use parquet::file::page_index::index::PageIndex;

use super::utils::decode_decimal128_from_bytes;
use super::utils::decode_decimal256_from_bytes;

pub fn convert_index_to_column_statistics(
    index: &Index,
    num_pagas: usize,
    typ: &TableDataType,
) -> Vec<Option<ColumnStatistics>> {
    match index {
        Index::NONE => vec![None; num_pagas],
        Index::BOOLEAN(index) => {
            assert_eq!(num_pagas, index.indexes.len());
            index
                .indexes
                .iter()
                .map(|index| convert_page_index_boolean(index, typ))
                .collect()
        }
        Index::INT32(index) => {
            assert_eq!(num_pagas, index.indexes.len());
            index
                .indexes
                .iter()
                .map(|index| convert_page_index_int32(index, typ))
                .collect()
        }
        Index::INT64(index) => {
            assert_eq!(num_pagas, index.indexes.len());
            index
                .indexes
                .iter()
                .map(|index| convert_page_index_int64(index, typ))
                .collect()
        }
        Index::INT96(index) => {
            assert_eq!(num_pagas, index.indexes.len());
            index
                .indexes
                .iter()
                .map(|index| convert_page_index_int96(index, typ))
                .collect()
        }
        Index::FLOAT(index) => {
            assert_eq!(num_pagas, index.indexes.len());
            index
                .indexes
                .iter()
                .map(|index| convert_page_index_float(index, typ))
                .collect()
        }
        Index::DOUBLE(index) => {
            assert_eq!(num_pagas, index.indexes.len());
            index
                .indexes
                .iter()
                .map(|index| convert_page_index_double(index, typ))
                .collect()
        }
        Index::BYTE_ARRAY(index) => {
            assert_eq!(num_pagas, index.indexes.len());
            index
                .indexes
                .iter()
                .map(|index| convert_page_index_byte_array(index, typ))
                .collect()
        }
        Index::FIXED_LEN_BYTE_ARRAY(index) => {
            assert_eq!(num_pagas, index.indexes.len());
            index
                .indexes
                .iter()
                .map(|index| convert_page_index_fixed_len_byte_array(index, typ))
                .collect()
        }
    }
}

fn convert_page_index_boolean(
    index: &PageIndex<bool>,
    _typ: &TableDataType,
) -> Option<ColumnStatistics> {
    match (index.min, index.max, index.null_count) {
        (Some(min), Some(max), Some(null_count)) => Some(ColumnStatistics {
            min: Scalar::Boolean(min),
            max: Scalar::Boolean(max),
            null_count: null_count as u64,
            in_memory_size: 0, // not needed,
            distinct_of_values: None,
        }),
        _ => None,
    }
}

fn convert_page_index_int32(
    index: &PageIndex<i32>,
    typ: &TableDataType,
) -> Option<ColumnStatistics> {
    match (index.min, index.max, index.null_count) {
        (Some(min), Some(max), Some(null_count)) => {
            let (max, min) = match typ {
                TableDataType::Number(NumberDataType::Int8) => {
                    (Scalar::from(max as i8), Scalar::from(min as i8))
                }
                TableDataType::Number(NumberDataType::Int16) => {
                    (Scalar::from(max as i16), Scalar::from(min as i16))
                }
                TableDataType::Number(NumberDataType::Int32) => {
                    (Scalar::from(max), Scalar::from(min))
                }
                TableDataType::Number(NumberDataType::UInt8) => {
                    (Scalar::from(max as u8), Scalar::from(min as u8))
                }
                TableDataType::Number(NumberDataType::UInt16) => {
                    (Scalar::from(max as u16), Scalar::from(min as u16))
                }
                TableDataType::Number(NumberDataType::UInt32) => {
                    (Scalar::from(max as u32), Scalar::from(min as u32))
                }
                TableDataType::Date => (Scalar::Date(max), Scalar::Date(min)),
                TableDataType::Decimal(DecimalDataType::Decimal128(size)) => (
                    Scalar::Decimal(DecimalScalar::Decimal128(i128::from(max), *size)),
                    Scalar::Decimal(DecimalScalar::Decimal128(i128::from(min), *size)),
                ),
                TableDataType::Decimal(DecimalDataType::Decimal256(size)) => (
                    Scalar::Decimal(DecimalScalar::Decimal256(I256::from_i128(max), *size)),
                    Scalar::Decimal(DecimalScalar::Decimal256(I256::from_i128(min), *size)),
                ),
                _ => unreachable!(),
            };
            Some(ColumnStatistics {
                min,
                max,
                null_count: null_count as u64,
                in_memory_size: 0, // not needed,
                distinct_of_values: None,
            })
        }
        _ => None,
    }
}

fn convert_page_index_int64(
    index: &PageIndex<i64>,
    typ: &TableDataType,
) -> Option<ColumnStatistics> {
    match (index.min, index.max, index.null_count) {
        (Some(min), Some(max), Some(null_count)) => {
            let (max, min) = match typ {
                TableDataType::Number(NumberDataType::UInt64) => {
                    (Scalar::from(max as u64), Scalar::from(min as u64))
                }
                TableDataType::Number(NumberDataType::Int64) => {
                    (Scalar::from(max), Scalar::from(min))
                }
                TableDataType::Timestamp => (Scalar::Timestamp(max), Scalar::Timestamp(min)),
                TableDataType::Decimal(DecimalDataType::Decimal128(size)) => (
                    Scalar::Decimal(DecimalScalar::Decimal128(i128::from(max), *size)),
                    Scalar::Decimal(DecimalScalar::Decimal128(i128::from(min), *size)),
                ),
                TableDataType::Decimal(DecimalDataType::Decimal256(size)) => (
                    Scalar::Decimal(DecimalScalar::Decimal256(I256::from_i128(max), *size)),
                    Scalar::Decimal(DecimalScalar::Decimal256(I256::from_i128(min), *size)),
                ),
                _ => unreachable!(),
            };
            Some(ColumnStatistics {
                min,
                max,
                null_count: null_count as u64,
                in_memory_size: 0, // not needed,
                distinct_of_values: None,
            })
        }
        _ => None,
    }
}

fn convert_page_index_int96(
    index: &PageIndex<Int96>,
    _typ: &TableDataType,
) -> Option<ColumnStatistics> {
    match (&index.min, &index.max, index.null_count) {
        (Some(min), Some(max), Some(null_count)) => {
            Some(ColumnStatistics {
                min: Scalar::Timestamp(min.to_i64()),
                max: Scalar::Timestamp(max.to_i64()),
                null_count: null_count as u64,
                in_memory_size: 0, // not needed,
                distinct_of_values: None,
            })
        }
        _ => None,
    }
}

fn convert_page_index_float(
    index: &PageIndex<f32>,
    _typ: &TableDataType,
) -> Option<ColumnStatistics> {
    match (index.min, index.max, index.null_count) {
        (Some(min), Some(max), Some(null_count)) => {
            Some(ColumnStatistics {
                min: Scalar::from(min),
                max: Scalar::from(max),
                null_count: null_count as u64,
                in_memory_size: 0, // not needed,
                distinct_of_values: None,
            })
        }
        _ => None,
    }
}

fn convert_page_index_double(
    index: &PageIndex<f64>,
    _typ: &TableDataType,
) -> Option<ColumnStatistics> {
    match (index.min, index.max, index.null_count) {
        (Some(min), Some(max), Some(null_count)) => {
            Some(ColumnStatistics {
                min: Scalar::from(min),
                max: Scalar::from(max),
                null_count: null_count as u64,
                in_memory_size: 0, // not needed,
                distinct_of_values: None,
            })
        }
        _ => None,
    }
}

fn convert_page_index_byte_array(
    index: &PageIndex<ByteArray>,
    _typ: &TableDataType,
) -> Option<ColumnStatistics> {
    match (&index.min, &index.max, index.null_count) {
        (Some(min), Some(max), Some(null_count)) => {
            Some(ColumnStatistics {
                min: Scalar::String(String::from_utf8(min.as_bytes().to_vec()).ok()?),
                max: Scalar::String(String::from_utf8(max.as_bytes().to_vec()).ok()?),
                null_count: null_count as u64,
                in_memory_size: 0, // not needed,
                distinct_of_values: None,
            })
        }
        _ => None,
    }
}

fn convert_page_index_fixed_len_byte_array(
    index: &PageIndex<FixedLenByteArray>,
    typ: &TableDataType,
) -> Option<ColumnStatistics> {
    match (&index.min, &index.max, index.null_count) {
        (Some(min), Some(max), Some(null_count)) => {
            let (max, min) = match typ {
                TableDataType::Decimal(DecimalDataType::Decimal128(size)) => (
                    decode_decimal128_from_bytes(max, *size),
                    decode_decimal128_from_bytes(min, *size),
                ),
                TableDataType::Decimal(DecimalDataType::Decimal256(size)) => (
                    decode_decimal256_from_bytes(max, *size),
                    decode_decimal256_from_bytes(min, *size),
                ),
                _ => unreachable!(),
            };
            Some(ColumnStatistics {
                min,
                max,
                null_count: null_count as u64,
                in_memory_size: 0, // not needed,
                distinct_of_values: None,
            })
        }
        _ => None,
    }
}
