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

use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::DecimalScalar;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::i256;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use parquet::data_type::AsBytes;
use parquet::data_type::FixedLenByteArray;
use parquet::data_type::Int96;
use parquet::file::page_index::column_index::ByteArrayColumnIndex;
use parquet::file::page_index::column_index::ColumnIndexMetaData;
use parquet::file::page_index::column_index::PrimitiveColumnIndex;

use super::utils::decode_decimal128_from_bytes;
use super::utils::decode_decimal256_from_bytes;

pub fn convert_index_to_column_statistics(
    index: &ColumnIndexMetaData,
    num_pagas: usize,
    typ: &TableDataType,
) -> Vec<Option<ColumnStatistics>> {
    match index {
        ColumnIndexMetaData::NONE => vec![None; num_pagas],
        ColumnIndexMetaData::BOOLEAN(index) => (0..index.num_pages() as usize)
            .map(|i| convert_page_index_bool(index, i, typ))
            .collect(),
        ColumnIndexMetaData::INT32(index) => (0..index.num_pages() as usize)
            .map(|i| convert_page_index_int32(index, i, typ))
            .collect(),
        ColumnIndexMetaData::INT64(index) => (0..index.num_pages() as usize)
            .map(|i| convert_page_index_int64(index, i, typ))
            .collect(),
        ColumnIndexMetaData::INT96(index) => (0..index.num_pages() as usize)
            .map(|i| convert_page_index_int96(index, i, typ))
            .collect(),
        ColumnIndexMetaData::FLOAT(index) => (0..index.num_pages() as usize)
            .map(|i| convert_page_index_float(index, i, typ))
            .collect(),
        ColumnIndexMetaData::DOUBLE(index) => (0..index.num_pages() as usize)
            .map(|i| convert_page_index_double(index, i, typ))
            .collect(),
        ColumnIndexMetaData::BYTE_ARRAY(index) => (0..index.num_pages() as usize)
            .map(|i| convert_page_index_byte_array(index, i, typ))
            .collect(),
        ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(index) => (0..index.num_pages() as usize)
            .map(|i| convert_page_index_fixed_len_byte_array(index, i, typ))
            .collect(),
    }
}

fn convert_page_index_int32(
    index: &PrimitiveColumnIndex<i32>,
    idx: usize,
    typ: &TableDataType,
) -> Option<ColumnStatistics> {
    match (
        index.min_value(idx),
        index.max_value(idx),
        index.null_count(idx),
    ) {
        (Some(min), Some(max), Some(null_count)) => {
            let (max, min) = match typ {
                TableDataType::Number(NumberDataType::Int8) => {
                    (Scalar::from(*max as i8), Scalar::from(*min as i8))
                }
                TableDataType::Number(NumberDataType::Int16) => {
                    (Scalar::from(*max as i16), Scalar::from(*min as i16))
                }
                TableDataType::Number(NumberDataType::Int32) => {
                    (Scalar::from(*max), Scalar::from(*min))
                }
                TableDataType::Number(NumberDataType::UInt8) => {
                    (Scalar::from(*max as u8), Scalar::from(*min as u8))
                }
                TableDataType::Number(NumberDataType::UInt16) => {
                    (Scalar::from(*max as u16), Scalar::from(*min as u16))
                }
                TableDataType::Number(NumberDataType::UInt32) => {
                    (Scalar::from(*max as u32), Scalar::from(*min as u32))
                }
                TableDataType::Date => (Scalar::Date(*max), Scalar::Date(*min)),
                TableDataType::Decimal(decimal) => match decimal {
                    DecimalDataType::Decimal128(size) => (
                        Scalar::Decimal(DecimalScalar::Decimal128(i128::from(*max), *size)),
                        Scalar::Decimal(DecimalScalar::Decimal128(i128::from(*min), *size)),
                    ),
                    DecimalDataType::Decimal256(size) => (
                        Scalar::Decimal(DecimalScalar::Decimal256(i256::from(*max), *size)),
                        Scalar::Decimal(DecimalScalar::Decimal256(i256::from(*min), *size)),
                    ),
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            };
            Some(ColumnStatistics::new(min, max, null_count as u64, 0, None))
        }
        _ => None,
    }
}

fn convert_page_index_int64(
    index: &PrimitiveColumnIndex<i64>,
    idx: usize,
    typ: &TableDataType,
) -> Option<ColumnStatistics> {
    match (
        index.min_value(idx),
        index.max_value(idx),
        index.null_count(idx),
    ) {
        (Some(min), Some(max), Some(null_count)) => {
            let (max, min) = match typ {
                TableDataType::Number(NumberDataType::UInt64) => {
                    (Scalar::from(*max as u64), Scalar::from(*min as u64))
                }
                TableDataType::Number(NumberDataType::Int64) => {
                    (Scalar::from(*max), Scalar::from(*min))
                }
                TableDataType::Timestamp => (Scalar::Timestamp(*max), Scalar::Timestamp(*min)),
                TableDataType::Decimal(DecimalDataType::Decimal128(size)) => (
                    Scalar::Decimal(DecimalScalar::Decimal128(i128::from(*max), *size)),
                    Scalar::Decimal(DecimalScalar::Decimal128(i128::from(*min), *size)),
                ),
                TableDataType::Decimal(DecimalDataType::Decimal256(size)) => (
                    Scalar::Decimal(DecimalScalar::Decimal256(i256::from(*max), *size)),
                    Scalar::Decimal(DecimalScalar::Decimal256(i256::from(*min), *size)),
                ),
                _ => unreachable!(),
            };
            Some(ColumnStatistics::new(min, max, null_count as u64, 0, None))
        }
        _ => None,
    }
}

fn convert_page_index_int96(
    index: &PrimitiveColumnIndex<Int96>,
    idx: usize,
    _typ: &TableDataType,
) -> Option<ColumnStatistics> {
    match (
        index.min_value(idx),
        index.max_value(idx),
        index.null_count(idx),
    ) {
        (Some(min), Some(max), Some(null_count)) => Some(ColumnStatistics::new(
            Scalar::Timestamp(min.to_micros()),
            Scalar::Timestamp(max.to_micros()),
            null_count as u64,
            0,
            None,
        )),
        _ => None,
    }
}

fn convert_page_index_float(
    index: &PrimitiveColumnIndex<f32>,
    idx: usize,
    _typ: &TableDataType,
) -> Option<ColumnStatistics> {
    match (
        index.min_value(idx),
        index.max_value(idx),
        index.null_count(idx),
    ) {
        (Some(min), Some(max), Some(null_count)) => Some(ColumnStatistics::new(
            Scalar::from(*min),
            Scalar::from(*max),
            null_count as u64,
            0,
            None,
        )),
        _ => None,
    }
}

fn convert_page_index_bool(
    index: &PrimitiveColumnIndex<bool>,
    idx: usize,
    _typ: &TableDataType,
) -> Option<ColumnStatistics> {
    match (
        index.min_value(idx),
        index.max_value(idx),
        index.null_count(idx),
    ) {
        (Some(min), Some(max), Some(null_count)) => Some(ColumnStatistics::new(
            Scalar::Boolean(*min),
            Scalar::Boolean(*max),
            null_count as u64,
            0,
            None,
        )),
        _ => None,
    }
}

fn convert_page_index_double(
    index: &PrimitiveColumnIndex<f64>,
    idx: usize,
    _typ: &TableDataType,
) -> Option<ColumnStatistics> {
    match (
        index.min_value(idx),
        index.max_value(idx),
        index.null_count(idx),
    ) {
        (Some(min), Some(max), Some(null_count)) => Some(ColumnStatistics::new(
            Scalar::from(*min),
            Scalar::from(*max),
            null_count as u64,
            0,
            None,
        )),
        _ => None,
    }
}

fn convert_page_index_byte_array(
    index: &ByteArrayColumnIndex,
    idx: usize,
    _typ: &TableDataType,
) -> Option<ColumnStatistics> {
    match (
        index.min_value(idx),
        index.max_value(idx),
        index.null_count(idx),
    ) {
        (Some(min), Some(max), Some(null_count)) => Some(ColumnStatistics::new(
            Scalar::String(String::from_utf8(min.as_bytes().to_vec()).ok()?),
            Scalar::String(String::from_utf8(max.as_bytes().to_vec()).ok()?),
            null_count as u64,
            0,
            None,
        )),
        _ => None,
    }
}

fn convert_page_index_fixed_len_byte_array(
    index: &ByteArrayColumnIndex,
    idx: usize,
    typ: &TableDataType,
) -> Option<ColumnStatistics> {
    match (
        index.min_value(idx),
        index.max_value(idx),
        index.null_count(idx),
    ) {
        (Some(min), Some(max), Some(null_count)) => {
            let (max, min) = match typ {
                TableDataType::Decimal(DecimalDataType::Decimal128(size)) => (
                    decode_decimal128_from_bytes(&FixedLenByteArray::from(max.to_vec()), *size),
                    decode_decimal128_from_bytes(&FixedLenByteArray::from(min.to_vec()), *size),
                ),
                TableDataType::Decimal(DecimalDataType::Decimal256(size)) => (
                    decode_decimal256_from_bytes(&FixedLenByteArray::from(max.to_vec()), *size),
                    decode_decimal256_from_bytes(&FixedLenByteArray::from(min.to_vec()), *size),
                ),
                _ => unreachable!(),
            };

            Some(ColumnStatistics::new(min, max, null_count as u64, 0, None))
        }
        _ => None,
    }
}
