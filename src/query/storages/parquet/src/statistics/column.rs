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

use databend_common_expression::types::decimal::DecimalScalar;
use databend_common_expression::types::i256;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use parquet::data_type::AsBytes;
use parquet::file::statistics::Statistics;

use super::utils::decode_decimal128_from_bytes;
use super::utils::decode_decimal256_from_bytes;

/// according to https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
pub fn convert_column_statistics(s: &Statistics, typ: &TableDataType) -> Option<ColumnStatistics> {
    let (max, min) = if s.has_min_max_set() {
        match s {
            Statistics::Boolean(s) => (Scalar::Boolean(*s.max()), Scalar::Boolean(*s.min())),
            Statistics::Int32(s) => {
                let (max, min) = (*s.max(), *s.min());
                match typ {
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
                        Scalar::Decimal(DecimalScalar::Decimal256(i256::from(max), *size)),
                        Scalar::Decimal(DecimalScalar::Decimal256(i256::from(min), *size)),
                    ),
                    _ => return None,
                }
            }
            Statistics::Int64(s) => {
                let (max, min) = (*s.max(), *s.min());
                match typ {
                    TableDataType::Number(NumberDataType::UInt64) => {
                        (Scalar::from(max as u64), Scalar::from(min as u64))
                    }
                    TableDataType::Number(NumberDataType::Int64) => {
                        (Scalar::from(max), Scalar::from(min))
                    }
                    TableDataType::Timestamp => {
                        let multi = match max.checked_ilog10().unwrap_or_default() + 1 {
                            0..=10 => 1_000_000,
                            11..=13 => 1_000,
                            _ => 1,
                        };
                        (
                            Scalar::Timestamp(max * multi),
                            Scalar::Timestamp(min * multi),
                        )
                    }
                    TableDataType::Decimal(DecimalDataType::Decimal128(size)) => (
                        Scalar::Decimal(DecimalScalar::Decimal128(i128::from(max), *size)),
                        Scalar::Decimal(DecimalScalar::Decimal128(i128::from(min), *size)),
                    ),
                    TableDataType::Decimal(DecimalDataType::Decimal256(size)) => (
                        Scalar::Decimal(DecimalScalar::Decimal256(i256::from(max), *size)),
                        Scalar::Decimal(DecimalScalar::Decimal256(i256::from(min), *size)),
                    ),
                    _ => return None,
                }
            }
            Statistics::Int96(s) => {
                let (max, min) = (s.max().to_i64(), s.min().to_i64());
                let multi = match max.checked_ilog10().unwrap_or_default() + 1 {
                    0..=10 => 1_000_000,
                    11..=13 => 1_000,
                    _ => 1,
                };
                (
                    Scalar::Timestamp(max * multi),
                    Scalar::Timestamp(min * multi),
                )
            }
            Statistics::Float(s) => (Scalar::from(*s.max()), Scalar::from(*s.min())),
            Statistics::Double(s) => (Scalar::from(*s.max()), Scalar::from(*s.min())),
            Statistics::ByteArray(s) => (
                Scalar::String(String::from_utf8(s.max().as_bytes().to_vec()).ok()?),
                Scalar::String(String::from_utf8(s.min().as_bytes().to_vec()).ok()?),
            ),
            Statistics::FixedLenByteArray(s) => {
                let (max, min) = (s.max(), s.min());
                match typ {
                    TableDataType::Decimal(DecimalDataType::Decimal128(size)) => (
                        decode_decimal128_from_bytes(max, *size),
                        decode_decimal128_from_bytes(min, *size),
                    ),
                    TableDataType::Decimal(DecimalDataType::Decimal256(size)) => (
                        decode_decimal256_from_bytes(max, *size),
                        decode_decimal256_from_bytes(min, *size),
                    ),
                    _ => return None,
                }
            }
        }
    } else {
        return None;
    };
    Some(ColumnStatistics::new(
        min,
        max,
        s.null_count(),
        0, // this field is not used.
        s.distinct_count(),
    ))
}
