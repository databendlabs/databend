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

use arrow_buffer::i256;
use common_exception::Result;
use common_expression::types::decimal::Decimal;
use common_expression::types::decimal::DecimalScalar;
use common_expression::types::DecimalDataType;
use common_expression::types::DecimalSize;
use common_expression::types::NumberDataType;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableSchema;
use ethnum::I256;
use parquet::data_type::AsBytes;
use parquet::data_type::FixedLenByteArray;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::StatisticsOfColumns;

/// Collect statistics of a batch of row groups.
///
/// The returned vector's length is the same as `rgs`.
///
/// TODO(parquet): we can only collect statistics of columns we need to eval.
pub fn collect_row_group_stats(
    schema: &TableSchema,
    rgs: &[RowGroupMetaData],
) -> Result<Vec<StatisticsOfColumns>> {
    let fields = schema.leaf_fields();
    let mut stats = Vec::with_capacity(rgs.len());
    for rg in rgs {
        assert_eq!(rg.num_columns(), fields.len());
        let mut stats_of_columns = HashMap::with_capacity(rg.columns().len());

        // Each row_group_stat is a `HashMap` holding key-value pairs.
        // The first element of the pair is the offset in the schema,
        // and the second element is the statistics of the column (according to the offset)
        for (index, (column, field)) in rg.columns().iter().zip(fields.iter()).enumerate() {
            let column_stats = column.statistics().unwrap();
            stats_of_columns.insert(
                index as u32,
                convert_column_statistics(column_stats, &field.data_type().remove_nullable()),
            );
        }
        stats.push(stats_of_columns);
    }
    Ok(stats)
}

/// according to https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
fn convert_column_statistics(s: &Statistics, typ: &TableDataType) -> ColumnStatistics {
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
                        Scalar::Decimal(DecimalScalar::Decimal256(
                            I256::from_i64(max as i64),
                            *size,
                        )),
                        Scalar::Decimal(DecimalScalar::Decimal256(
                            I256::from_i64(min as i64),
                            *size,
                        )),
                    ),
                    _ => unreachable!(),
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
                    TableDataType::Timestamp => (Scalar::Timestamp(max), Scalar::Timestamp(min)),
                    TableDataType::Decimal(DecimalDataType::Decimal128(size)) => (
                        Scalar::Decimal(DecimalScalar::Decimal128(i128::from(max), *size)),
                        Scalar::Decimal(DecimalScalar::Decimal128(i128::from(min), *size)),
                    ),
                    TableDataType::Decimal(DecimalDataType::Decimal256(size)) => (
                        Scalar::Decimal(DecimalScalar::Decimal256(I256::from_i64(max), *size)),
                        Scalar::Decimal(DecimalScalar::Decimal256(I256::from_i64(min), *size)),
                    ),
                    _ => unreachable!(),
                }
            }
            Statistics::Int96(s) => (
                Scalar::Timestamp(s.max().to_i64()),
                Scalar::Timestamp(s.min().to_i64()),
            ),
            Statistics::Float(s) => (Scalar::from(*s.max()), Scalar::from(*s.min())),
            Statistics::Double(s) => (Scalar::from(*s.max()), Scalar::from(*s.min())),
            Statistics::ByteArray(s) => (
                Scalar::String(s.max().as_bytes().to_vec()),
                Scalar::String(s.max().as_bytes().to_vec()),
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
                    _ => unreachable!(),
                }
            }
        }
    } else {
        (Scalar::Null, Scalar::Null)
    };
    ColumnStatistics::new(
        min,
        max,
        s.null_count(),
        0, // this field is not used.
        s.distinct_count(),
    )
}

fn decode_decimal128_from_bytes(arr: &FixedLenByteArray, size: DecimalSize) -> Scalar {
    let v = i128::from_be_bytes(sign_extend_be(arr.as_bytes()));
    Scalar::Decimal(DecimalScalar::Decimal128(v, size))
}

fn decode_decimal256_from_bytes(arr: &FixedLenByteArray, size: DecimalSize) -> Scalar {
    let v = i256::from_be_bytes(sign_extend_be(arr.as_bytes()));
    let (lo, hi) = v.to_parts();
    let v = I256::from_words(hi, lo as i128);
    Scalar::Decimal(DecimalScalar::Decimal256(v, size))
}

// from arrow-rs
fn sign_extend_be<const N: usize>(b: &[u8]) -> [u8; N] {
    assert!(b.len() <= N, "Array too large, expected less than {N}");
    let is_negative = (b[0] & 128u8) == 128u8;
    let mut result = if is_negative { [255u8; N] } else { [0u8; N] };
    for (d, s) in result.iter_mut().skip(N - b.len()).zip(b) {
        *d = *s;
    }
    result
}
