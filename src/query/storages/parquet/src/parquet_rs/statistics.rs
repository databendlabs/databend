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
use common_expression::types::DataType;
use common_expression::types::DecimalDataType;
use common_expression::types::DecimalSize;
use common_expression::types::NumberDataType;
use common_expression::Scalar;
use common_expression::TableDataType;
use ethnum::I256;
use parquet::data_type::AsBytes;
use parquet::data_type::FixedLenByteArray;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::StatisticsOfColumns;

use crate::parquet_rs::column_nodes::ColumnNodesRS;

/// Collect statistics of a batch of row groups of the specified columns.
///
/// The returned vector's length is the same as `rgs`.
pub fn collect_row_group_stats(
    column_nodes: &ColumnNodesRS,
    rgs: &[RowGroupMetaData],
) -> Result<Vec<StatisticsOfColumns>> {
    let name_to_idx = rgs[0]
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, col)| (col.column_path().parts()[0].clone(), idx))
        .collect::<HashMap<String, usize>>();
    let column_ids = column_nodes
        .column_nodes
        .iter()
        .map(|column_node| name_to_idx[column_node.field.name()])
        .collect::<Vec<_>>();

    let mut stats = Vec::with_capacity(rgs.len());
    for rg in rgs {
        let mut stats_of_columns = HashMap::with_capacity(column_ids.len());

        // Each row_group_stat is a `HashMap` holding key-value pairs.
        // The first element of the pair is the offset in the schema,
        // and the second element is the statistics of the column (according to the offset)
        // `column_nodes` is parallel to the schema, so we can iterate `column_nodes` directly.
        for (index, column_node) in column_nodes.column_nodes.iter().enumerate() {
            let column_stats = rg.column(column_ids[index]).statistics().unwrap();
            stats_of_columns.insert(
                index,
                convert_column_statistics(
                    column_stats,
                    (&TableDataType::try_from(&column_node.field).unwrap()).into(),
                ),
            );
        }
        stats.push(StatisticsOfColumns::default());
    }
    Ok(stats)
}

/// according to https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
fn convert_column_statistics(s: &Statistics, typ: DataType) -> ColumnStatistics {
    let (max, min) = if s.has_min_max_set() {
        match s {
            Statistics::Boolean(s) => (Scalar::Boolean(*s.max()), Scalar::Boolean(*s.min())),
            Statistics::Int32(s) => {
                let (max, min) = (*s.max(), *s.min());
                match typ {
                    DataType::Number(NumberDataType::Int8) => {
                        (Scalar::from(max as i8), Scalar::from(min as i8))
                    }
                    DataType::Number(NumberDataType::Int16) => {
                        (Scalar::from(max as i16), Scalar::from(min as i16))
                    }
                    DataType::Number(NumberDataType::Int32) => {
                        (Scalar::from(max), Scalar::from(min))
                    }
                    DataType::Number(NumberDataType::UInt8) => {
                        (Scalar::from(max as u8), Scalar::from(min as u8))
                    }
                    DataType::Number(NumberDataType::UInt16) => {
                        (Scalar::from(max as u16), Scalar::from(min as u16))
                    }
                    DataType::Number(NumberDataType::UInt32) => {
                        (Scalar::from(max as u32), Scalar::from(min as u32))
                    }
                    DataType::Date => (Scalar::Date(max), Scalar::Date(min)),
                    DataType::Decimal(DecimalDataType::Decimal128(size)) => (
                        Scalar::Decimal(DecimalScalar::Decimal128(i128::from(max), size)),
                        Scalar::Decimal(DecimalScalar::Decimal128(i128::from(min), size)),
                    ),
                    DataType::Decimal(DecimalDataType::Decimal256(size)) => (
                        Scalar::Decimal(DecimalScalar::Decimal256(
                            I256::from_i64(max as i64),
                            size,
                        )),
                        Scalar::Decimal(DecimalScalar::Decimal256(
                            I256::from_i64(min as i64),
                            size,
                        )),
                    ),
                    _ => unreachable!(),
                }
            }
            Statistics::Int64(s) => {
                let (max, min) = (*s.max(), *s.min());
                match typ {
                    DataType::Number(NumberDataType::UInt64) => {
                        (Scalar::from(max as u64), Scalar::from(min as u64))
                    }
                    DataType::Number(NumberDataType::Int64) => {
                        (Scalar::from(max), Scalar::from(min))
                    }
                    DataType::Timestamp => (Scalar::Timestamp(max), Scalar::Timestamp(min)),
                    DataType::Decimal(DecimalDataType::Decimal128(size)) => (
                        Scalar::Decimal(DecimalScalar::Decimal128(i128::from(max), size)),
                        Scalar::Decimal(DecimalScalar::Decimal128(i128::from(min), size)),
                    ),
                    DataType::Decimal(DecimalDataType::Decimal256(size)) => (
                        Scalar::Decimal(DecimalScalar::Decimal256(I256::from_i64(max), size)),
                        Scalar::Decimal(DecimalScalar::Decimal256(I256::from_i64(min), size)),
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
                    DataType::Decimal(DecimalDataType::Decimal128(size)) => (
                        decode_decimal128_from_bytes(max, size),
                        decode_decimal128_from_bytes(min, size),
                    ),
                    DataType::Decimal(DecimalDataType::Decimal256(size)) => (
                        decode_decimal256_from_bytes(max, size),
                        decode_decimal256_from_bytes(min, size),
                    ),
                    _ => unreachable!(),
                }
            }
        }
    } else {
        (Scalar::Null, Scalar::Null)
    };
    ColumnStatistics {
        min,
        max,
        null_count: s.null_count(),
        in_memory_size: 0, // this field is not used.
        distinct_of_values: s.distinct_count(),
    }
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
