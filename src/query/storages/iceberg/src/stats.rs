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

use common_expression::types::Number;
use common_expression::types::NumberDataType;
use common_expression::types::F32;
use common_expression::types::F64;
use common_expression::with_integer_mapped_type;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use icelake::types::DataFile;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::StatisticsOfColumns;

/// Try to convert statistics in [`DataFile`] to [`StatisticsOfColumns`].
pub fn get_stats_of_data_file(schema: &TableSchema, df: &DataFile) -> Option<StatisticsOfColumns> {
    match (&df.lower_bounds, &df.upper_bounds, &df.null_value_counts) {
        (Some(lower), Some(upper), Some(null_counts)) => {
            let mut stats: HashMap<u32, ColumnStatistics> =
                HashMap::with_capacity(schema.num_fields());
            for field in schema.fields.iter() {
                if let Some(stat) =
                    get_column_stats(field, lower, upper, null_counts, &df.distinct_counts)
                {
                    stats.insert(field.column_id, stat);
                }
            }
            Some(stats)
        }
        (_, _, _) => None,
    }
}

/// Try get [`ColumnStatistics`] for one column.
fn get_column_stats(
    field: &TableField,
    lower: &HashMap<i32, Vec<u8>>,
    upper: &HashMap<i32, Vec<u8>>,
    null_counts: &HashMap<i32, i64>,
    distinct_counts: &Option<HashMap<i32, i64>>,
) -> Option<ColumnStatistics> {
    // The column id in iceberg is 1-based while the column id in Databend is 0-based.
    let iceberg_col_id = field.column_id as i32 + 1;
    match (
        lower.get(&iceberg_col_id),
        upper.get(&iceberg_col_id),
        null_counts.get(&iceberg_col_id),
    ) {
        (Some(lo), Some(up), Some(nc)) => {
            let min = parse_binary_value(&field.data_type, lo)?;
            let max = parse_binary_value(&field.data_type, up)?;
            let distinct_of_values = distinct_counts
                .as_ref()
                .and_then(|dc| dc.get(&iceberg_col_id))
                .map(|dc| *dc as u64);
            Some(ColumnStatistics {
                min,
                max,
                null_count: *nc as u64,
                in_memory_size: 0, // this field is not used.
                distinct_of_values,
            })
        }
        (_, _, _) => None,
    }
}

/// Deserialize binary value to [`Scalar`] according to [Binary single-value serialization](https://iceberg.apache.org/spec/#binary-single-value-serialization)
fn parse_binary_value(ty: &TableDataType, data: &[u8]) -> Option<Scalar> {
    let ty = ty.remove_nullable();
    match ty {
        TableDataType::Boolean => Some(Scalar::Boolean(data[0] != 0)),
        TableDataType::Number(ty) => with_integer_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                let v = NUM_TYPE::from_le_bytes(data.try_into().ok()?);
                Some(Scalar::Number(NUM_TYPE::upcast_scalar(v)))
            }
            NumberDataType::Float32 => {
                let v = f32::from_le_bytes(data.try_into().ok()?);
                Some(Scalar::Number(F32::upcast_scalar(F32::from(v))))
            }
            NumberDataType::Float64 => {
                let v = f64::from_le_bytes(data.try_into().ok()?);
                Some(Scalar::Number(F64::upcast_scalar(F64::from(v))))
            }
        }),
        TableDataType::Date => {
            let v = i32::from_le_bytes(data.try_into().ok()?);
            Some(Scalar::Date(v))
        }
        TableDataType::Timestamp => {
            let v = i64::from_le_bytes(data.try_into().ok()?);
            Some(Scalar::Timestamp(v))
        }
        TableDataType::String => Some(Scalar::String(data.to_vec())),
        // TODO: support Decimal.
        _ => None, // Not supported.
    }
}
