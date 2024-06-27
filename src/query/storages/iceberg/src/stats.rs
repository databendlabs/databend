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

use databend_common_expression::types::Number;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::F32;
use databend_common_expression::types::F64;
use databend_common_expression::with_integer_mapped_type;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use iceberg::spec::DataFile;
use iceberg::spec::Datum;
use iceberg::spec::PrimitiveLiteral;

/// Try to convert statistics in [`DataFile`] to [`StatisticsOfColumns`].
pub fn get_stats_of_data_file(schema: &TableSchema, df: &DataFile) -> Option<StatisticsOfColumns> {
    let mut stats: HashMap<u32, ColumnStatistics> = HashMap::with_capacity(schema.num_fields());
    for field in schema.fields.iter() {
        if let Some(stat) = get_column_stats(
            field,
            df.lower_bounds(),
            df.upper_bounds(),
            df.null_value_counts(),
        ) {
            stats.insert(field.column_id, stat);
        }
    }
    Some(stats)
}

/// Try get [`ColumnStatistics`] for one column.
fn get_column_stats(
    field: &TableField,
    lower: &HashMap<i32, Datum>,
    upper: &HashMap<i32, Datum>,
    null_counts: &HashMap<i32, u64>,
) -> Option<ColumnStatistics> {
    // The column id in iceberg is 1-based while the column id in Databend is 0-based.
    let iceberg_col_id = field.column_id as i32 + 1;
    match (
        lower.get(&iceberg_col_id),
        upper.get(&iceberg_col_id),
        null_counts.get(&iceberg_col_id),
    ) {
        (Some(lo), Some(up), Some(nc)) => {
            let min = parse_datum(&field.data_type, lo)?;
            let max = parse_datum(&field.data_type, up)?;
            Some(ColumnStatistics::new(
                min, max, *nc as u64, 0, // this field is not used.
                None,
            ))
        }
        (_, _, _) => None,
    }
}

/// TODO: we need to support more types.
fn parse_datum(ty: &TableDataType, data: &Datum) -> Option<Scalar> {
    let ty = ty.remove_nullable();
    match data.literal() {
        PrimitiveLiteral::Boolean(v) => Some(Scalar::Boolean(*v)),
        PrimitiveLiteral::Int(v) => Some(Scalar::Number(i32::upcast_scalar(*v))),
        PrimitiveLiteral::Long(v) => Some(Scalar::Number(i64::upcast_scalar(*v))),
        PrimitiveLiteral::Float(v) => Some(Scalar::Number(F32::upcast_scalar(F32::from(v)))),
        PrimitiveLiteral::Double(v) => Some(Scalar::Number(F64::upcast_scalar(F64::from(v)))),
        PrimitiveLiteral::Date(v) => Some(Scalar::Date(*v)),
        PrimitiveLiteral::Timestamp(v) => Some(Scalar::Timestamp(*v)),
        PrimitiveLiteral::String(v) => Some(Scalar::String(v.clone())),
        _ => None,
    }
}
