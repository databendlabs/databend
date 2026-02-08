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

use databend_common_base::base::OrderedFloat;
use databend_common_catalog::statistics;
use databend_common_catalog::statistics::BasicColumnStatistics;
use databend_common_catalog::table::ColumnStatisticsProvider;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::Decimal;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::F32;
use databend_common_expression::types::F64;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::Number;
use iceberg::spec::DataContentType;
use iceberg::spec::Datum;
use iceberg::spec::ManifestStatus;
use iceberg::spec::PrimitiveLiteral;
use iceberg::spec::PrimitiveType;
use uuid::Uuid;

use crate::IcebergTable;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Default)]
pub struct IcebergStatistics {
    /// Number of records in this table
    pub record_count: u64,
    /// Total table size in bytes
    pub file_size_in_bytes: u64,
    /// Number of manifest files in this table
    pub number_of_manifest_files: u64,
    /// Number of data files in this table
    pub number_of_data_files: u64,

    /// Computed statistics for each column
    pub computed_statistics: HashMap<ColumnId, statistics::BasicColumnStatistics>,
}

impl IcebergStatistics {
    /// Get statistics of an iceberg table.
    pub async fn parse(table: &iceberg::table::Table) -> Result<IcebergStatistics> {
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok(IcebergStatistics::default());
        };

        // Map from column id to the total size on disk of all regions that
        // store the column. Does not include bytes necessary to read other
        // columns, like footers. Leave null for row-oriented formats (Avro)
        let mut column_sizes: HashMap<i32, u64> = HashMap::new();
        // Map from column id to number of values in the column (including null
        // and NaN values)
        let mut value_counts: HashMap<i32, u64> = HashMap::new();
        // Map from column id to number of null values in the column
        let mut null_value_counts: HashMap<i32, u64> = HashMap::new();
        // Map from column id to number of NaN values in the column
        let mut nan_value_counts: HashMap<i32, u64> = HashMap::new();
        // Map from column id to lower bound in the column serialized as biary.
        // Each value must be less than or equal to all non-null, non-NaN values
        // in the column for the file.
        // Reference:
        //
        // - [Binary single-value serialization](https://iceberg.apache.org/spec/#binary-single-value-serialization)
        let mut lower_bounds: HashMap<i32, Datum> = HashMap::new();
        // Map from column id to upper bound in the column serialized as binary.
        // Each value must be greater than or equal to all non-null, non-Nan
        // values in the column for the file.
        //
        // Reference:
        //
        // - [Binary single-value serialization](https://iceberg.apache.org/spec/#binary-single-value-serialization)
        let mut upper_bounds: HashMap<i32, Datum> = HashMap::new();

        let mut statistics = IcebergStatistics::default();

        let manifest = snapshot
            .load_manifest_list(table.file_io(), table.metadata())
            .await
            .map_err(|err| ErrorCode::Internal(format!("load manifest list error: {err:?}")))?;

        for manifest_file in manifest.entries() {
            statistics.number_of_manifest_files += 1;

            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .map_err(|err| ErrorCode::Internal(format!("load manifest file error: {err:?}")))?;

            manifest.entries().iter().for_each(|entry| {
                if entry.status() == ManifestStatus::Deleted {
                    return;
                }
                let data_file = entry.data_file();
                if data_file.content_type() != DataContentType::Data {
                    return;
                }

                statistics.record_count += data_file.record_count();
                statistics.file_size_in_bytes += data_file.file_size_in_bytes();
                statistics.number_of_data_files += 1;

                data_file.column_sizes().iter().for_each(|(col_id, size)| {
                    *column_sizes.entry(*col_id).or_default() += size;
                });
                data_file.value_counts().iter().for_each(|(col_id, count)| {
                    *value_counts.entry(*col_id).or_default() += count;
                });
                data_file
                    .null_value_counts()
                    .iter()
                    .for_each(|(col_id, count)| {
                        *null_value_counts.entry(*col_id).or_default() += count;
                    });
                data_file
                    .nan_value_counts()
                    .iter()
                    .for_each(|(col_id, count)| {
                        *nan_value_counts.entry(*col_id).or_default() += *count;
                    });

                data_file
                    .lower_bounds()
                    .iter()
                    .for_each(|(&col_id, new_value)| {
                        lower_bounds
                            .entry(col_id)
                            .and_modify(|existing_value| {
                                if new_value < existing_value {
                                    *existing_value = new_value.clone();
                                }
                            })
                            .or_insert_with(|| new_value.clone());
                    });

                data_file
                    .upper_bounds()
                    .iter()
                    .for_each(|(&col_id, new_value)| {
                        upper_bounds
                            .entry(col_id)
                            .and_modify(|existing_value| {
                                if new_value > existing_value {
                                    *existing_value = new_value.clone();
                                }
                            })
                            .or_insert_with(|| new_value.clone());
                    });
            });
        }

        let mut computed_statistics = HashMap::new();
        for field in IcebergTable::get_schema(table)?.fields() {
            let column_stats = get_column_stats(
                field,
                &column_sizes,
                &lower_bounds,
                &upper_bounds,
                &null_value_counts,
            );
            computed_statistics.insert(field.column_id, column_stats);
        }

        statistics.computed_statistics = computed_statistics;
        Ok(statistics)
    }
}

impl ColumnStatisticsProvider for IcebergStatistics {
    fn column_statistics(&self, column_id: ColumnId) -> Option<&statistics::BasicColumnStatistics> {
        self.computed_statistics.get(&column_id)
    }

    fn num_rows(&self) -> Option<u64> {
        Some(self.record_count)
    }

    fn stats_num_rows(&self) -> Option<u64> {
        Some(self.record_count)
    }

    fn average_size(&self, column_id: ColumnId) -> Option<u64> {
        self.computed_statistics
            .get(&column_id)
            .and_then(|v| v.in_memory_size.checked_div(self.record_count))
    }
}

/// Try get [`ColumnStatistics`] for one column.
fn get_column_stats(
    field: &TableField,
    column_size: &HashMap<i32, u64>,
    lower: &HashMap<i32, Datum>,
    upper: &HashMap<i32, Datum>,
    null_counts: &HashMap<i32, u64>,
) -> BasicColumnStatistics {
    let iceberg_col_id = field.column_id as i32;
    BasicColumnStatistics {
        min: lower
            .get(&iceberg_col_id)
            .and_then(parse_datum)
            .and_then(Scalar::to_datum),
        max: upper
            .get(&iceberg_col_id)
            .and_then(parse_datum)
            .and_then(Scalar::to_datum),
        ndv: None,
        null_count: null_counts
            .get(&iceberg_col_id)
            .copied()
            .unwrap_or_default(),
        in_memory_size: column_size
            .get(&iceberg_col_id)
            .copied()
            .unwrap_or_default(),
    }
}

/// Try to parse iceberg [`Datum`] to databend [`Scalar`].
pub fn parse_datum(data: &Datum) -> Option<Scalar> {
    match data.literal() {
        PrimitiveLiteral::Boolean(v) => Some(Scalar::Boolean(*v)),
        PrimitiveLiteral::Int(v) => Some(Scalar::Number(i32::upcast_scalar(*v))),
        PrimitiveLiteral::Long(v) => Some(Int64Type::upcast_scalar(*v)),
        PrimitiveLiteral::Float(v) => {
            Some(Scalar::Number(F32::upcast_scalar(OrderedFloat::from(v.0))))
        }
        PrimitiveLiteral::Double(v) => {
            Some(Scalar::Number(F64::upcast_scalar(OrderedFloat::from(v.0))))
        }
        PrimitiveLiteral::String(v) => Some(Scalar::String(v.clone())),
        PrimitiveLiteral::Binary(v) => Some(Scalar::Binary(v.clone())),
        // Iceberg use i128 to represent decimal
        PrimitiveLiteral::Int128(v) => {
            if let PrimitiveType::Decimal { precision, scale } = data.data_type() {
                Some(Scalar::Decimal(v.to_scalar(DecimalSize::new_unchecked(
                    *precision as u8,
                    *scale as u8,
                ))))
            } else {
                None
            }
        }
        // Iceberg use u128 to represent uuid
        PrimitiveLiteral::UInt128(v) => {
            Some(Scalar::String(Uuid::from_u128(*v).as_simple().to_string()))
        }
        PrimitiveLiteral::AboveMax => None,
        PrimitiveLiteral::BelowMin => None,
    }
}
