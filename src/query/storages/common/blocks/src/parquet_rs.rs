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

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::converts::arrow::table_schema_to_arrow_schema_ignore_inside_nullable;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchema;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::table::TableCompression;
use ethnum::I256;
use parquet_rs::arrow::arrow_to_parquet_schema;
use parquet_rs::arrow::ArrowWriter;
use parquet_rs::basic::Encoding;
use parquet_rs::basic::Type as PhysicalType;
use parquet_rs::file::properties::EnabledStatistics;
use parquet_rs::file::properties::WriterProperties;
use parquet_rs::file::properties::WriterPropertiesBuilder;
use parquet_rs::format::FileMetaData;
use parquet_rs::schema::types::ColumnPath;
use parquet_rs::schema::types::Type;
use parquet_rs::schema::types::TypePtr;

/// Serialize data blocks to parquet format.
pub fn blocks_to_parquet(
    table_schema: &TableSchema,
    blocks: Vec<DataBlock>,
    write_buffer: &mut Vec<u8>,
    compression: TableCompression,
    stat: &StatisticsOfColumns,
) -> Result<FileMetaData> {
    assert!(!blocks.is_empty());
    let mut props_builder = WriterProperties::builder()
        .set_compression(compression.into())
        // use `usize::MAX` to effectively limit the number of row groups to 1
        .set_max_row_group_size(usize::MAX)
        // this is a global setting, will be covered by the column level setting(if set)
        .set_encoding(Encoding::PLAIN)
        // ditto
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_bloom_filter_enabled(false);
    let arrow_schema = Arc::new(table_schema_to_arrow_schema_ignore_inside_nullable(
        table_schema,
    ));
    let parquet_schema = arrow_to_parquet_schema(&arrow_schema)?;
    if blocks.len() == 1 {
        // doesn't not cover the case of multiple blocks for now. to simplify the implementation
        props_builder = choose_compression_scheme(
            props_builder,
            &blocks[0],
            parquet_schema.root_schema().get_fields(),
            table_schema,
            stat,
        )?;
    }
    let props = props_builder.build();
    let batches = blocks
        .into_iter()
        .map(|block| block.to_record_batch(table_schema))
        .collect::<Result<Vec<_>>>()?;

    let mut writer = ArrowWriter::try_new(write_buffer, arrow_schema, Some(props))?;
    for batch in batches {
        writer.write(&batch)?;
    }
    let file_meta = writer.close()?;
    Ok(file_meta)
}

fn choose_compression_scheme(
    mut props: WriterPropertiesBuilder,
    block: &DataBlock,
    parquet_fields: &[TypePtr],
    table_schema: &TableSchema,
    stat: &StatisticsOfColumns,
) -> Result<WriterPropertiesBuilder> {
    for ((parquet_field, table_field), col) in parquet_fields
        .iter()
        .zip(table_schema.fields.iter())
        .zip(block.columns())
    {
        match parquet_field.as_ref() {
            Type::PrimitiveType {
                basic_info: _,
                physical_type,
                type_length: _,
                scale: _,
                precision: _,
            } => {
                let distinct_of_values = stat
                    .get(&table_field.column_id)
                    .and_then(|stat| stat.distinct_of_values);
                let num_rows = block.num_rows();
                if can_apply_dict_encoding(physical_type, distinct_of_values, num_rows, col)? {
                    let col_path = ColumnPath::new(vec![table_field.name().clone()]);
                    props = props.set_column_dictionary_enabled(col_path, true);
                } else if can_apply_delta_binary_pack(physical_type, col, num_rows)? {
                    let col_path = ColumnPath::new(vec![table_field.name().clone()]);
                    props = props.set_column_encoding(col_path, Encoding::DELTA_BINARY_PACKED);
                }
            }
            Type::GroupType {
                basic_info: _,
                fields: _,
            } => {} // TODO: handle nested fields
        }
    }
    Ok(props)
}

fn can_apply_dict_encoding(
    physical_type: &PhysicalType,
    distinct_of_values: Option<u64>,
    num_rows: usize,
    col: &BlockEntry,
) -> Result<bool> {
    const LOW_CARDINALITY_THRESHOLD: f64 = 10.0;
    const AVG_BYTES_PER_VALUE: f64 = 10.0;
    if !matches!(physical_type, PhysicalType::BYTE_ARRAY) {
        return Ok(false);
    }
    let is_low_cardinality = distinct_of_values
        .is_some_and(|ndv| num_rows as f64 / ndv as f64 > LOW_CARDINALITY_THRESHOLD);
    let column = col.value.convert_to_full_column(&col.data_type, num_rows);
    let memory_size = column.memory_size();
    let total_bytes = memory_size - num_rows * 8;
    let avg_bytes_per_value = total_bytes as f64 / num_rows as f64;
    Ok(is_low_cardinality && avg_bytes_per_value < AVG_BYTES_PER_VALUE)
}

fn can_apply_delta_binary_pack(
    physical_type: &PhysicalType,
    col: &BlockEntry,
    num_rows: usize,
) -> Result<bool> {
    const MAX_DELTA: i64 = 1 << 3;
    if !matches!(
        physical_type,
        PhysicalType::INT32 | PhysicalType::INT64 | PhysicalType::INT96
    ) {
        return Ok(false);
    }
    if num_rows == 0 {
        return Ok(false);
    }
    let col = col
        .value
        .convert_to_full_column(&col.data_type, num_rows)
        .remove_nullable();
    match col.data_type().remove_nullable() {
        DataType::Number(NumberDataType::UInt8) => {
            let mut max_delta = 0;
            let mut min_delta = u8::MAX;
            let col = col.as_number().unwrap().as_u_int8().unwrap();
            let mut col_iter = col.iter();
            let mut prev = *col_iter.next().unwrap();
            for &v in col_iter {
                let delta = if v > prev { v - prev } else { prev - v };
                if delta > max_delta {
                    max_delta = delta;
                }
                if delta < min_delta {
                    min_delta = delta;
                }
                prev = v;
            }
            Ok((max_delta - min_delta) as i64 <= MAX_DELTA)
        }
        DataType::Number(NumberDataType::UInt16) => {
            let mut max_delta = 0;
            let mut min_delta = u16::MAX;
            let col = col.as_number().unwrap().as_u_int16().unwrap();
            let mut col_iter = col.iter();
            let mut prev = *col_iter.next().unwrap();
            for &v in col_iter {
                let delta = if v > prev { v - prev } else { prev - v };
                if delta > max_delta {
                    max_delta = delta;
                }
                if delta < min_delta {
                    min_delta = delta;
                }
                prev = v;
            }
            Ok((max_delta - min_delta) as i64 <= MAX_DELTA)
        }
        DataType::Number(NumberDataType::UInt32) => {
            let mut max_delta = 0;
            let mut min_delta = u32::MAX;
            let col = col.as_number().unwrap().as_u_int32().unwrap();
            let mut col_iter = col.iter();
            let mut prev = *col_iter.next().unwrap();
            for &v in col_iter {
                let delta = if v > prev { v - prev } else { prev - v };
                if delta > max_delta {
                    max_delta = delta;
                }
                if delta < min_delta {
                    min_delta = delta;
                }
                prev = v;
            }
            Ok((max_delta - min_delta) as i64 <= MAX_DELTA)
        }
        DataType::Number(NumberDataType::UInt64) => {
            let mut max_delta = 0;
            let mut min_delta = u64::MAX;
            let col = col.as_number().unwrap().as_u_int64().unwrap();
            let mut col_iter = col.iter();
            let mut prev = *col_iter.next().unwrap();
            for &v in col_iter {
                let delta = if v > prev { v - prev } else { prev - v };
                if delta > max_delta {
                    max_delta = delta;
                }
                if delta < min_delta {
                    min_delta = delta;
                }
                prev = v;
            }
            Ok((max_delta - min_delta) <= MAX_DELTA as u64)
        }
        DataType::Number(NumberDataType::Int8) => {
            let mut max_delta = 0;
            let mut min_delta = i8::MAX;
            let col = col.as_number().unwrap().as_int8().unwrap();
            let mut col_iter = col.iter();
            let mut prev = *col_iter.next().unwrap();
            for &v in col_iter {
                let delta = if v > prev { v - prev } else { prev - v };
                if delta > max_delta {
                    max_delta = delta;
                }
                if delta < min_delta {
                    min_delta = delta;
                }
                prev = v;
            }
            Ok((max_delta - min_delta) as i64 <= MAX_DELTA)
        }
        DataType::Number(NumberDataType::Int16) => {
            let mut max_delta = 0;
            let mut min_delta = i16::MAX;
            let col = col.as_number().unwrap().as_int16().unwrap();
            let mut col_iter = col.iter();
            let mut prev = *col_iter.next().unwrap();
            for &v in col_iter {
                let delta = if v > prev { v - prev } else { prev - v };
                if delta > max_delta {
                    max_delta = delta;
                }
                if delta < min_delta {
                    min_delta = delta;
                }
                prev = v;
            }
            Ok((max_delta - min_delta) as i64 <= MAX_DELTA)
        }
        DataType::Number(NumberDataType::Int32) => {
            let mut max_delta = 0;
            let mut min_delta = i32::MAX;
            let col = col.as_number().unwrap().as_int32().unwrap();
            let mut col_iter = col.iter();
            let mut prev = *col_iter.next().unwrap();
            for &v in col_iter {
                let delta = if v > prev { v - prev } else { prev - v };
                if delta > max_delta {
                    max_delta = delta;
                }
                if delta < min_delta {
                    min_delta = delta;
                }
                prev = v;
            }
            Ok((max_delta - min_delta) as i64 <= MAX_DELTA)
        }
        DataType::Number(NumberDataType::Int64) => {
            let mut max_delta = 0;
            let mut min_delta = i64::MAX;
            let col = col.as_number().unwrap().as_int64().unwrap();
            let mut col_iter = col.iter();
            let mut prev = *col_iter.next().unwrap();
            for &v in col_iter {
                let delta = if v > prev { v - prev } else { prev - v };
                if delta > max_delta {
                    max_delta = delta;
                }
                if delta < min_delta {
                    min_delta = delta;
                }
                prev = v;
            }
            Ok((max_delta - min_delta) <= MAX_DELTA)
        }
        DataType::Decimal(DecimalDataType::Decimal128(_)) => {
            let mut max_delta = 0;
            let mut min_delta = i128::MAX;
            let (col, _) = col.as_decimal().unwrap().as_decimal128().unwrap();
            let mut col_iter = col.iter();
            let mut prev = *col_iter.next().unwrap();
            for &v in col_iter {
                let delta = if v > prev { v - prev } else { prev - v };
                if delta > max_delta {
                    max_delta = delta;
                }
                if delta < min_delta {
                    min_delta = delta;
                }
                prev = v;
            }
            Ok((max_delta - min_delta) <= MAX_DELTA as i128)
        }
        DataType::Decimal(DecimalDataType::Decimal256(_)) => {
            let mut max_delta: I256 = I256::ZERO;
            let mut min_delta = I256::MAX;
            let (col, _) = col.as_decimal().unwrap().as_decimal256().unwrap();
            let mut col_iter = col.iter();
            let mut prev = *col_iter.next().unwrap();
            for &v in col_iter {
                let delta = if v > prev { v - prev } else { prev - v };
                if delta > max_delta {
                    max_delta = delta;
                }
                if delta < min_delta {
                    min_delta = delta;
                }
                prev = v;
            }
            Ok(max_delta - min_delta <= I256::from(MAX_DELTA))
        }
        DataType::Timestamp => {
            let mut max_delta = 0;
            let mut min_delta = i64::MAX;
            let col = col.as_timestamp().unwrap();
            let mut col_iter = col.iter();
            let mut prev = *col_iter.next().unwrap();
            for &v in col_iter {
                let delta = if v > prev { v - prev } else { prev - v };
                if delta > max_delta {
                    max_delta = delta;
                }
                if delta < min_delta {
                    min_delta = delta;
                }
                prev = v;
            }
            Ok((max_delta - min_delta) <= MAX_DELTA)
        }
        DataType::Date => {
            let mut max_delta = 0;
            let mut min_delta = i32::MAX;
            let col = col.as_date().unwrap();
            let mut col_iter = col.iter();
            let mut prev = *col_iter.next().unwrap();
            for &v in col_iter {
                let delta = if v > prev { v - prev } else { prev - v };
                if delta > max_delta {
                    max_delta = delta;
                }
                if delta < min_delta {
                    min_delta = delta;
                }
                prev = v;
            }
            Ok((max_delta - min_delta) as i64 <= MAX_DELTA)
        }
        _ => Err(ErrorCode::Internal(format!(
            "Unsupported data type for delta binary pack: {:?}",
            col.data_type()
        ))),
    }
}
