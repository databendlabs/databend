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

use std::collections::BTreeMap;

use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use paimon::spec::DataFileMeta;

use super::decode_key;
use super::decode_stats_json;
use super::format_partition;
use super::json;
use super::partition_fields;
use super::trimmed_key_columns;
use super::value_stats_columns;
use crate::error::map_paimon_result;

pub async fn read(table: &paimon::Table) -> Result<DataBlock> {
    let read_builder = table.new_read_builder();
    // `with_scan_all_files` keeps every data file (including lower levels merged
    // away by a normal read plan) so the files table lists the full file set.
    let plan = map_paimon_result(read_builder.new_scan().with_scan_all_files().plan().await)?;
    let part_fields = partition_fields(table)?;
    let key_columns = trimmed_key_columns(table);
    let field_names: Vec<String> = table
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect();

    let mut partition = Vec::new();
    let mut bucket = Vec::new();
    let mut file_path = Vec::new();
    let mut file_format = Vec::new();
    let mut schema_id = Vec::new();
    let mut level = Vec::new();
    let mut record_count = Vec::new();
    let mut file_size = Vec::new();
    let mut min_key = Vec::new();
    let mut max_key = Vec::new();
    let mut null_value_counts = Vec::new();
    let mut min_value_stats = Vec::new();
    let mut max_value_stats = Vec::new();
    let mut min_sequence_number = Vec::new();
    let mut max_sequence_number = Vec::new();
    let mut creation_time = Vec::new();
    let mut delete_row_count = Vec::new();
    let mut file_source = Vec::new();
    let mut first_row_id = Vec::new();

    // `write_cols` is `nullable(Array(nullable string))`; build it through a
    // type-driven column builder so the nested nullability matches the schema.
    let write_cols_type = DataType::Nullable(Box::new(DataType::Array(Box::new(
        DataType::Nullable(Box::new(DataType::String)),
    ))));
    let inner_element_type = DataType::Nullable(Box::new(DataType::String));
    let mut write_cols_builder = ColumnBuilder::with_capacity(&write_cols_type, 0);

    for split in plan.splits() {
        let partition_str = format_partition(split.partition(), &part_fields)?;
        for file in split.data_files() {
            partition.push(partition_str.clone());
            bucket.push(split.bucket());
            file_path.push(format!(
                "{}/{}",
                split.bucket_path().trim_end_matches('/'),
                file.file_name
            ));
            file_format.push(file_format_of(&file.file_name));
            schema_id.push(file.schema_id);
            level.push(file.level);
            record_count.push(file.row_count);
            file_size.push(file.file_size);
            min_key.push(decode_key(&file.min_key, &key_columns));
            max_key.push(decode_key(&file.max_key, &key_columns));
            null_value_counts.push(null_value_counts_json(file, &field_names)?);
            let stats_columns = value_stats_columns(
                table,
                file.value_stats_cols.as_ref(),
                file.write_cols.as_ref(),
            );
            min_value_stats.push(
                decode_stats_json(file.value_stats.min_values(), &stats_columns)?
                    .unwrap_or_else(|| "{}".to_string()),
            );
            max_value_stats.push(
                decode_stats_json(file.value_stats.max_values(), &stats_columns)?
                    .unwrap_or_else(|| "{}".to_string()),
            );
            min_sequence_number.push(Some(file.min_sequence_number));
            max_sequence_number.push(Some(file.max_sequence_number));
            creation_time.push(file.creation_time.map(|time| time.timestamp_micros()));
            delete_row_count.push(file.delete_row_count);
            file_source.push(file.file_source.map(file_source_name));
            first_row_id.push(file.first_row_id);

            match &file.write_cols {
                Some(cols) => {
                    let mut element_builder =
                        ColumnBuilder::with_capacity(&inner_element_type, cols.len());
                    for col in cols {
                        element_builder.push(ScalarRef::String(col));
                    }
                    write_cols_builder.push(ScalarRef::Array(element_builder.build()));
                }
                None => write_cols_builder.push(ScalarRef::Null),
            }
        }
    }

    Ok(DataBlock::new_from_columns(vec![
        StringType::from_opt_data(partition),
        Int32Type::from_data(bucket),
        StringType::from_data(file_path),
        StringType::from_data(file_format),
        Int64Type::from_data(schema_id),
        Int32Type::from_data(level),
        Int64Type::from_data(record_count),
        Int64Type::from_data(file_size),
        StringType::from_opt_data(min_key),
        StringType::from_opt_data(max_key),
        StringType::from_data(null_value_counts),
        StringType::from_data(min_value_stats),
        StringType::from_data(max_value_stats),
        Int64Type::from_opt_data(min_sequence_number),
        Int64Type::from_opt_data(max_sequence_number),
        TimestampType::from_opt_data(creation_time),
        Int64Type::from_opt_data(delete_row_count),
        StringType::from_opt_data(file_source),
        Int64Type::from_opt_data(first_row_id),
        write_cols_builder.build(),
    ]))
}

fn file_format_of(file_name: &str) -> String {
    file_name
        .rsplit_once('.')
        .map(|(_, ext)| ext.to_ascii_lowercase())
        .unwrap_or_else(|| "parquet".to_string())
}

fn file_source_name(source: i32) -> String {
    match source {
        0 => "APPEND".to_string(),
        1 => "COMPACT".to_string(),
        other => other.to_string(),
    }
}

fn null_value_counts_json(file: &DataFileMeta, field_names: &[String]) -> Result<String> {
    let counts = file.value_stats.null_counts();
    let names: Vec<&str> = match &file.value_stats_cols {
        Some(cols) => cols.iter().map(|col| col.as_str()).collect(),
        None => field_names.iter().map(|name| name.as_str()).collect(),
    };
    let map: BTreeMap<&str, Option<i64>> = names.into_iter().zip(counts.iter().copied()).collect();
    json(&map, "files null_value_counts")
}
