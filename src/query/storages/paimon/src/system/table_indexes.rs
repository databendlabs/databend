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

use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::StringType;
use paimon::SnapshotManager;
use paimon::spec::BinaryRow;
use paimon::spec::FileKind;
use paimon::spec::IndexManifest;

use super::format_partition;
use super::partition_fields;
use crate::error::map_paimon_result;

pub async fn read(table: &paimon::Table) -> Result<DataBlock> {
    let manager = SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let mut entries = match map_paimon_result(manager.get_latest_snapshot().await)? {
        Some(snapshot) => match snapshot.index_manifest() {
            Some(name) if !name.is_empty() => {
                let path = manager.manifest_path(name);
                map_paimon_result(IndexManifest::read(table.file_io(), &path).await)?
            }
            _ => Vec::new(),
        },
        None => Vec::new(),
    };
    // Keep only live index files, matching paimon-datafusion.
    entries.retain(|entry| entry.kind == FileKind::Add);
    entries.sort_by(|a, b| {
        a.partition
            .cmp(&b.partition)
            .then(a.bucket.cmp(&b.bucket))
            .then(a.index_file.index_type.cmp(&b.index_file.index_type))
            .then(a.index_file.file_name.cmp(&b.index_file.file_name))
    });

    let part_fields = partition_fields(table)?;

    let mut partition = Vec::with_capacity(entries.len());
    let mut bucket = Vec::with_capacity(entries.len());
    let mut index_type = Vec::with_capacity(entries.len());
    let mut file_name = Vec::with_capacity(entries.len());
    let mut file_size = Vec::with_capacity(entries.len());
    let mut row_count = Vec::with_capacity(entries.len());
    let mut row_range_start = Vec::with_capacity(entries.len());
    let mut row_range_end = Vec::with_capacity(entries.len());
    let mut index_field_id = Vec::with_capacity(entries.len());
    let mut index_field_name = Vec::with_capacity(entries.len());

    // dv_ranges: nullable(Array(Tuple(file_name, offset, length, cardinality))).
    let tuple_type = DataType::Tuple(vec![
        DataType::String,
        DataType::Number(NumberDataType::Int32),
        DataType::Number(NumberDataType::Int32),
        DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int64))),
    ]);
    let dv_ranges_type =
        DataType::Nullable(Box::new(DataType::Array(Box::new(tuple_type.clone()))));
    let mut dv_ranges_builder = ColumnBuilder::with_capacity(&dv_ranges_type, entries.len());

    for entry in &entries {
        partition.push(decode_partition(&entry.partition, &part_fields)?);
        bucket.push(entry.bucket);
        index_type.push(entry.index_file.index_type.clone());
        file_name.push(entry.index_file.file_name.clone());
        file_size.push(i64::from(entry.index_file.file_size));
        row_count.push(i64::from(entry.index_file.row_count));

        let global = entry.index_file.global_index_meta.as_ref();
        row_range_start.push(global.map(|meta| meta.row_range_start));
        row_range_end.push(global.map(|meta| meta.row_range_end));
        index_field_id.push(global.map(|meta| meta.index_field_id));
        index_field_name.push(global.and_then(|meta| field_name_by_id(table, meta.index_field_id)));

        match &entry.index_file.deletion_vectors_ranges {
            Some(ranges) => {
                let mut element_builder = ColumnBuilder::with_capacity(&tuple_type, ranges.len());
                for (data_file, meta) in ranges {
                    let cardinality = match meta.cardinality {
                        Some(value) => ScalarRef::Number(NumberScalar::Int64(value)),
                        None => ScalarRef::Null,
                    };
                    element_builder.push(ScalarRef::Tuple(vec![
                        ScalarRef::String(data_file),
                        ScalarRef::Number(NumberScalar::Int32(meta.offset)),
                        ScalarRef::Number(NumberScalar::Int32(meta.length)),
                        cardinality,
                    ]));
                }
                dv_ranges_builder.push(ScalarRef::Array(element_builder.build()));
            }
            None => dv_ranges_builder.push(ScalarRef::Null),
        }
    }

    Ok(DataBlock::new_from_columns(vec![
        StringType::from_opt_data(partition),
        Int32Type::from_data(bucket),
        StringType::from_data(index_type),
        StringType::from_data(file_name),
        Int64Type::from_data(file_size),
        Int64Type::from_data(row_count),
        dv_ranges_builder.build(),
        Int64Type::from_opt_data(row_range_start),
        Int64Type::from_opt_data(row_range_end),
        Int32Type::from_opt_data(index_field_id),
        StringType::from_opt_data(index_field_name),
    ]))
}

fn decode_partition(
    bytes: &[u8],
    fields: &[(String, paimon::spec::DataType)],
) -> Result<Option<String>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    match BinaryRow::from_serialized_bytes(bytes) {
        Ok(row) => format_partition(&row, fields),
        Err(_) => Ok(None),
    }
}

fn field_name_by_id(table: &paimon::Table, field_id: i32) -> Option<String> {
    table
        .schema()
        .fields()
        .iter()
        .find(|field| field.id() == field_id)
        .map(|field| field.name().to_string())
}
