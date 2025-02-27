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

use databend_common_expression::types::NumberDataType;
use databend_common_expression::ColumnId;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_storages_common_table_meta::meta::supported_stat_type;

pub const ROW_COUNT: &str = "row_count";
pub const BLOCK_SIZE: &str = "block_size";
pub const FILE_SIZE: &str = "file_size";
pub const CLUSTER_STATS: &str = "cluster_stats";
pub const LOCATION: &str = "location";
pub const BLOOM_FILTER_INDEX_LOCATION: &str = "bloom_filter_index_location";
pub const BLOOM_FILTER_INDEX_SIZE: &str = "bloom_filter_index_size";
pub const INVERTED_INDEX_SIZE: &str = "inverted_index_size";
pub const COMPRESSION: &str = "compression";
pub const CREATE_ON: &str = "create_on";
pub const LOCATION_PATH: &str = "path";
pub const LOCATION_FORMAT_VERSION: &str = "format_version";

fn location_parts() -> (Vec<String>, Vec<TableDataType>) {
    (
        vec![
            LOCATION_PATH.to_string(),
            LOCATION_FORMAT_VERSION.to_string(),
        ],
        vec![
            TableDataType::String,
            TableDataType::Number(NumberDataType::UInt64),
        ],
    )
}

fn location_type() -> TableDataType {
    let (fields_name, fields_type) = location_parts();
    TableDataType::Tuple {
        fields_name,
        fields_type,
    }
}

fn nullable_location_type() -> TableDataType {
    let (fields_name, fields_type) = location_parts();
    TableDataType::Nullable(Box::new(TableDataType::Tuple {
        fields_name,
        fields_type,
    }))
}

fn col_stats_type(col_type: &TableDataType) -> TableDataType {
    TableDataType::Tuple {
        fields_name: vec![
            "min".to_string(),
            "max".to_string(),
            "null_count".to_string(),
            "in_memory_size".to_string(),
            "distinct_of_values".to_string(),
        ],

        fields_type: vec![
            col_type.wrap_nullable(),
            col_type.wrap_nullable(),
            TableDataType::Number(NumberDataType::UInt64),
            TableDataType::Number(NumberDataType::UInt64),
            TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
        ],
    }
}

fn col_meta_type() -> TableDataType {
    TableDataType::Tuple {
        fields_name: vec![
            "offset".to_string(),
            "len".to_string(),
            "num_values".to_string(),
        ],
        fields_type: vec![
            TableDataType::Number(NumberDataType::UInt64),
            TableDataType::Number(NumberDataType::UInt64),
            TableDataType::Number(NumberDataType::UInt64),
        ],
    }
}

pub fn segment_schema(table_schema: &TableSchema) -> TableSchema {
    let u64_t = TableDataType::Number(NumberDataType::UInt64);
    let nullable_u64_t = TableDataType::Nullable(Box::new(u64_t.clone()));
    let u8_t = TableDataType::Number(NumberDataType::UInt8);
    let i64_t = TableDataType::Number(NumberDataType::Int64);
    let nullable_binary_t = TableDataType::Nullable(Box::new(TableDataType::Binary));
    let mut fields = vec![
        TableField::new(ROW_COUNT, u64_t.clone()),
        TableField::new(BLOCK_SIZE, u64_t.clone()),
        TableField::new(FILE_SIZE, u64_t.clone()),
        // In order to simplify the implementation, cluster stats is serialized as binary instead of nested struct, TODO(Sky): find out if nested struct is better
        TableField::new(CLUSTER_STATS, nullable_binary_t.clone()),
        TableField::new(LOCATION, location_type()),
        TableField::new(BLOOM_FILTER_INDEX_LOCATION, nullable_location_type()),
        TableField::new(BLOOM_FILTER_INDEX_SIZE, u64_t.clone()),
        TableField::new(INVERTED_INDEX_SIZE, nullable_u64_t.clone()),
        TableField::new(COMPRESSION, u8_t.clone()),
        TableField::new(CREATE_ON, i64_t.clone()),
    ];

    for field in table_schema.leaf_fields() {
        if supported_stat_type(&field.data_type().into()) {
            fields.push(TableField::new(
                &stat_name(field.column_id()),
                col_stats_type(field.data_type()),
            ));
        }
        fields.push(TableField::new(
            &meta_name(field.column_id()),
            col_meta_type(),
        ));
    }
    TableSchema::new(fields)
}

pub fn stat_name(col_id: ColumnId) -> String {
    format!("stat_{}", col_id)
}

pub fn meta_name(col_id: ColumnId) -> String {
    format!("meta_{}", col_id)
}
