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

use crate::meta::supported_stat_type;

fn location_parts() -> (Vec<String>, Vec<TableDataType>) {
    (
        vec!["path".to_string(), "format_version".to_string()],
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
            col_type.clone(),
            col_type.clone(),
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
        TableField::new("row_count", u64_t.clone()),
        TableField::new("block_size", u64_t.clone()),
        TableField::new("file_size", u64_t.clone()),
        // In order to simplify the implementation, cluster stats is serialized as binary instead of nested struct, TODO(Sky): find out if nested struct is better
        TableField::new("cluster_stats", nullable_binary_t.clone()),
        TableField::new("location", location_type()),
        TableField::new("bloom_filter_index_location", nullable_location_type()),
        TableField::new("bloom_filter_index_size", u64_t.clone()),
        TableField::new("inverted_index_size", nullable_u64_t.clone()),
        TableField::new("compression", u8_t.clone()),
        TableField::new("create_on", i64_t.clone()),
    ];

    for field in table_schema.leaf_fields() {
        if supported_stat_type(&field.data_type().into()) {
            fields.push(TableField::new(
                &stat_name(field.column_id()),
                col_stats_type(&field.data_type()),
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
