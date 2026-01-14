// Copyright 2023 Datafuse Labs.
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
use std::sync::Arc;

use chrono::TimeZone;
use chrono::Utc;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::VIRTUAL_COLUMN_ID_START;
use databend_common_expression::VariantDataType;
use databend_common_expression::VirtualDataField;
use databend_common_expression::VirtualDataSchema;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::VectorDataType;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_meta_app::schema as mt;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use fastrace::func_name;
use maplit::btreemap;
use maplit::btreeset;

use crate::common;

// These bytes are built when a new version in introduced,
// and are kept for backward compatibility test.
//
// *************************************************************
// * These messages should never be updated,                   *
// * only be added when a new version is added,                *
// * or be removed when an old version is no longer supported. *
// *************************************************************
//
// The message bytes are built from the output of `test_pb_from_to()`
#[test]
fn test_decode_v129_schema() -> anyhow::Result<()> {
    let table_schema_v129 = vec![
        10, 31, 10, 1, 97, 26, 19, 154, 2, 9, 34, 0, 160, 6, 129, 1, 168, 6, 24, 160, 6, 129, 1,
        168, 6, 24, 160, 6, 129, 1, 168, 6, 24, 10, 113, 10, 1, 98, 26, 99, 202, 2, 89, 10, 2, 98,
        49, 10, 2, 98, 50, 18, 51, 202, 2, 41, 10, 3, 98, 49, 49, 10, 3, 98, 49, 50, 18, 10, 138,
        2, 0, 160, 6, 129, 1, 168, 6, 24, 18, 10, 146, 2, 0, 160, 6, 129, 1, 168, 6, 24, 160, 6,
        129, 1, 168, 6, 24, 160, 6, 129, 1, 168, 6, 24, 18, 19, 154, 2, 9, 66, 0, 160, 6, 129, 1,
        168, 6, 24, 160, 6, 129, 1, 168, 6, 24, 160, 6, 129, 1, 168, 6, 24, 160, 6, 129, 1, 168, 6,
        24, 32, 1, 160, 6, 129, 1, 168, 6, 24, 10, 33, 10, 1, 99, 26, 19, 154, 2, 9, 34, 0, 160, 6,
        129, 1, 168, 6, 24, 160, 6, 129, 1, 168, 6, 24, 32, 4, 160, 6, 129, 1, 168, 6, 24, 10, 53,
        10, 10, 100, 101, 99, 105, 109, 97, 108, 49, 50, 56, 26, 30, 218, 2, 20, 10, 11, 8, 18, 16,
        3, 160, 6, 129, 1, 168, 6, 24, 160, 6, 129, 1, 168, 6, 24, 160, 6, 129, 1, 168, 6, 24, 32,
        5, 160, 6, 129, 1, 168, 6, 24, 10, 53, 10, 10, 100, 101, 99, 105, 109, 97, 108, 50, 53, 54,
        26, 30, 218, 2, 20, 18, 11, 8, 46, 16, 6, 160, 6, 129, 1, 168, 6, 24, 160, 6, 129, 1, 168,
        6, 24, 160, 6, 129, 1, 168, 6, 24, 32, 6, 160, 6, 129, 1, 168, 6, 24, 10, 32, 10, 9, 101,
        109, 112, 116, 121, 95, 109, 97, 112, 26, 10, 226, 2, 0, 160, 6, 129, 1, 168, 6, 24, 32, 7,
        160, 6, 129, 1, 168, 6, 24, 10, 29, 10, 6, 98, 105, 116, 109, 97, 112, 26, 10, 234, 2, 0,
        160, 6, 129, 1, 168, 6, 24, 32, 8, 160, 6, 129, 1, 168, 6, 24, 10, 27, 10, 4, 103, 101,
        111, 109, 26, 10, 250, 2, 0, 160, 6, 129, 1, 168, 6, 24, 32, 9, 160, 6, 129, 1, 168, 6, 24,
        10, 31, 10, 8, 105, 110, 116, 101, 114, 118, 97, 108, 26, 10, 138, 3, 0, 160, 6, 129, 1,
        168, 6, 24, 32, 10, 160, 6, 129, 1, 168, 6, 24, 10, 50, 10, 6, 118, 101, 99, 116, 111, 114,
        26, 31, 146, 3, 21, 10, 9, 74, 0, 160, 6, 129, 1, 168, 6, 24, 16, 128, 2, 160, 6, 129, 1,
        168, 6, 24, 160, 6, 129, 1, 168, 6, 24, 32, 11, 160, 6, 129, 1, 168, 6, 24, 24, 12, 160, 6,
        129, 1, 168, 6, 24,
    ];

    let b1 = TableDataType::Tuple {
        fields_name: vec!["b11".to_string(), "b12".to_string()],
        fields_type: vec![TableDataType::Boolean, TableDataType::String],
    };
    let b = TableDataType::Tuple {
        fields_name: vec!["b1".to_string(), "b2".to_string()],
        fields_type: vec![b1, TableDataType::Number(NumberDataType::Int64)],
    };
    let fields = vec![
        TableField::new("a", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new("b", b),
        TableField::new("c", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new(
            "decimal128",
            TableDataType::Decimal(DecimalDataType::Decimal128(DecimalSize::new_unchecked(
                18, 3,
            ))),
        ),
        TableField::new(
            "decimal256",
            TableDataType::Decimal(DecimalDataType::Decimal256(DecimalSize::new_unchecked(
                46, 6,
            ))),
        ),
        TableField::new("empty_map", TableDataType::EmptyMap),
        TableField::new("bitmap", TableDataType::Bitmap),
        TableField::new("geom", TableDataType::Geometry),
        TableField::new("interval", TableDataType::Interval),
        TableField::new(
            "vector",
            TableDataType::Vector(VectorDataType::Float32(256)),
        ),
    ];
    let want = || TableSchema::new(fields.clone());
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), table_schema_v129.as_slice(), 129, want())?;
    Ok(())
}

#[test]
fn test_decode_v129_table_meta() -> anyhow::Result<()> {
    let table_meta_v129 = vec![
        10, 224, 7, 10, 55, 10, 8, 110, 117, 108, 108, 97, 98, 108, 101, 18, 5, 97, 32, 43, 32, 51,
        26, 29, 178, 2, 19, 154, 2, 9, 42, 0, 160, 6, 129, 1, 168, 6, 24, 160, 6, 129, 1, 168, 6,
        24, 160, 6, 129, 1, 168, 6, 24, 160, 6, 129, 1, 168, 6, 24, 10, 27, 10, 4, 98, 111, 111,
        108, 26, 10, 138, 2, 0, 160, 6, 129, 1, 168, 6, 24, 32, 1, 160, 6, 129, 1, 168, 6, 24, 10,
        36, 10, 4, 105, 110, 116, 56, 26, 19, 154, 2, 9, 42, 0, 160, 6, 129, 1, 168, 6, 24, 160, 6,
        129, 1, 168, 6, 24, 32, 2, 160, 6, 129, 1, 168, 6, 24, 10, 37, 10, 5, 105, 110, 116, 49,
        54, 26, 19, 154, 2, 9, 50, 0, 160, 6, 129, 1, 168, 6, 24, 160, 6, 129, 1, 168, 6, 24, 32,
        3, 160, 6, 129, 1, 168, 6, 24, 10, 37, 10, 5, 105, 110, 116, 51, 50, 26, 19, 154, 2, 9, 58,
        0, 160, 6, 129, 1, 168, 6, 24, 160, 6, 129, 1, 168, 6, 24, 32, 4, 160, 6, 129, 1, 168, 6,
        24, 10, 37, 10, 5, 105, 110, 116, 54, 52, 26, 19, 154, 2, 9, 66, 0, 160, 6, 129, 1, 168, 6,
        24, 160, 6, 129, 1, 168, 6, 24, 32, 5, 160, 6, 129, 1, 168, 6, 24, 10, 37, 10, 5, 117, 105,
        110, 116, 56, 26, 19, 154, 2, 9, 10, 0, 160, 6, 129, 1, 168, 6, 24, 160, 6, 129, 1, 168, 6,
        24, 32, 6, 160, 6, 129, 1, 168, 6, 24, 10, 38, 10, 6, 117, 105, 110, 116, 49, 54, 26, 19,
        154, 2, 9, 18, 0, 160, 6, 129, 1, 168, 6, 24, 160, 6, 129, 1, 168, 6, 24, 32, 7, 160, 6,
        129, 1, 168, 6, 24, 10, 38, 10, 6, 117, 105, 110, 116, 51, 50, 26, 19, 154, 2, 9, 26, 0,
        160, 6, 129, 1, 168, 6, 24, 160, 6, 129, 1, 168, 6, 24, 32, 8, 160, 6, 129, 1, 168, 6, 24,
        10, 38, 10, 6, 117, 105, 110, 116, 54, 52, 26, 19, 154, 2, 9, 34, 0, 160, 6, 129, 1, 168,
        6, 24, 160, 6, 129, 1, 168, 6, 24, 32, 9, 160, 6, 129, 1, 168, 6, 24, 10, 39, 10, 7, 102,
        108, 111, 97, 116, 51, 50, 26, 19, 154, 2, 9, 74, 0, 160, 6, 129, 1, 168, 6, 24, 160, 6,
        129, 1, 168, 6, 24, 32, 10, 160, 6, 129, 1, 168, 6, 24, 10, 39, 10, 7, 102, 108, 111, 97,
        116, 54, 52, 26, 19, 154, 2, 9, 82, 0, 160, 6, 129, 1, 168, 6, 24, 160, 6, 129, 1, 168, 6,
        24, 32, 11, 160, 6, 129, 1, 168, 6, 24, 10, 27, 10, 4, 100, 97, 116, 101, 26, 10, 170, 2,
        0, 160, 6, 129, 1, 168, 6, 24, 32, 12, 160, 6, 129, 1, 168, 6, 24, 10, 32, 10, 9, 116, 105,
        109, 101, 115, 116, 97, 109, 112, 26, 10, 162, 2, 0, 160, 6, 129, 1, 168, 6, 24, 32, 13,
        160, 6, 129, 1, 168, 6, 24, 10, 29, 10, 6, 115, 116, 114, 105, 110, 103, 26, 10, 146, 2, 0,
        160, 6, 129, 1, 168, 6, 24, 32, 14, 160, 6, 129, 1, 168, 6, 24, 10, 70, 10, 6, 115, 116,
        114, 117, 99, 116, 26, 51, 202, 2, 41, 10, 3, 102, 111, 111, 10, 3, 98, 97, 114, 18, 10,
        138, 2, 0, 160, 6, 129, 1, 168, 6, 24, 18, 10, 146, 2, 0, 160, 6, 129, 1, 168, 6, 24, 160,
        6, 129, 1, 168, 6, 24, 160, 6, 129, 1, 168, 6, 24, 32, 15, 160, 6, 129, 1, 168, 6, 24, 10,
        38, 10, 5, 97, 114, 114, 97, 121, 26, 20, 186, 2, 10, 138, 2, 0, 160, 6, 129, 1, 168, 6,
        24, 160, 6, 129, 1, 168, 6, 24, 32, 17, 160, 6, 129, 1, 168, 6, 24, 10, 30, 10, 7, 118, 97,
        114, 105, 97, 110, 116, 26, 10, 210, 2, 0, 160, 6, 129, 1, 168, 6, 24, 32, 18, 160, 6, 129,
        1, 168, 6, 24, 10, 36, 10, 13, 118, 97, 114, 105, 97, 110, 116, 95, 97, 114, 114, 97, 121,
        26, 10, 210, 2, 0, 160, 6, 129, 1, 168, 6, 24, 32, 19, 160, 6, 129, 1, 168, 6, 24, 10, 37,
        10, 14, 118, 97, 114, 105, 97, 110, 116, 95, 111, 98, 106, 101, 99, 116, 26, 10, 210, 2, 0,
        160, 6, 129, 1, 168, 6, 24, 32, 20, 160, 6, 129, 1, 168, 6, 24, 10, 31, 10, 8, 105, 110,
        116, 101, 114, 118, 97, 108, 26, 10, 250, 1, 0, 160, 6, 129, 1, 168, 6, 24, 32, 21, 160, 6,
        129, 1, 168, 6, 24, 10, 29, 10, 6, 98, 105, 116, 109, 97, 112, 26, 10, 234, 2, 0, 160, 6,
        129, 1, 168, 6, 24, 32, 22, 160, 6, 129, 1, 168, 6, 24, 10, 27, 10, 4, 103, 101, 111, 109,
        26, 10, 250, 2, 0, 160, 6, 129, 1, 168, 6, 24, 32, 23, 160, 6, 129, 1, 168, 6, 24, 10, 31,
        10, 8, 105, 110, 116, 101, 114, 118, 97, 108, 26, 10, 138, 3, 0, 160, 6, 129, 1, 168, 6,
        24, 32, 24, 160, 6, 129, 1, 168, 6, 24, 10, 50, 10, 6, 118, 101, 99, 116, 111, 114, 26, 31,
        146, 3, 21, 10, 9, 74, 0, 160, 6, 129, 1, 168, 6, 24, 16, 128, 2, 160, 6, 129, 1, 168, 6,
        24, 160, 6, 129, 1, 168, 6, 24, 32, 25, 160, 6, 129, 1, 168, 6, 24, 18, 6, 10, 1, 97, 18,
        1, 98, 24, 26, 160, 6, 129, 1, 168, 6, 24, 42, 10, 10, 3, 120, 121, 122, 18, 3, 102, 111,
        111, 50, 2, 52, 52, 58, 10, 10, 3, 97, 98, 99, 18, 3, 100, 101, 102, 64, 0, 74, 10, 40, 97,
        32, 43, 32, 50, 44, 32, 98, 41, 162, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49,
        50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 170, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50,
        57, 32, 49, 50, 58, 48, 48, 58, 49, 48, 32, 85, 84, 67, 178, 1, 13, 116, 97, 98, 108, 101,
        95, 99, 111, 109, 109, 101, 110, 116, 186, 1, 7, 160, 6, 129, 1, 168, 6, 24, 202, 1, 1, 99,
        202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99,
        202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99,
        202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99,
        202, 1, 1, 99, 202, 1, 1, 99, 226, 1, 1, 1, 234, 1, 6, 10, 1, 97, 18, 1, 98, 250, 1, 78,
        10, 1, 118, 18, 73, 10, 1, 118, 18, 1, 1, 24, 1, 34, 40, 55, 52, 101, 99, 55, 100, 51, 51,
        50, 54, 56, 48, 102, 57, 101, 54, 48, 50, 51, 52, 99, 48, 55, 102, 53, 100, 101, 102, 56,
        100, 48, 97, 53, 50, 98, 48, 102, 98, 53, 53, 42, 12, 10, 4, 116, 121, 112, 101, 18, 4,
        104, 110, 115, 119, 48, 2, 160, 6, 129, 1, 168, 6, 24, 130, 2, 56, 10, 31, 10, 7, 102, 105,
        101, 108, 100, 95, 48, 18, 2, 10, 0, 18, 2, 50, 0, 18, 4, 58, 2, 10, 0, 24, 19, 32, 128,
        188, 193, 150, 11, 18, 6, 10, 1, 97, 18, 1, 98, 24, 129, 188, 193, 150, 11, 32, 10, 160, 6,
        129, 1, 168, 6, 24, 160, 6, 129, 1, 168, 6, 24,
    ];

    let want = || mt::TableMeta {
        schema: Arc::new(TableSchema::new_from(
            vec![
                TableField::new(
                    "nullable",
                    TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int8))),
                )
                .with_default_expr(Some("a + 3".to_string())),
                TableField::new("bool", TableDataType::Boolean),
                TableField::new("int8", TableDataType::Number(NumberDataType::Int8)),
                TableField::new("int16", TableDataType::Number(NumberDataType::Int16)),
                TableField::new("int32", TableDataType::Number(NumberDataType::Int32)),
                TableField::new("int64", TableDataType::Number(NumberDataType::Int64)),
                TableField::new("uint8", TableDataType::Number(NumberDataType::UInt8)),
                TableField::new("uint16", TableDataType::Number(NumberDataType::UInt16)),
                TableField::new("uint32", TableDataType::Number(NumberDataType::UInt32)),
                TableField::new("uint64", TableDataType::Number(NumberDataType::UInt64)),
                TableField::new("float32", TableDataType::Number(NumberDataType::Float32)),
                TableField::new("float64", TableDataType::Number(NumberDataType::Float64)),
                TableField::new("date", TableDataType::Date),
                TableField::new("timestamp", TableDataType::Timestamp),
                TableField::new("string", TableDataType::String),
                TableField::new("struct", TableDataType::Tuple {
                    fields_name: vec![s("foo"), s("bar")],
                    fields_type: vec![TableDataType::Boolean, TableDataType::String],
                }),
                TableField::new(
                    "array",
                    TableDataType::Array(Box::new(TableDataType::Boolean)),
                ),
                TableField::new("variant", TableDataType::Variant),
                TableField::new("variant_array", TableDataType::Variant),
                TableField::new("variant_object", TableDataType::Variant),
                // NOTE: It is safe to convert Interval to NULL, because `Interval` is never really used.
                TableField::new("interval", TableDataType::Null),
                TableField::new("bitmap", TableDataType::Bitmap),
                TableField::new("geom", TableDataType::Geometry),
                TableField::new("interval", TableDataType::Interval),
                TableField::new(
                    "vector",
                    TableDataType::Vector(VectorDataType::Float32(256)),
                ),
            ],
            btreemap! {s("a") => s("b")},
        )),
        engine: "44".to_string(),
        storage_params: None,
        part_prefix: "".to_string(),
        engine_options: btreemap! {s("abc") => s("def")},
        options: btreemap! {s("xyz") => s("foo")},
        cluster_key: Some("(a + 2, b)".to_string()),
        cluster_key_id: Some(0),
        cluster_key_seq: 0,
        created_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
        updated_on: Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 10).unwrap(),
        comment: s("table_comment"),
        field_comments: vec!["c".to_string(); 21],
        virtual_schema: Some(VirtualDataSchema {
            fields: vec![VirtualDataField {
                name: "field_0".to_string(),
                data_types: vec![
                    VariantDataType::Jsonb,
                    VariantDataType::String,
                    VariantDataType::Array(Box::new(VariantDataType::Jsonb)),
                ],
                source_column_id: 19,
                column_id: VIRTUAL_COLUMN_ID_START,
            }],
            metadata: btreemap! {s("a") => s("b")},
            next_column_id: VIRTUAL_COLUMN_ID_START + 1,
            number_of_blocks: 10,
        }),
        drop_on: None,
        statistics: Default::default(),
        shared_by: btreeset! {1},
        column_mask_policy: Some(btreemap! {s("a") => s("b")}),
        column_mask_policy_columns_ids: BTreeMap::new(),
        row_access_policy: None,
        row_access_policy_columns_ids: None,
        indexes: btreemap! {s("v") => TableIndex {
            index_type: TableIndexType::Vector,
            name: "v".to_string(),
            column_ids: vec![1],
            sync_creation: true,
            version: "74ec7d332680f9e60234c07f5def8d0a52b0fb55".to_string(),
            options: btreemap! {s("type") => s("hnsw")},
        }},
        constraints: btreemap! {},
        refs: btreemap! {},
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), table_meta_v129.as_slice(), 129, want())?;

    Ok(())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
