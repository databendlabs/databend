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

use std::collections::BTreeSet;
use std::sync::Arc;

use chrono::TimeZone;
use chrono::Utc;
use databend_common_expression as ex;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::schema as mt;
use databend_common_meta_app::storage::StorageParams;
use fastrace::func_name;
use maplit::btreemap;

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
fn test_decode_v24_table_meta() -> anyhow::Result<()> {
    let bytes = vec![
        10, 169, 5, 10, 51, 10, 8, 110, 117, 108, 108, 97, 98, 108, 101, 18, 5, 97, 32, 43, 32, 51,
        26, 26, 178, 2, 17, 154, 2, 8, 42, 0, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24, 160,
        6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24, 10, 23, 10, 4, 98, 111, 111, 108, 26, 9, 138, 2,
        0, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24, 10, 31, 10, 4, 105, 110, 116, 56, 26,
        17, 154, 2, 8, 42, 0, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6,
        24, 10, 32, 10, 5, 105, 110, 116, 49, 54, 26, 17, 154, 2, 8, 50, 0, 160, 6, 24, 168, 6, 24,
        160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24, 10, 32, 10, 5, 105, 110, 116, 51, 50, 26,
        17, 154, 2, 8, 58, 0, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6,
        24, 10, 32, 10, 5, 105, 110, 116, 54, 52, 26, 17, 154, 2, 8, 66, 0, 160, 6, 24, 168, 6, 24,
        160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24, 10, 32, 10, 5, 117, 105, 110, 116, 56, 26,
        17, 154, 2, 8, 10, 0, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6,
        24, 10, 33, 10, 6, 117, 105, 110, 116, 49, 54, 26, 17, 154, 2, 8, 18, 0, 160, 6, 24, 168,
        6, 24, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24, 10, 33, 10, 6, 117, 105, 110, 116,
        51, 50, 26, 17, 154, 2, 8, 26, 0, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24, 160, 6,
        24, 168, 6, 24, 10, 33, 10, 6, 117, 105, 110, 116, 54, 52, 26, 17, 154, 2, 8, 34, 0, 160,
        6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24, 10, 34, 10, 7, 102, 108,
        111, 97, 116, 51, 50, 26, 17, 154, 2, 8, 74, 0, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6,
        24, 160, 6, 24, 168, 6, 24, 10, 34, 10, 7, 102, 108, 111, 97, 116, 54, 52, 26, 17, 154, 2,
        8, 82, 0, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24, 10, 23,
        10, 4, 100, 97, 116, 101, 26, 9, 170, 2, 0, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24,
        10, 28, 10, 9, 116, 105, 109, 101, 115, 116, 97, 109, 112, 26, 9, 162, 2, 0, 160, 6, 24,
        168, 6, 24, 160, 6, 24, 168, 6, 24, 10, 25, 10, 6, 115, 116, 114, 105, 110, 103, 26, 9,
        146, 2, 0, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24, 10, 63, 10, 6, 115, 116, 114,
        117, 99, 116, 26, 47, 202, 2, 38, 10, 3, 102, 111, 111, 10, 3, 98, 97, 114, 18, 9, 138, 2,
        0, 160, 6, 24, 168, 6, 24, 18, 9, 146, 2, 0, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6,
        24, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24, 10, 33, 10, 5, 97, 114, 114, 97, 121,
        26, 18, 186, 2, 9, 138, 2, 0, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24, 160, 6, 24,
        168, 6, 24, 10, 31, 10, 3, 109, 97, 112, 26, 18, 194, 2, 9, 138, 2, 0, 160, 6, 24, 168, 6,
        24, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24, 10, 26, 10, 7, 118, 97, 114, 105, 97,
        110, 116, 26, 9, 210, 2, 0, 160, 6, 24, 168, 6, 24, 160, 6, 24, 168, 6, 24, 18, 6, 10, 1,
        97, 18, 1, 98, 160, 6, 24, 168, 6, 24, 34, 10, 40, 97, 32, 43, 32, 50, 44, 32, 98, 41, 42,
        10, 10, 3, 120, 121, 122, 18, 3, 102, 111, 111, 50, 2, 52, 52, 58, 10, 10, 3, 97, 98, 99,
        18, 3, 100, 101, 102, 64, 0, 74, 10, 40, 97, 32, 43, 32, 50, 44, 32, 98, 41, 82, 23, 110,
        101, 118, 101, 114, 45, 103, 111, 110, 110, 97, 45, 103, 105, 118, 101, 45, 121, 111, 117,
        45, 117, 112, 162, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49, 50, 58, 48, 48,
        58, 48, 57, 32, 85, 84, 67, 170, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 57, 32, 49, 50,
        58, 48, 48, 58, 49, 48, 32, 85, 84, 67, 178, 1, 13, 116, 97, 98, 108, 101, 95, 99, 111,
        109, 109, 101, 110, 116, 186, 1, 6, 160, 6, 24, 168, 6, 24, 202, 1, 1, 99, 202, 1, 1, 99,
        202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99,
        202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99,
        202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99,
        202, 1, 1, 99, 210, 1, 15, 18, 13, 10, 5, 95, 100, 97, 116, 97, 160, 6, 24, 168, 6, 24,
        218, 1, 5, 108, 117, 108, 117, 95, 160, 6, 24, 168, 6, 24,
    ];

    let want = || mt::TableMeta {
        schema: Arc::new(ex::TableSchema::new_from(
            vec![
                ex::TableField::new(
                    "nullable",
                    ex::TableDataType::Nullable(Box::new(ex::TableDataType::Number(
                        NumberDataType::Int8,
                    ))),
                )
                .with_default_expr(Some("a + 3".to_string())),
                ex::TableField::new("bool", ex::TableDataType::Boolean),
                ex::TableField::new("int8", ex::TableDataType::Number(NumberDataType::Int8)),
                ex::TableField::new("int16", ex::TableDataType::Number(NumberDataType::Int16)),
                ex::TableField::new("int32", ex::TableDataType::Number(NumberDataType::Int32)),
                ex::TableField::new("int64", ex::TableDataType::Number(NumberDataType::Int64)),
                ex::TableField::new("uint8", ex::TableDataType::Number(NumberDataType::UInt8)),
                ex::TableField::new("uint16", ex::TableDataType::Number(NumberDataType::UInt16)),
                ex::TableField::new("uint32", ex::TableDataType::Number(NumberDataType::UInt32)),
                ex::TableField::new("uint64", ex::TableDataType::Number(NumberDataType::UInt64)),
                ex::TableField::new(
                    "float32",
                    ex::TableDataType::Number(NumberDataType::Float32),
                ),
                ex::TableField::new(
                    "float64",
                    ex::TableDataType::Number(NumberDataType::Float64),
                ),
                ex::TableField::new("date", ex::TableDataType::Date),
                ex::TableField::new("timestamp", ex::TableDataType::Timestamp),
                ex::TableField::new("string", ex::TableDataType::String),
                ex::TableField::new("struct", ex::TableDataType::Tuple {
                    fields_name: vec![s("foo"), s("bar")],
                    fields_type: vec![ex::TableDataType::Boolean, ex::TableDataType::String],
                }),
                ex::TableField::new(
                    "array",
                    ex::TableDataType::Array(Box::new(ex::TableDataType::Boolean)),
                ),
                ex::TableField::new(
                    "map",
                    ex::TableDataType::Map(Box::new(ex::TableDataType::Boolean)),
                ),
                ex::TableField::new("variant", ex::TableDataType::Variant),
            ],
            btreemap! {s("a") => s("b")},
        )),
        engine: "44".to_string(),
        engine_options: btreemap! {s("abc") => s("def")},
        storage_params: Some(StorageParams::default()),
        part_prefix: "lulu_".to_string(),
        options: btreemap! {s("xyz") => s("foo")},
        default_cluster_key: Some("(a + 2, b)".to_string()),
        cluster_keys: vec!["(a + 2, b)".to_string()],
        default_cluster_key_id: Some(0),
        created_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
        updated_on: Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 10).unwrap(),
        comment: s("table_comment"),
        field_comments: vec!["c".to_string(); 21],
        drop_on: None,
        statistics: Default::default(),
        shared_by: BTreeSet::new(),
        column_mask_policy: None,
        indexes: btreemap! {},
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 24, want())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
