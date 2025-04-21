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

use std::sync::Arc;

use chrono::TimeZone;
use chrono::Utc;
use databend_common_expression as ce;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::ComputedExpr;
use databend_common_meta_app::schema as mt;
use databend_common_meta_app::schema::TableIndexType;
use fastrace::func_name;
use maplit::btreemap;
use maplit::btreeset;

use crate::common;

#[test]
fn test_decode_v82_table_meta() -> anyhow::Result<()> {
    let table_meta_v82 = vec![
        10, 223, 1, 10, 51, 10, 8, 110, 117, 108, 108, 97, 98, 108, 101, 18, 5, 97, 32, 43, 32, 51,
        26, 26, 178, 2, 17, 154, 2, 8, 42, 0, 160, 6, 82, 168, 6, 24, 160, 6, 82, 168, 6, 24, 160,
        6, 82, 168, 6, 24, 160, 6, 82, 168, 6, 24, 10, 27, 10, 6, 115, 116, 114, 105, 110, 103, 26,
        9, 146, 2, 0, 160, 6, 82, 168, 6, 24, 32, 1, 160, 6, 82, 168, 6, 24, 10, 62, 10, 14, 118,
        105, 114, 116, 117, 97, 108, 95, 115, 116, 114, 105, 110, 103, 26, 9, 146, 2, 0, 160, 6,
        82, 168, 6, 24, 32, 2, 42, 25, 10, 17, 116, 111, 95, 98, 97, 115, 101, 54, 52, 40, 115,
        116, 114, 105, 110, 103, 41, 160, 6, 82, 168, 6, 24, 160, 6, 82, 168, 6, 24, 10, 59, 10,
        13, 115, 116, 111, 114, 101, 100, 95, 115, 116, 114, 105, 110, 103, 26, 9, 146, 2, 0, 160,
        6, 82, 168, 6, 24, 32, 3, 42, 23, 18, 15, 114, 101, 118, 101, 114, 115, 101, 40, 115, 116,
        114, 105, 110, 103, 41, 160, 6, 82, 168, 6, 24, 160, 6, 82, 168, 6, 24, 18, 6, 10, 1, 97,
        18, 1, 98, 24, 4, 160, 6, 82, 168, 6, 24, 34, 10, 40, 97, 32, 43, 32, 50, 44, 32, 98, 41,
        42, 10, 10, 3, 120, 121, 122, 18, 3, 102, 111, 111, 50, 2, 52, 52, 58, 10, 10, 3, 97, 98,
        99, 18, 3, 100, 101, 102, 64, 0, 74, 10, 40, 97, 32, 43, 32, 50, 44, 32, 98, 41, 82, 7,
        100, 101, 102, 97, 117, 108, 116, 162, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32,
        49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 170, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45,
        50, 57, 32, 49, 50, 58, 48, 48, 58, 49, 48, 32, 85, 84, 67, 178, 1, 13, 116, 97, 98, 108,
        101, 95, 99, 111, 109, 109, 101, 110, 116, 186, 1, 6, 160, 6, 82, 168, 6, 24, 202, 1, 1,
        99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1,
        99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1,
        99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1,
        99, 202, 1, 1, 99, 202, 1, 1, 99, 226, 1, 1, 1, 234, 1, 6, 10, 1, 97, 18, 1, 98, 250, 1,
        24, 10, 4, 105, 100, 120, 49, 18, 16, 10, 4, 105, 100, 120, 49, 18, 2, 1, 2, 160, 6, 82,
        168, 6, 24, 160, 6, 82, 168, 6, 24,
    ];

    let want = || mt::TableMeta {
        schema: Arc::new(ce::TableSchema::new_from(
            vec![
                ce::TableField::new(
                    "nullable",
                    ce::TableDataType::Nullable(Box::new(ce::TableDataType::Number(
                        NumberDataType::Int8,
                    ))),
                )
                .with_default_expr(Some("a + 3".to_string())),
                ce::TableField::new("string", ce::TableDataType::String),
                ce::TableField::new("virtual_string", ce::TableDataType::String)
                    .with_computed_expr(Some(ComputedExpr::Virtual(
                        "to_base64(string)".to_string(),
                    ))),
                ce::TableField::new("stored_string", ce::TableDataType::String)
                    .with_computed_expr(Some(ComputedExpr::Stored("reverse(string)".to_string()))),
            ],
            btreemap! {s("a") => s("b")},
        )),
        engine: "44".to_string(),
        storage_params: None,
        part_prefix: "".to_string(),
        engine_options: btreemap! {s("abc") => s("def")},
        options: btreemap! {s("xyz") => s("foo")},
        cluster_key: Some("(a + 2, b)".to_string()),
        cluster_key_seq: 0,
        created_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
        updated_on: Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 10).unwrap(),
        comment: s("table_comment"),
        field_comments: vec!["c".to_string(); 21],
        virtual_schema: None,
        drop_on: None,
        statistics: Default::default(),
        shared_by: btreeset! {1},
        column_mask_policy: Some(btreemap! {s("a") => s("b")}),
        indexes: btreemap! {s("idx1") => mt::TableIndex {
            index_type: TableIndexType::Inverted,
            name: "idx1".to_string(),
            column_ids: vec![1, 2],
            sync_creation: false,
            version: "".to_string(),
            options: btreemap! {},
        }},
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), table_meta_v82.as_slice(), 82, want())?;

    Ok(())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
