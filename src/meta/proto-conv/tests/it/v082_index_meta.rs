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

use chrono::TimeZone;
use chrono::Utc;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::IndexType;
use minitrace::func_name;

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
// The message bytes are built from the output of `proto_conv::test_build_pb_buf()`
#[test]
fn test_decode_v82_index() -> anyhow::Result<()> {
    let index_v082 = vec![
        8, 7, 16, 3, 26, 23, 50, 48, 49, 53, 45, 48, 51, 45, 48, 57, 32, 50, 48, 58, 48, 48, 58,
        48, 57, 32, 85, 84, 67, 74, 54, 10, 20, 10, 1, 97, 26, 9, 146, 2, 0, 160, 6, 82, 168, 6,
        24, 160, 6, 82, 168, 6, 24, 10, 22, 10, 1, 98, 26, 9, 146, 2, 0, 160, 6, 82, 168, 6, 24,
        32, 1, 160, 6, 82, 168, 6, 24, 24, 2, 160, 6, 82, 168, 6, 24, 160, 6, 82, 168, 6, 24,
    ];

    let want = || {
        let table_id = 7;
        let index_type = IndexType::INVERTED;
        let created_on = Utc.with_ymd_and_hms(2015, 3, 9, 20, 0, 9).unwrap();
        let original_query = "".to_string();
        let query = "".to_string();

        let fields = vec![
            TableField::new("a", TableDataType::String),
            TableField::new("b", TableDataType::String),
        ];
        let index_schema = Some(Arc::new(TableSchema::new(fields)));

        IndexMeta {
            table_id,
            index_type,
            created_on,
            dropped_on: None,
            original_query,
            query,
            updated_on: None,
            sync_creation: false,
            index_schema,
        }
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), index_v082.as_slice(), 82, want())?;

    Ok(())
}
