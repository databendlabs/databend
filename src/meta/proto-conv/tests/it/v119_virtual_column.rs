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

use chrono::TimeZone;
use chrono::Utc;
use databend_common_expression::TableDataType;
use databend_common_meta_app::schema::VirtualColumnMeta;
use databend_common_meta_app::schema::VirtualField;
use fastrace::func_name;

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
fn test_decode_v119_virtual_column() -> anyhow::Result<()> {
    let schema_v119 = vec![
        8, 7, 18, 7, 118, 58, 107, 49, 58, 107, 50, 18, 7, 118, 91, 49, 93, 91, 50, 93, 18, 7, 118,
        58, 107, 51, 58, 107, 52, 26, 23, 50, 48, 50, 51, 45, 48, 51, 45, 48, 57, 32, 49, 48, 58,
        48, 48, 58, 48, 48, 32, 85, 84, 67, 34, 23, 50, 48, 50, 51, 45, 48, 53, 45, 50, 57, 32, 49,
        48, 58, 48, 48, 58, 48, 48, 32, 85, 84, 67, 42, 18, 178, 2, 9, 210, 2, 0, 160, 6, 119, 168,
        6, 24, 160, 6, 119, 168, 6, 24, 42, 18, 178, 2, 9, 210, 2, 0, 160, 6, 119, 168, 6, 24, 160,
        6, 119, 168, 6, 24, 42, 18, 178, 2, 9, 146, 2, 0, 160, 6, 119, 168, 6, 24, 160, 6, 119,
        168, 6, 24, 50, 10, 8, 1, 18, 6, 118, 97, 108, 117, 101, 49, 50, 10, 8, 2, 18, 6, 118, 97,
        108, 117, 101, 50, 56, 1, 160, 6, 119, 168, 6, 24,
    ];

    let want = || {
        let table_id = 7;
        let virtual_columns = vec![
            VirtualField {
                expr: "v:k1:k2".to_string(),
                data_type: TableDataType::Nullable(Box::new(TableDataType::Variant)),
                alias_name: None,
            },
            VirtualField {
                expr: "v[1][2]".to_string(),
                data_type: TableDataType::Nullable(Box::new(TableDataType::Variant)),
                alias_name: Some("value1".to_string()),
            },
            VirtualField {
                expr: "v:k3:k4".to_string(),
                data_type: TableDataType::Nullable(Box::new(TableDataType::String)),
                alias_name: Some("value2".to_string()),
            },
        ];
        let created_on = Utc.with_ymd_and_hms(2023, 3, 9, 10, 0, 0).unwrap();
        let updated_on = Some(Utc.with_ymd_and_hms(2023, 5, 29, 10, 0, 0).unwrap());
        let auto_generated = true;

        VirtualColumnMeta {
            table_id,
            virtual_columns,
            created_on,
            updated_on,
            auto_generated,
        }
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), schema_v119.as_slice(), 119, want())?;

    Ok(())
}
