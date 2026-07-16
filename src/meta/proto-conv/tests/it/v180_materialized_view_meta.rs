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

use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::schema::MVDefinition;
use databend_common_meta_app::schema::SourceTableMVIds;
use databend_meta_client::types::anyerror::func_name;

use crate::common;

#[test]
fn test_decode_v180_materialized_view_definition() -> anyhow::Result<()> {
    let mv_definition_v180 = vec![
        10, 16, 83, 69, 76, 69, 67, 84, 32, 105, 100, 32, 70, 82, 79, 77, 32, 116, 18, 32, 83, 69,
        76, 69, 67, 84, 32, 105, 100, 32, 70, 82, 79, 77, 32, 100, 101, 102, 97, 117, 108, 116, 46,
        100, 101, 102, 97, 117, 108, 116, 46, 116, 26, 91, 10, 37, 10, 7, 115, 117, 109, 40, 105,
        100, 41, 26, 19, 154, 2, 9, 34, 0, 160, 6, 180, 1, 168, 6, 24, 160, 6, 180, 1, 168, 6, 24,
        160, 6, 180, 1, 168, 6, 24, 10, 41, 10, 9, 99, 111, 117, 110, 116, 40, 105, 100, 41, 26,
        19, 154, 2, 9, 34, 0, 160, 6, 180, 1, 168, 6, 24, 160, 6, 180, 1, 168, 6, 24, 32, 1, 160,
        6, 180, 1, 168, 6, 24, 24, 2, 160, 6, 180, 1, 168, 6, 24, 160, 6, 180, 1, 168, 6, 24,
    ];

    let want = || MVDefinition {
        original_query: "SELECT id FROM t".to_string(),
        query: "SELECT id FROM default.default.t".to_string(),
        schema: TableSchema::new(vec![
            TableField::new("sum(id)", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("count(id)", TableDataType::Number(NumberDataType::UInt64)),
        ]),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), mv_definition_v180.as_slice(), 180, want())?;

    Ok(())
}

#[test]
fn test_decode_v180_materialized_view_source_index() -> anyhow::Result<()> {
    let source_index_v180 = vec![10, 2, 42, 43, 160, 6, 180, 1, 168, 6, 24];

    let want = || SourceTableMVIds::from_ids(vec![42, 43]);

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), source_index_v180.as_slice(), 180, want())?;

    Ok(())
}
