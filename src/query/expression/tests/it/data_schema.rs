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

use common_exception::Result;
use common_expression::types::NumberDataType;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use pretty_assertions::assert_eq;

#[test]
fn test_project_schema_from_tuple() -> Result<()> {
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
    ];
    let schema = TableSchema::new(fields);

    let expect_fields = vec![
        TableField::new("a", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new("b:b1:b11", TableDataType::Boolean),
        TableField::new("b:b1:b12", TableDataType::String),
        TableField::new("b:b2", TableDataType::Number(NumberDataType::Int64)),
    ];
    let expect_schema = TableSchema::new(expect_fields);

    let mut path_indices = BTreeMap::new();
    path_indices.insert(0, vec![0]);
    path_indices.insert(1, vec![1, 0, 0]);
    path_indices.insert(2, vec![1, 0, 1]);
    path_indices.insert(3, vec![1, 1]);
    let project_shcema = schema.inner_project(&path_indices);
    assert_eq!(project_shcema, expect_schema);

    Ok(())
}
