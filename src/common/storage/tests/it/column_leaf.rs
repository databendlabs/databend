// Copyright 2022 Datafuse Labs.
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

use common_exception::Result;
use common_expression::types::NumberDataType;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_storage::ColumnLeaves;

// a complex schema to cover all data types.
fn create_a_complex_schema() -> TableSchema {
    let child_field11 = TableDataType::Number(NumberDataType::UInt64);
    let child_field12 = TableDataType::Number(NumberDataType::UInt64);
    let child_field22 = TableDataType::Number(NumberDataType::UInt64);

    let s = TableDataType::Tuple {
        fields_name: vec!["0".to_string(), "1".to_string()],
        fields_type: vec![child_field11, child_field12],
    };

    let tuple = TableDataType::Tuple {
        fields_name: vec!["0".to_string(), "1".to_string()],
        fields_type: vec![s.clone(), TableDataType::Array(Box::new(child_field22))],
    };

    let array = TableDataType::Array(Box::new(s));
    let nullarray = TableDataType::Nullable(Box::new(TableDataType::Array(Box::new(
        TableDataType::Number(NumberDataType::UInt64),
    ))));
    let maparray = TableDataType::Map(Box::new(TableDataType::Array(Box::new(
        TableDataType::Number(NumberDataType::UInt64),
    ))));

    let field1 = TableField::new("a", TableDataType::Number(NumberDataType::UInt64));
    let field2 = TableField::new("b", tuple);
    let field3 = TableField::new("c", array);
    let field4 = TableField::new("d", nullarray);
    let field5 = TableField::new("e", maparray);
    let field6 = TableField::new(
        "f",
        TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
    );

    TableSchema::new(vec![field1, field2, field3, field4, field5, field6])
}

#[test]
fn test_column_leaf_schema_from_struct() -> Result<()> {
    let schema = create_a_complex_schema();

    let column_leaves =
        ColumnLeaves::new_from_schema(&schema.to_arrow(), Some(schema.column_id_map()));
    let column_1_ids = vec![0];
    let column_2_ids = vec![1, 2, 3];
    let column_3_ids = vec![4, 5];
    let column_4_ids = vec![6];
    let column_5_ids = vec![7];
    let column_6_ids = vec![8];
    let expeted_column_ids = vec![
        ("a", &column_1_ids),
        ("b", &column_2_ids),
        ("c", &column_3_ids),
        ("d", &column_4_ids),
        ("e", &column_5_ids),
        ("f", &column_6_ids),
    ];

    for (i, column_leaf) in column_leaves.column_leaves.iter().enumerate() {
        let expeted_column_id = expeted_column_ids[i];
        assert_eq!(expeted_column_id.0.to_string(), column_leaf.field.name);
        assert_eq!(*expeted_column_id.1, column_leaf.leaf_column_ids);
    }

    Ok(())
}

#[test]
fn test_column_leaf_schema_from_struct_of_old_version() -> Result<()> {
    let old_schema = create_a_complex_schema();
    let old_column_leaves = ColumnLeaves::new_from_schema(&old_schema.to_arrow(), None);

    let new_schema = TableSchema::init_if_need(old_schema);
    let new_column_leaves =
        ColumnLeaves::new_from_schema(&new_schema.to_arrow(), Some(new_schema.column_id_map()));

    // make sure old and new schema build the same column id map
    assert_eq!(
        old_column_leaves.build_column_id_map(),
        new_column_leaves.build_column_id_map()
    );
    for (old_leaf, new_leaf) in old_column_leaves
        .column_leaves
        .iter()
        .zip(new_column_leaves.column_leaves.iter())
    {
        assert_eq!(old_leaf.field.name, new_leaf.field.name);
        assert_eq!(old_leaf.leaf_ids, new_leaf.leaf_ids);
        for (leaf_id, column_id) in old_leaf
            .leaf_ids
            .iter()
            .zip(new_leaf.leaf_column_ids.iter())
        {
            assert_eq!(*leaf_id as u32, *column_id);
        }
    }

    Ok(())
}
