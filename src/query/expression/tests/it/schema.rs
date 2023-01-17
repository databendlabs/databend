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

use common_exception::Result;
use common_expression::types::NumberDataType;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use pretty_assertions::assert_eq;

// #[test]
// fn test_project_schema_from_tuple() -> Result<()> {
// let b1 = TableDataType::Tuple {
// fields_name: vec!["b11".to_string(), "b12".to_string()],
// fields_type: vec![TableDataType::Boolean, TableDataType::String],
// };
// let b = TableDataType::Tuple {
// fields_name: vec!["b1".to_string(), "b2".to_string()],
// fields_type: vec![b1, TableDataType::Number(NumberDataType::Int64)],
// };
// let fields = vec![
// TableField::new("a", TableDataType::Number(NumberDataType::UInt64)),
// TableField::new("b", b),
// TableField::new("c", TableDataType::Number(NumberDataType::UInt64)),
// ];
// let schema = TableSchema::new(fields);
//
// let expect_fields = vec![
// TableField::new("a", TableDataType::Number(NumberDataType::UInt64)),
// TableField::new("b:b1:b11", TableDataType::Boolean),
// TableField::new("b:b1:b12", TableDataType::String),
// TableField::new("b:b2", TableDataType::Number(NumberDataType::Int64)),
// ];
// let expect_schema = TableSchema::new(expect_fields);
//
// let mut path_indices = BTreeMap::new();
// path_indices.insert(0, vec![0]);
// path_indices.insert(1, vec![1, 0, 0]);
// path_indices.insert(2, vec![1, 0, 1]);
// path_indices.insert(3, vec![1, 1]);
// let project_shcema = schema.inner_project(&path_indices);
// assert_eq!(project_shcema, expect_schema);
//
// Ok(())
// }

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

    TableSchema::new(vec![field1, field2, field3, field4, field5])
}

#[test]
fn test_schema_new_from_field() -> Result<()> {
    let field1 = TableField::new("a", TableDataType::Number(NumberDataType::UInt64));
    let field2 = TableField::new("b", TableDataType::Number(NumberDataType::UInt64));
    let field3 = TableField::new("c", TableDataType::Number(NumberDataType::UInt64));

    let schema = TableSchema::new(vec![field1, field2, field3]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.column_id_of("b").unwrap(), 1);
    assert_eq!(schema.column_id_of("c").unwrap(), 2);
    assert_eq!(schema.next_column_id(), 3);

    Ok(())
}

#[test]
fn test_schema_from_struct() -> Result<()> {
    let schema = create_a_complex_schema();

    let column_id_of_names = vec![
        ("a", 0),
        ("b", 1),
        ("b:0", 1),
        ("b:0:0", 1),
        ("b:0:1", 2),
        ("b:1", 3),
        ("b:1:0", 3),
        ("c", 4),
        ("c:0", 4),
        ("c:0:0", 4),
        ("c:0:1", 5),
        ("d", 6),
        ("d:0", 6),
        ("e", 7),
        ("e:0", 7),
        ("e:0:0", 7),
    ];
    for (name, column_id) in column_id_of_names {
        assert_eq!(schema.column_id_of(name).unwrap(), column_id,);
    }
    assert_eq!(schema.next_column_id(), 8);

    Ok(())
}

#[test]
fn test_schema_modify_field() -> Result<()> {
    let field1 = TableField::new("a", TableDataType::Number(NumberDataType::UInt64));
    let field2 = TableField::new("b", TableDataType::Number(NumberDataType::UInt64));
    let field3 = TableField::new("c", TableDataType::Number(NumberDataType::UInt64));

    let mut schema = TableSchema::new(vec![field1.clone()]);

    assert_eq!(schema.fields().to_owned(), vec![field1.clone()]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.next_column_id(), 1);

    // add column b
    schema.add_columns(&[field2.clone()])?;
    assert_eq!(schema.fields().to_owned(), vec![field1.clone(), field2,]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.column_id_of("b").unwrap(), 1);
    assert_eq!(schema.next_column_id(), 2);

    // drop column b
    schema.drop_column("b")?;
    assert_eq!(schema.fields().to_owned(), vec![field1.clone(),]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.next_column_id(), 2);

    // add column c
    schema.add_columns(&[field3.clone()])?;
    assert_eq!(schema.fields().to_owned(), vec![field1, field3]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.column_id_of("c").unwrap(), 2);
    assert_eq!(schema.next_column_id(), 3);

    // add struct column
    let child_field11 = TableDataType::Number(NumberDataType::UInt64);
    let child_field12 = TableDataType::Number(NumberDataType::UInt64);
    let child_field22 = TableDataType::Number(NumberDataType::UInt64);
    let s = TableDataType::Tuple {
        fields_name: vec!["0".to_string(), "1".to_string()],
        fields_type: vec![child_field11, child_field12],
    };
    let s2 = TableDataType::Tuple {
        fields_name: vec!["0".to_string(), "1".to_string()],
        fields_type: vec![s, child_field22],
    };
    schema.add_columns(&[TableField::new("s", s2)])?;
    assert_eq!(schema.column_id_of("s").unwrap(), 3);
    assert_eq!(schema.column_id_of("s:0").unwrap(), 3);
    assert_eq!(schema.column_id_of("s:0:0").unwrap(), 3);
    assert_eq!(schema.column_id_of("s:0:1").unwrap(), 4);
    assert_eq!(schema.column_id_of("s:1").unwrap(), 5);
    assert_eq!(schema.next_column_id(), 6);

    // drop column
    schema.drop_column("s")?;
    assert!(schema.column_id_of("s").is_err());
    assert!(schema.column_id_of("s:0").is_err());
    assert!(schema.column_id_of("s:1").is_err());
    assert!(schema.column_id_of("s:0:0").is_err());
    assert!(schema.column_id_of("s:0:1").is_err());

    Ok(())
}
