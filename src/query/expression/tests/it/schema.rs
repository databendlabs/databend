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
use common_expression::create_test_complex_schema;
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
        fields_type: vec![b1.clone(), TableDataType::Number(NumberDataType::Int64)],
    };
    let fields = vec![
        TableField::new("a", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new("b", b.clone()),
        TableField::new("c", TableDataType::Number(NumberDataType::UInt64)),
    ];
    let mut schema = TableSchema::new(fields);

    // project schema
    {
        let expect_fields = vec![
            TableField::new_from_column_ids(
                "a",
                TableDataType::Number(NumberDataType::UInt64),
                0,
                vec![0],
            ),
            TableField::new_from_column_ids("b:b1:b11", TableDataType::Boolean, 1, vec![1]),
            TableField::new_from_column_ids("b:b1:b12", TableDataType::String, 2, vec![2]),
            TableField::new_from_column_ids(
                "b:b2",
                TableDataType::Number(NumberDataType::Int64),
                3,
                vec![3],
            ),
            TableField::new_from_column_ids("b:b1", b1.clone(), 1, vec![1, 1, 2]),
            TableField::new_from_column_ids("b", b.clone(), 1, vec![1, 1, 1, 2, 3]),
        ];

        let mut path_indices = BTreeMap::new();
        path_indices.insert(0, vec![0]);
        path_indices.insert(1, vec![1, 0, 0]);
        path_indices.insert(2, vec![1, 0, 1]);
        path_indices.insert(3, vec![1, 1]);
        path_indices.insert(4, vec![1, 0]);
        path_indices.insert(5, vec![1]);
        let project_schema = schema.inner_project(&path_indices);

        for (i, field) in project_schema.fields().iter().enumerate() {
            assert_eq!(*field, expect_fields[i]);
        }
        assert_eq!(project_schema.next_column_id(), schema.next_column_id());
        assert_eq!(project_schema.column_id_set(), schema.column_id_set());
    }

    // drop column
    {
        schema.drop_column("b")?;
        let mut path_indices = BTreeMap::new();
        path_indices.insert(0, vec![0]);
        path_indices.insert(1, vec![1]);
        let project_schema = schema.inner_project(&path_indices);

        let expect_fields = vec![
            TableField::new_from_column_ids(
                "a",
                TableDataType::Number(NumberDataType::UInt64),
                0,
                vec![0],
            ),
            TableField::new_from_column_ids(
                "c",
                TableDataType::Number(NumberDataType::UInt64),
                4,
                vec![4],
            ),
        ];

        for (i, field) in project_schema.fields().iter().enumerate() {
            assert_eq!(*field, expect_fields[i]);
        }
        assert_eq!(project_schema.next_column_id(), schema.next_column_id());
        assert_eq!(project_schema.column_id_set(), schema.column_id_set());
    }

    // add column
    {
        schema.add_columns(&[TableField::new("b", b.clone())])?;

        let mut path_indices = BTreeMap::new();
        path_indices.insert(0, vec![0]);
        path_indices.insert(1, vec![1]);
        path_indices.insert(2, vec![2, 0, 0]);
        path_indices.insert(3, vec![2, 0, 1]);
        path_indices.insert(4, vec![2, 1]);
        path_indices.insert(5, vec![2, 0]);
        path_indices.insert(6, vec![2]);

        let expect_fields = vec![
            TableField::new_from_column_ids(
                "a",
                TableDataType::Number(NumberDataType::UInt64),
                0,
                vec![0],
            ),
            TableField::new_from_column_ids(
                "c",
                TableDataType::Number(NumberDataType::UInt64),
                4,
                vec![4],
            ),
            TableField::new_from_column_ids("b:b1:b11", TableDataType::Boolean, 5, vec![5]),
            TableField::new_from_column_ids("b:b1:b12", TableDataType::String, 6, vec![6]),
            TableField::new_from_column_ids(
                "b:b2",
                TableDataType::Number(NumberDataType::Int64),
                7,
                vec![7],
            ),
            TableField::new_from_column_ids("b:b1", b1, 5, vec![5, 5, 6]),
            TableField::new_from_column_ids("b", b, 5, vec![5, 5, 5, 6, 7]),
        ];
        let project_schema = schema.inner_project(&path_indices);

        for (i, field) in project_schema.fields().iter().enumerate() {
            assert_eq!(*field, expect_fields[i]);
        }
        assert_eq!(project_schema.next_column_id(), schema.next_column_id());
        assert_eq!(project_schema.column_id_set(), schema.column_id_set());
    }

    Ok(())
}

#[test]
fn test_schema_from_simple_type() -> Result<()> {
    let field1 = TableField::new("a", TableDataType::Number(NumberDataType::UInt64));
    let field2 = TableField::new("b", TableDataType::Number(NumberDataType::UInt64));
    let field3 = TableField::new(
        "c",
        TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
    );

    let schema = TableSchema::new(vec![field1, field2, field3]);
    assert_eq!(schema.to_column_ids(), vec![0, 1, 2]);
    assert_eq!(schema.to_leaf_column_ids(), vec![0, 1, 2]);
    assert_eq!(schema.next_column_id(), 3);

    let (leaf_column_ids, leaf_fields) = schema.leaf_fields();
    assert_eq!(leaf_column_ids, schema.to_leaf_column_ids());
    let leaf_field_names = vec!["a", "b", "c"];
    for (i, field) in leaf_fields.iter().enumerate() {
        assert_eq!(field.name(), leaf_field_names[i])
    }

    Ok(())
}

#[test]
fn test_schema_from_struct() -> Result<()> {
    let schema = create_test_complex_schema();
    let flat_column_ids = schema.to_leaf_column_ids();

    let (leaf_column_ids, leaf_fields) = schema.leaf_fields();
    assert_eq!(leaf_column_ids, schema.to_leaf_column_ids());
    let expected_fields = vec![
        ("u64", TableDataType::Number(NumberDataType::UInt64)),
        ("0", TableDataType::Number(NumberDataType::UInt64)),
        ("1", TableDataType::Number(NumberDataType::UInt64)),
        ("1:0", TableDataType::Number(NumberDataType::UInt64)),
        ("0", TableDataType::Number(NumberDataType::UInt64)),
        ("1", TableDataType::Number(NumberDataType::UInt64)),
        (
            "nullarray",
            TableDataType::Nullable(Box::new(TableDataType::Array(Box::new(
                TableDataType::Number(NumberDataType::UInt64),
            )))),
        ),
        (
            "maparray",
            TableDataType::Map(Box::new(TableDataType::Array(Box::new(
                TableDataType::Number(NumberDataType::UInt64),
            )))),
        ),
        (
            "nullu64",
            TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
        ),
        ("u64array:0", TableDataType::Number(NumberDataType::UInt64)),
        ("a", TableDataType::Number(NumberDataType::Int32)),
        ("b", TableDataType::Number(NumberDataType::Int32)),
    ];
    for (i, field) in leaf_fields.iter().enumerate() {
        let expected_field = &expected_fields[i];
        assert_eq!(field.name(), expected_field.0);
        assert_eq!(field.data_type().to_owned(), expected_field.1);
    }

    let expeted_column_ids = vec![
        ("u64", vec![0]),
        ("tuplearray", vec![1, 1, 1, 2, 3, 3]),
        ("arraytuple", vec![4, 4, 4, 5]),
        ("nullarray", vec![6]),
        ("maparray", vec![7]),
        ("nullu64", vec![8]),
        ("u64array", vec![9, 9]),
        ("tuplesimple", vec![10, 10, 11]),
    ];

    for (i, column_id) in schema.field_column_ids().iter().enumerate() {
        let expeted_column_id = &expeted_column_ids[i];
        assert_eq!(
            expeted_column_id.0.to_string(),
            schema.fields()[i].name().to_string()
        );
        assert_eq!(expeted_column_id.1, **column_id);
    }

    let expeted_flat_column_ids = vec![
        ("u64", vec![0]),
        ("tuplearray", vec![1, 2, 3]),
        ("arraytuple", vec![4, 5]),
        ("nullarray", vec![6]),
        ("maparray", vec![7]),
        ("nullu64", vec![8]),
        ("u64array", vec![9]),
        ("tuplesimple", vec![10, 11]),
    ];

    for (i, field) in schema.fields().iter().enumerate() {
        let expeted_column_id = &expeted_flat_column_ids[i];
        assert_eq!(expeted_column_id.0.to_string(), field.name().to_string());
        assert_eq!(expeted_column_id.1, field.leaf_column_ids());
    }

    assert_eq!(schema.next_column_id(), 12);

    // make sure column ids is adjacent integers(in case there is no add or drop column operations)
    assert_eq!(flat_column_ids.len(), schema.next_column_id() as usize);
    for i in 1..flat_column_ids.len() {
        assert_eq!(flat_column_ids[i], flat_column_ids[i - 1] + 1);
    }

    Ok(())
}

#[test]
fn test_schema_modify_field() -> Result<()> {
    let field1 = TableField::new("a", TableDataType::Number(NumberDataType::UInt64));
    let field2 = TableField::new("b", TableDataType::Number(NumberDataType::UInt64));
    let field3 = TableField::new("c", TableDataType::Number(NumberDataType::UInt64));

    let mut schema = TableSchema::new(vec![field1]);

    let expected_field1 = TableField::new_from_column_ids(
        "a",
        TableDataType::Number(NumberDataType::UInt64),
        0,
        vec![0],
    );
    let expected_field2 = TableField::new_from_column_ids(
        "b",
        TableDataType::Number(NumberDataType::UInt64),
        1,
        vec![1],
    );
    let expected_field3 = TableField::new_from_column_ids(
        "c",
        TableDataType::Number(NumberDataType::UInt64),
        2,
        vec![2],
    );

    assert_eq!(schema.fields().to_owned(), vec![expected_field1.clone()]);
    // assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.is_column_deleted(0), false);
    assert_eq!(schema.next_column_id(), 1);

    // add column b
    schema.add_columns(&[field2])?;
    assert_eq!(schema.fields().to_owned(), vec![
        expected_field1.clone(),
        expected_field2,
    ]);
    // assert_eq!(schema.column_id_of("a").unwrap(), 0);
    // assert_eq!(schema.column_id_of("b").unwrap(), 1);
    assert_eq!(schema.is_column_deleted(0), false);
    assert_eq!(schema.is_column_deleted(1), false);
    assert_eq!(schema.next_column_id(), 2);

    // drop column b
    schema.drop_column("b")?;
    assert_eq!(schema.fields().to_owned(), vec![expected_field1.clone(),]);
    // assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.is_column_deleted(0), false);
    assert_eq!(schema.is_column_deleted(1), true);
    assert_eq!(schema.next_column_id(), 2);

    // add column c
    schema.add_columns(&[field3])?;
    assert_eq!(schema.fields().to_owned(), vec![
        expected_field1,
        expected_field3
    ]);
    // assert_eq!(schema.column_id_of("a").unwrap(), 0);
    // assert_eq!(schema.column_id_of("c").unwrap(), 2);
    assert_eq!(schema.is_column_deleted(0), false);
    assert_eq!(schema.is_column_deleted(1), true);
    assert_eq!(schema.is_column_deleted(2), false);
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
    // assert_eq!(schema.column_id_of("s").unwrap(), 3);
    // assert_eq!(schema.column_id_of("s:0").unwrap(), 3);
    // assert_eq!(schema.column_id_of("s:0:0").unwrap(), 3);
    // assert_eq!(schema.column_id_of("s:0:1").unwrap(), 4);
    // assert_eq!(schema.column_id_of("s:1").unwrap(), 5);
    assert_eq!(schema.is_column_deleted(0), false);
    assert_eq!(schema.is_column_deleted(1), true);
    assert_eq!(schema.is_column_deleted(2), false);
    assert_eq!(schema.is_column_deleted(3), false);
    assert_eq!(schema.next_column_id(), 6);

    // add array column
    let ary = TableDataType::Array(Box::new(TableDataType::Array(Box::new(
        TableDataType::Number(NumberDataType::UInt64),
    ))));
    schema.add_columns(&[TableField::new("ary", ary)])?;
    // assert_eq!(schema.column_id_of("ary").unwrap(), 6);
    // assert_eq!(schema.column_id_of("ary:0").unwrap(), 6);
    assert_eq!(schema.is_column_deleted(0), false);
    assert_eq!(schema.is_column_deleted(1), true);
    assert_eq!(schema.is_column_deleted(2), false);
    assert_eq!(schema.is_column_deleted(3), false);
    assert_eq!(schema.is_column_deleted(6), false);
    assert_eq!(schema.next_column_id(), 7);

    // drop column
    schema.drop_column("s")?;
    assert_eq!(schema.is_column_deleted(0), false);
    assert_eq!(schema.is_column_deleted(1), true);
    assert_eq!(schema.is_column_deleted(2), false);
    assert_eq!(schema.is_column_deleted(3), true);
    assert_eq!(schema.is_column_deleted(6), false);
    // assert!(schema.column_id_of("s").is_err());
    // assert!(schema.column_id_of("s:0").is_err());
    // assert!(schema.column_id_of("s:1").is_err());
    // assert!(schema.column_id_of("s:0:0").is_err());
    // assert!(schema.column_id_of("s:0:1").is_err());

    Ok(())
}
