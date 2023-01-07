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

use common_datavalues::prelude::*;
use common_exception::Result;
use pretty_assertions::assert_eq;

#[test]
fn test_schema_new_from_field() -> Result<()> {
    let field1 = DataField::new_nullable("a", u64::to_data_type());
    let field2 = DataField::new_nullable("b", u64::to_data_type());
    let field3 = DataField::new_nullable("c", u64::to_data_type());

    let schema = DataSchema::new(vec![field1, field2, field3]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.column_id_of("b").unwrap(), 1);
    assert_eq!(schema.column_id_of("c").unwrap(), 2);
    assert_eq!(schema.max_column_id(), 3);

    Ok(())
}

#[test]
fn test_schema_from_struct() -> Result<()> {
    let child_field11 = u64::to_data_type();
    let child_field12 = u64::to_data_type();
    let child_field22 = u64::to_data_type();

    let s = StructType::create_with_child_ids(
        Some(vec!["b11".to_string(), "b12".to_string()]),
        vec![child_field11, child_field12],
        Some(vec![1, 2]),
    );

    let s2 = StructType::create_with_child_ids(
        Some(vec!["b1".to_string(), "b2".to_string()]),
        vec![DataTypeImpl::Struct(s), child_field22],
        Some(vec![1, 3]),
    );

    let field1 = DataField::new_with_column_id("a", u64::to_data_type(), 0);
    let field2 = DataField::new_with_column_id("b", DataTypeImpl::Struct(s2), 1);
    let field3 = DataField::new_with_column_id("c", u64::to_data_type(), 4);

    let schema = DataSchema::new(vec![field1, field2, field3]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.column_id_of("b").unwrap(), 1);
    assert_eq!(schema.column_id_of("c").unwrap(), 4);
    assert_eq!(schema.max_column_id(), 5);

    let column_id_and_child_column_id = vec![
        (0, None, None),
        (
            1,
            Some(vec![1, 3]),
            Some(vec!["b1".to_string(), "b2".to_string()]),
        ),
        (4, None, None),
    ];

    for (i, field) in schema.fields().iter().enumerate() {
        assert_eq!(field.column_id(), Some(column_id_and_child_column_id[i].0));
        assert_eq!(
            field.child_column_ids(),
            &column_id_and_child_column_id[i].1
        );
        if let Some(child_names) = &column_id_and_child_column_id[i].2 {
            if let DataTypeImpl::Struct(s) = field.data_type() {
                assert_eq!(s.names(), &Some(child_names.clone()));
            }
            continue;
        }
        assert!(column_id_and_child_column_id[i].2.is_none());
    }
    assert_eq!(
        schema
            .column_id_of_path(&["b".to_string(), "0".to_string(), "1".to_string()])
            .unwrap(),
        2
    );

    // let mut path_indices = BTreeMap::new();
    // path_indices.insert(0, vec![0]);
    // path_indices.insert(1, vec![1, 0, 0, 1, 1]);
    // path_indices.insert(4, vec![4]);
    // let project_shcema = schema.inner_project(&path_indices);
    // assert_eq!(project_shcema, schema);

    Ok(())
}

#[test]
fn test_schema_modify_field() -> Result<()> {
    let field1 = DataField::new_with_column_id("a", u64::to_data_type(), 0);
    let field2 = DataField::new_with_column_id("b", u64::to_data_type(), 1);
    let field3 = DataField::new_with_column_id("c", u64::to_data_type(), 2);

    let mut schema = DataSchema::new(vec![DataField::new("a", u64::to_data_type())]);

    assert_eq!(schema.fields().to_owned(), vec![field1.clone()]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.max_column_id(), 1);

    // add column b
    schema.add_columns(&[field2.clone()])?;
    assert_eq!(schema.fields().to_owned(), vec![field1.clone(), field2,]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.column_id_of("b").unwrap(), 1);
    assert_eq!(schema.max_column_id(), 2);

    // drop column b
    schema.drop_column("b")?;
    assert_eq!(schema.fields().to_owned(), vec![field1.clone(),]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.max_column_id(), 2);

    // add column c
    schema.add_columns(&[field3.clone()])?;
    assert_eq!(schema.fields().to_owned(), vec![field1, field3]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.column_id_of("c").unwrap(), 2);
    assert_eq!(schema.max_column_id(), 3);

    Ok(())
}
