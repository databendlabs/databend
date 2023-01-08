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

    let s = StructType::create(None, vec![child_field11, child_field12]);

    let s2 = StructType::create(None, vec![DataTypeImpl::Struct(s), child_field22]);

    let field1 = DataField::new("a", u64::to_data_type());
    let field2 = DataField::new("b", DataTypeImpl::Struct(s2));
    let field3 = DataField::new("c", u64::to_data_type());

    let schema = DataSchema::new(vec![field1, field2, field3]);

    let column_id_of_names = vec![
        ("a", 0),
        ("b", 1),
        ("b:0", 2),
        ("b:0:0", 3),
        ("b:0:1", 4),
        ("b:1", 5),
        ("c", 6),
    ];
    for (name, column_id) in column_id_of_names {
        assert_eq!(schema.column_id_of(name).unwrap(), column_id,);
    }
    assert_eq!(schema.max_column_id(), 7);

    assert_eq!(
        schema
            .column_id_of_path(&["b".to_string(), "0".to_string(), "1".to_string()])
            .unwrap(),
        4
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
    let field1 = DataField::new("a", u64::to_data_type());
    let field2 = DataField::new("b", u64::to_data_type());
    let field3 = DataField::new("c", u64::to_data_type());

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

    // add struct column
    let child_field11 = u64::to_data_type();
    let child_field12 = u64::to_data_type();
    let child_field22 = u64::to_data_type();
    let s = StructType::create(None, vec![child_field11, child_field12]);
    let s2 = StructType::create(None, vec![DataTypeImpl::Struct(s), child_field22]);
    schema.add_columns(&[DataField::new("s", DataTypeImpl::Struct(s2))])?;
    assert_eq!(schema.column_id_of("s").unwrap(), 3);
    assert_eq!(schema.column_id_of("s:0").unwrap(), 4);
    assert_eq!(schema.column_id_of("s:0:0").unwrap(), 5);
    assert_eq!(schema.column_id_of("s:0:1").unwrap(), 6);
    assert_eq!(schema.column_id_of("s:1").unwrap(), 7);
    assert_eq!(schema.max_column_id(), 8);

    // drop column
    schema.drop_column("s")?;
    assert!(schema.column_id_of("s").is_err());
    assert!(schema.column_id_of("s:0").is_err());
    assert!(schema.column_id_of("s:1").is_err());
    assert!(schema.column_id_of("s:0:0").is_err());
    assert!(schema.column_id_of("s:0:1").is_err());

    Ok(())
}
