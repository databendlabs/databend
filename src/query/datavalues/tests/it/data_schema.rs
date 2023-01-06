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
    assert_eq!(schema.max_column_id(), 2);

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
    assert_eq!(schema.max_column_id(), 0);

    // add column b
    schema.add_columns(&[field2.clone()])?;
    assert_eq!(schema.fields().to_owned(), vec![field1.clone(), field2,]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.column_id_of("b").unwrap(), 1);
    assert_eq!(schema.max_column_id(), 1);

    // drop column b
    schema.drop_column("b")?;
    assert_eq!(schema.fields().to_owned(), vec![field1.clone(),]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.max_column_id(), 1);

    // add column c
    schema.add_columns(&[field3.clone()]);
    assert_eq!(schema.fields().to_owned(), vec![field1, field3]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.column_id_of("c").unwrap(), 2);
    assert_eq!(schema.max_column_id(), 2);

    Ok(())
}
