// Copyright 2021 Datafuse Labs.
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
use common_datavalues::DataType;
use common_exception::Result;
use common_functions::scalars::*;
use pretty_assertions::assert_eq;

#[test]
fn test_tuple_function() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int32, false),
        DataField::new("b", DataType::Int64, false),
        DataField::new("c", DataType::Float32, false),
    ]);

    let columns: Vec<DataColumn> = vec![
        Series::new(vec![1i32, 2, 3]).into(),
        Series::new(vec![4i64, 5, 6]).into(),
        Series::new(vec![7.1f32, 8.2, 9.3]).into(),
    ];

    let rows = 3;
    let func = TupleFunction::try_create_func("")?;

    let mut args = vec![];
    let mut fields = vec![];
    for name in &["a", "b", "c"] {
        args.push(schema.field_with_name(name)?.data_type().clone());
        fields.push(schema.field_with_name(name)?.clone());
    }

    let input: Vec<DataColumnWithField> = columns
        .iter()
        .zip(fields.iter())
        .map(|(c, f)| DataColumnWithField::new(c.clone(), f.clone()))
        .collect();

    // Display check.
    let expect_display = "TUPLE".to_string();
    let actual_display = format!("{}", func);
    assert_eq!(expect_display, actual_display);

    // Nullable check.
    let actual_null = func.nullable(schema.fields())?;
    assert!(!actual_null);

    let expect_type = func.return_type(&args)?;
    let v = &(func.eval(&input, rows)?);
    let actual_type = v.data_type();
    assert_eq!(expect_type, actual_type);

    let expect_values = "[[1, 4, 7.1], [2, 5, 8.2], [3, 6, 9.3]]".to_string();
    let actual_values = format!("{:?}", v.to_values()?);
    assert_eq!(expect_values, actual_values);

    Ok(())
}
