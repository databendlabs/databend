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

use common_datavalues::prelude::ArrayCompare;
use common_datavalues::prelude::DataColumn;
use common_datavalues::prelude::DataColumnWithField;
use common_datavalues::DataField;
use common_datavalues::DataType;
use common_datavalues::DataTypeAndNullable;
use common_exception::Result;
use common_functions::scalars::Function;

pub struct ScalarFunctionTest {
    pub name: &'static str,
    pub nullable: bool,
    pub columns: Vec<DataColumn>,
    pub expect: DataColumn,
    pub error: &'static str,
}

pub struct ScalarFunctionTestWithType {
    pub name: &'static str,
    pub nullable: bool,
    pub columns: Vec<DataColumnWithField>,
    pub expect: DataColumn,
    pub error: &'static str,
}

pub fn test_scalar_functions(
    test_function: Box<dyn Function>,
    tests: &[ScalarFunctionTest],
) -> Result<()> {
    let mut tests_with_type = Vec::with_capacity(tests.len());
    for test in tests {
        let mut arguments = Vec::with_capacity(test.columns.len());

        for (index, arg_column) in test.columns.iter().enumerate() {
            match arg_column {
                DataColumn::Constant(v, _n) => {
                    arguments.push(DataColumnWithField::new(
                        arg_column.clone(),
                        DataField::new(
                            &format!("dummy_{}", index),
                            arg_column.data_type(),
                            v.is_null(),
                        ),
                    ));
                }
                DataColumn::Array(series) if series.null_count() == 0 => {
                    arguments.push(DataColumnWithField::new(
                        arg_column.clone(),
                        DataField::new(&format!("dummy_{}", index), arg_column.data_type(), false),
                    ));
                }
                DataColumn::Array(_series) => {
                    arguments.push(DataColumnWithField::new(
                        arg_column.clone(),
                        DataField::new(&format!("dummy_{}", index), arg_column.data_type(), true),
                    ));
                }
            };
        }

        tests_with_type.push(ScalarFunctionTestWithType {
            name: test.name,
            nullable: test.nullable,
            columns: arguments,
            expect: test.expect.clone(),
            error: test.error,
        })
    }

    test_scalar_functions_with_type(test_function, &tests_with_type)
}

pub fn test_scalar_functions_with_type(
    test_function: Box<dyn Function>,
    tests: &[ScalarFunctionTestWithType],
) -> Result<()> {
    for test in tests {
        let mut rows_size = 0;
        let mut arguments_type = Vec::with_capacity(test.columns.len());

        for (_, arg_column) in test.columns.iter().enumerate() {
            match arg_column.column() {
                DataColumn::Constant(v, n) => {
                    rows_size = *n;
                    arguments_type.push(DataTypeAndNullable::create(
                        arg_column.data_type(),
                        v.is_null(),
                    ));
                }
                DataColumn::Array(series) if series.null_count() == 0 => {
                    rows_size = series.len();
                    arguments_type.push(DataTypeAndNullable::create(arg_column.data_type(), false));
                }
                DataColumn::Array(series) => {
                    rows_size = series.len();
                    arguments_type.push(DataTypeAndNullable::create(arg_column.data_type(), true));
                }
            };
        }

        // Check the return type and nullable for non-error.
        if test.error.is_empty() {
            let return_type = test_function.return_type(&arguments_type)?;
            assert_eq!(test.nullable, return_type.is_nullable(), "{}", test.name);
        }

        match eval(&test_function, rows_size, &test.columns, &arguments_type) {
            Ok(v) if !matches!(v.data_type(), DataType::Struct(_)) => {
                let cmp = v.to_array()?.eq(&test.expect.to_array()?)?;
                for s in cmp.inner() {
                    assert!(s.unwrap_or(true), "{}", test.name);
                }
            }
            Ok(v) => assert_eq!(
                format!("{:?}", v.to_array()),
                format!("{:?}", test.expect.to_array())
            ),
            Err(cause) => {
                assert_eq!(test.error, cause.message(), "{}", test.name);
            }
        }
    }

    Ok(())
}

#[allow(clippy::borrowed_box)]
fn eval(
    test_function: &Box<dyn Function>,
    rows_size: usize,
    arguments: &[DataColumnWithField],
    arguments_type: &[DataTypeAndNullable],
) -> Result<DataColumn> {
    test_function.return_type(arguments_type)?;
    test_function.eval(arguments, rows_size)
}
