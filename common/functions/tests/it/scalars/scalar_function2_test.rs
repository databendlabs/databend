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
use common_exception::Result;
use common_functions::scalars::Function;
use common_functions::scalars::FunctionAdapter;
use pretty_assertions::assert_eq;

pub struct ScalarFunctionTest {
    pub name: &'static str,
    pub columns: Vec<ColumnRef>,
    pub expect: ColumnRef,
    pub error: &'static str,
}

pub struct ScalarFunctionWithFieldTest {
    pub name: &'static str,
    pub columns: Vec<ColumnWithField>,
    pub expect: ColumnRef,
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
            let f = ColumnWithField::new(
                arg_column.clone(),
                DataField::new(&format!("dummy_{}", index), arg_column.data_type()),
            );

            arguments.push(f);
        }

        tests_with_type.push(ScalarFunctionWithFieldTest {
            name: test.name,
            columns: arguments,
            expect: test.expect.clone(),
            error: test.error,
        })
    }

    test_scalar_functions_with_type(test_function, &tests_with_type)
}

pub fn test_scalar_functions_with_type(
    test_function: Box<dyn Function>,
    tests: &[ScalarFunctionWithFieldTest],
) -> Result<()> {
    for test in tests {
        let mut rows_size = 0;
        let mut arguments_type = Vec::with_capacity(test.columns.len());

        for c in test.columns.iter() {
            arguments_type.push(c.data_type());
            rows_size = c.column().len();
        }

        match test_eval_with_type(&test_function, rows_size, &test.columns, &arguments_type) {
            Ok(v) => {
                let v = v.convert_full_column();

                assert_eq!(test.expect, v, "{}", test.name);
            }
            Err(cause) => {
                assert_eq!(test.error, cause.message(), "{}", test.name);
            }
        }
    }

    Ok(())
}

#[allow(clippy::borrowed_box)]
pub fn test_eval(test_function: &Box<dyn Function>, columns: &[ColumnRef]) -> Result<ColumnRef> {
    let mut rows_size = 0;
    let mut arguments = Vec::with_capacity(columns.len());
    let mut arguments_type = Vec::with_capacity(columns.len());

    for (index, arg_column) in columns.iter().enumerate() {
        let f = ColumnWithField::new(
            arg_column.clone(),
            DataField::new(&format!("dummy_{}", index), arg_column.data_type()),
        );

        arguments_type.push(arg_column.data_type());

        rows_size = arg_column.len();
        arguments.push(f);
    }

    let mut types = Vec::with_capacity(columns.len());
    for t in arguments_type.iter() {
        types.push(t);
    }

    test_eval_with_type(test_function, rows_size, &arguments, &types)
}

#[allow(clippy::borrowed_box)]
pub fn test_eval_with_type(
    test_function: &Box<dyn Function>,
    rows_size: usize,
    arguments: &[ColumnWithField],
    arguments_type: &[&DataTypePtr],
) -> Result<ColumnRef> {
    let adaptor = FunctionAdapter::create(test_function.clone());
    adaptor.return_type(arguments_type)?;
    adaptor.eval(arguments, rows_size)
}
