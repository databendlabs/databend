use common_datavalues::{DataField, DataTypeAndNullable};
use common_datavalues::prelude::{ArrayCompare, DataColumn, DataColumnsWithField, DataColumnWithField, ToValues};
use common_datavalues::series::{Series, SeriesWrap};
use common_functions::scalars::{Function, FunctionFactory};
use common_exception::Result;

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

pub fn test_scalar_functions(test_function: Box<dyn Function>, tests: &[ScalarFunctionTest]) -> Result<()> {
    let mut tests_with_type = Vec::with_capacity(tests.len());
    for test in tests {
        let mut arguments = Vec::with_capacity(test.columns.len());

        for (index, arg_column) in test.columns.iter().enumerate() {
            match arg_column {
                DataColumn::Constant(v, n) => {
                    arguments.push(DataColumnWithField::new(
                        arg_column.clone(),
                        DataField::new(&format!("dummy_{}", index), arg_column.data_type(), v.is_null()),
                    ));
                }
                DataColumn::Array(series) if series.null_count() == 0 => {
                    arguments.push(DataColumnWithField::new(
                        arg_column.clone(),
                        DataField::new(&format!("dummy_{}", index), arg_column.data_type(), false),
                    ));
                }
                DataColumn::Array(series) => {
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

pub fn test_scalar_functions_with_type(test_function: Box<dyn Function>, tests: &[ScalarFunctionTestWithType]) -> Result<()> {
    for test in tests {
        let mut rows_size = 0;
        let mut arguments_type = Vec::with_capacity(test.columns.len());

        for (index, arg_column) in test.columns.iter().enumerate() {
            match arg_column.column() {
                DataColumn::Constant(v, n) => {
                    rows_size = *n;
                    arguments_type.push(DataTypeAndNullable::create(&arg_column.data_type(), v.is_null()));
                }
                DataColumn::Array(series) if series.null_count() == 0 => {
                    rows_size = series.len();
                    arguments_type.push(DataTypeAndNullable::create(&arg_column.data_type(), false));
                }
                DataColumn::Array(series) => {
                    rows_size = series.len();
                    arguments_type.push(DataTypeAndNullable::create(&arg_column.data_type(), true));
                }
            };
        }

        assert_eq!(test.nullable, test_function.nullable(&arguments_type)?, "{}", test.name);
        match eval(&test_function, rows_size, &test.columns, &arguments_type) {
            Ok(v) => {
                println!("{:?} eq {:?}", v.to_values()?, test.expect.to_values()?);
                let mut cmp = v.to_array()?.eq(&test.expect.to_array()?)?;
                for s in cmp.inner() {
                    assert!(s.unwrap_or(true), "{}", test.name);
                }
            }
            Err(cause) => {
                assert_eq!(test.error, cause.message(), "{}", test.name);
            }
        }
    }

    Ok(())
}


fn eval(test_function: &Box<dyn Function>, rows_size: usize, arguments: &[DataColumnWithField], arguments_type: &[DataTypeAndNullable]) -> Result<DataColumn> {
    test_function.return_type(arguments_type)?;
    test_function.eval(arguments, rows_size)
}

