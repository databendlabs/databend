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
use common_functions::scalars::*;
use pretty_assertions::assert_eq;
use crate::scalars::scalar_function_test::{ScalarFunctionTest, ScalarFunctionTestWithType, test_scalar_functions, test_scalar_functions_with_type};

#[test]
fn test_cast_function() -> Result<()> {
    let tests = vec![
        (
            CastFunction::create("toint8".to_string(), DataType::Int8)?,
            ScalarFunctionTest {
                name: "cast-int64-to-int8-passed",
                nullable: false,
                columns: vec![Series::new(vec![4i64, 3, 2, 4]).into()],
                expect: Series::new(vec![4i8, 3, 2, 4]).into(),
                error: "",
            }
        ),
        (
            CastFunction::create("toint8".to_string(), DataType::Int8)?,
            ScalarFunctionTest {
                name: "cast-string-to-int8-passed",
                nullable: false,
                columns: vec![Series::new(vec!["4", "3", "2", "4"]).into()],
                expect: Series::new(vec![4i8, 3, 2, 4]).into(),
                error: "",
            }
        ),
        (
            CastFunction::create("toint16".to_string(), DataType::Int16)?,
            ScalarFunctionTest {
                name: "cast-string-to-int16-passed",
                nullable: false,
                columns: vec![Series::new(vec!["4", "3", "2", "4"]).into()],
                expect: Series::new(vec![4i16, 3, 2, 4]).into(),
                error: "",
            }
        ),
        (
            CastFunction::create("toint32".to_string(), DataType::Int32)?,
            ScalarFunctionTest {
                name: "cast-string-to-int32-passed",
                nullable: false,
                columns: vec![Series::new(vec!["4", "3", "2", "4"]).into()],
                expect: Series::new(vec![4i32, 3, 2, 4]).into(),
                error: "",
            }
        ),
        (
            CastFunction::create("toint64".to_string(), DataType::Int64)?,
            ScalarFunctionTest {
                name: "cast-string-to-int64-passed",
                nullable: false,
                columns: vec![Series::new(vec!["4", "3", "2", "4"]).into()],
                expect: Series::new(vec![4i64, 3, 2, 4]).into(),
                error: "",
            }
        ),
        (
            CastFunction::create("cast".to_string(), DataType::Date16)?,
            ScalarFunctionTest {
                name: "cast-string-to-date16-passed",
                nullable: false,
                columns: vec![Series::new(vec!["2021-03-05", "2021-10-24"]).into()],
                expect: Series::new(vec![18691u16, 18924]).into(),
                error: "",
            }
        ),
        (
            CastFunction::create("cast".to_string(), DataType::Date32)?,
            ScalarFunctionTest {
                name: "cast-string-to-date32-passed",
                nullable: false,
                columns: vec![Series::new(vec!["2021-03-05", "2021-10-24"]).into()],
                expect: Series::new(vec![18691i32, 18924]).into(),
                error: "",
            }),
        (
            CastFunction::create("cast".to_string(), DataType::DateTime32(None))?,
            ScalarFunctionTest {
                name: "cast-string-to-datetime32-passed",
                nullable: false,
                columns: vec![Series::new(vec!["2021-03-05 01:01:01", "2021-10-24 10:10:10"]).into()],
                expect: Series::new(vec![1614906061u32, 1635070210]).into(),
                error: "",
            }
        ),
    ];

    for (test_func, test) in tests {
        test_scalar_functions(test_func, &[test])?;
    }

    Ok(())
}

#[test]
fn test_datetime_cast_function() -> Result<()> {
    let tests = vec![
        (
            CastFunction::create("cast".to_string(), DataType::String)?,
            ScalarFunctionTestWithType {
                name: "cast-date32-to-string-passed",
                nullable: false,
                columns: vec![
                    DataColumnWithField::new(
                        Series::new(vec![18691i32, 18924]).into(),
                        DataField::new("dummy_1", DataType::Date32, false),
                    )
                ],
                expect: Series::new(vec!["2021-03-05", "2021-10-24"]).into(),
                error: "",
            }),
        (
            CastFunction::create("cast".to_string(), DataType::String)?,
            ScalarFunctionTestWithType {
                name: "cast-datetime-to-string-passed",
                nullable: false,
                columns: vec![
                    DataColumnWithField::new(
                        Series::new(vec![1614906061u32, 1635070210]).into(),
                        DataField::new("dummy_1", DataType::DateTime32(None), false),
                    )
                ],
                expect: Series::new(vec!["2021-03-05 01:01:01", "2021-10-24 10:10:10"]).into(),
                error: "",
            }
        ),
    ];

    for (test_func, test) in tests {
        test_scalar_functions_with_type(test_func, &[test])?;
    }

    Ok(())
}
