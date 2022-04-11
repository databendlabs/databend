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

use crate::scalars::scalar_function2_test::test_eval;
use crate::scalars::scalar_function2_test::ScalarFunctionTest;

#[test]
fn test_floor_function() -> Result<()> {
    let tests = vec![
        (Int32Type::arc(), ScalarFunctionTest {
            name: "floor(123)",
            columns: vec![Series::from_data([123])],
            expect: Series::from_data(vec![123_f64]),
            error: "",
        }),
        (Float64Type::arc(), ScalarFunctionTest {
            name: "floor(1.7)",
            columns: vec![Series::from_data([1.7])],
            expect: Series::from_data(vec![1_f64]),
            error: "",
        }),
        (Float64Type::arc(), ScalarFunctionTest {
            name: "floor(-2.1)",
            columns: vec![Series::from_data([-2.1])],
            expect: Series::from_data(vec![-3_f64]),
            error: "",
        }),
        (StringType::arc(), ScalarFunctionTest {
            name: "floor('123')",
            columns: vec![Series::from_data(["123"])],
            expect: Series::from_data(vec![123_f64]),
            error: "Expected a numeric type, but got String",
        }),
        (StringType::arc(), ScalarFunctionTest {
            name: "floor('+123.8a1')",
            columns: vec![Series::from_data(["+123.8a1"])],
            expect: Series::from_data(vec![123_f64]),
            error: "Expected a numeric type, but got String",
        }),
        (StringType::arc(), ScalarFunctionTest {
            name: "floor('-123.2a1')",
            columns: vec![Series::from_data(["-123.2a1"])],
            expect: Series::from_data(vec![-124_f64]),
            error: "Expected a numeric type, but got String",
        }),
        (StringType::arc(), ScalarFunctionTest {
            name: "floor('a')",
            columns: vec![Series::from_data(["a"])],
            expect: Series::from_data(vec![0_f64]),
            error: "Expected a numeric type, but got String",
        }),
        (StringType::arc(), ScalarFunctionTest {
            name: "floor('a123')",
            columns: vec![Series::from_data(["a123"])],
            expect: Series::from_data(vec![0_f64]),
            error: "Expected a numeric type, but got String",
        }),
        (BooleanType::arc(), ScalarFunctionTest {
            name: "floor(true)",
            columns: vec![Series::from_data([true])],
            expect: Series::from_data([1_u8]),
            error: "Expected a numeric type, but got Boolean",
        }),
    ];

    for (typ, test) in tests {
        match FloorFunction::try_create("floor", &[&typ]) {
            Ok(f) => {
                test_eval(&f, &test.columns, true)?;
            }
            Err(cause) => {
                assert_eq!(test.error, cause.message(), "{}", test.name);
            }
        }
    }

    Ok(())
}
