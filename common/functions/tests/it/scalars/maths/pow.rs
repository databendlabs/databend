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

use crate::scalars::scalar_function2_test::test_scalar_functions;
use crate::scalars::scalar_function2_test::ScalarFunctionTest;

#[test]
fn test_pow_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "pow-with-literal",
            columns: vec![Series::from_data([2]), Series::from_data([2])],
            expect: Series::from_data(vec![4_f64]),
            error: "",
        },
        ScalarFunctionTest {
            name: "pow-with-series",
            columns: vec![Series::from_data([2, 2]), Series::from_data([2, -2])],
            expect: Series::from_data([4_f64, 0.25]),
            error: "",
        },
        ScalarFunctionTest {
            name: "pow-with-null",
            columns: vec![
                Series::from_data([Some(2), None, None]),
                Series::from_data([Some(2), Some(-2), None]),
            ],
            expect: Series::from_data([Some(4_f64), None, None]),
            error: "",
        },
        ScalarFunctionTest {
            name: "pow-x-constant",
            columns: vec![
                ConstColumn::new(Series::from_data(vec![2]), 3).arc(),
                Series::from_data([Some(2), Some(-2), None]),
            ],
            expect: Series::from_data([Some(4_f64), Some(0.25), None]),
            error: "",
        },
        ScalarFunctionTest {
            name: "pow-y-constant",
            columns: vec![
                Series::from_data([Some(2), Some(-2), None]),
                ConstColumn::new(Series::from_data(vec![2]), 3).arc(),
            ],
            expect: Series::from_data([Some(4_f64), Some(4.0), None]),
            error: "",
        },
    ];

    test_scalar_functions(PowFunction::try_create("pow")?, &tests)
}
