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
fn test_abs_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "abs(1)",
            columns: vec![Series::from_data([1_u32])],
            expect: Series::from_data(vec![1_u32]),
            error: "",
        },
        ScalarFunctionTest {
            name: "abs(-10086)",
            columns: vec![Series::from_data([-10086])],
            expect: Series::from_data(vec![10086_u32]),
            error: "",
        },
        ScalarFunctionTest {
            name: "abs('-2.0')",
            columns: vec![Series::from_data(["-2.0"])],
            expect: Series::from_data(vec![2.0_f64]),
            error: "Expected a numeric type, but got String",
        },
        ScalarFunctionTest {
            name: "abs(true)",
            columns: vec![Series::from_data([false])],
            expect: Series::from_data([0_u8]),
            error: "Expected a numeric type, but got Boolean",
        },
    ];

    test_scalar_functions(AbsFunction::try_create("abs(false)")?, &tests)
}
