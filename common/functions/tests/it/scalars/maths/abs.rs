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

use crate::scalars::scalar_function_test::test_scalar_functions;
use crate::scalars::scalar_function_test::ScalarFunctionTest;

#[test]
fn test_abs_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "abs(-1)",
            nullable: false,
            columns: vec![Series::new([-1]).into()],
            expect: Series::new(vec![1_u32]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "abs(-10086)",
            nullable: false,
            columns: vec![Series::new([-10086]).into()],
            expect: Series::new(vec![10086_u32]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "abs('-2.0')",
            nullable: false,
            columns: vec![Series::new(["-2.0"]).into()],
            expect: Series::new(vec![2.0_f64]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "abs(true)",
            nullable: false,
            columns: vec![Series::new([false]).into()],
            expect: Series::new([0_u8]).into(),
            error: "Expected numeric types, but got Boolean",
        },
    ];

    test_scalar_functions(AbsFunction::try_create("abs(false)")?, &tests)
}
