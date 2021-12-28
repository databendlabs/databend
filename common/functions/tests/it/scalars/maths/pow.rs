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
fn test_pow_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "pow-with-literal",
            nullable: false,
            columns: vec![Series::new([2]).into(), Series::new([2]).into()],
            expect: DataColumn::Constant(4_f64.into(), 1),
            error: "",
        },
        ScalarFunctionTest {
            name: "pow-with-series",
            nullable: false,
            columns: vec![Series::new([2, 2]).into(), Series::new([2, -2]).into()],
            expect: Series::new([4_f64, 0.25]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "pow-with-null",
            nullable: true,
            columns: vec![
                Series::new([Some(2), None, None]).into(),
                Series::new([Some(2), Some(-2), None]).into(),
            ],
            expect: Series::new([Some(4_f64), None, None]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "pow-x-constant",
            nullable: true,
            columns: vec![
                DataColumn::Constant(2.into(), 2),
                Series::new([Some(2), Some(-2), None]).into(),
            ],
            expect: Series::new([Some(4_f64), Some(0.25), None]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "pow-y-constant",
            nullable: true,
            columns: vec![
                Series::new([Some(2), Some(-2), None]).into(),
                DataColumn::Constant(2.into(), 2),
            ],
            expect: Series::new([Some(4_f64), Some(4.0), None]).into(),
            error: "",
        },
    ];

    test_scalar_functions(PowFunction::try_create("pow")?, &tests)
}
