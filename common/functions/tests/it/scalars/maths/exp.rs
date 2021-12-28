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
use crate::scalars::scalar_function_test::{ScalarFunctionTest, test_scalar_functions};

#[test]
fn test_exp_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "exp-with-literal",
            nullable: false,
            columns: vec![Series::new(vec![2]).into()],
            expect: Series::new(vec![7.38905609893065_f64]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "exp-with-series",
            nullable: false,
            columns: vec![Series::new(vec![2, -2, 0]).into()],
            expect: Series::new(vec![7.38905609893065_f64, 0.1353352832366127, 1_f64]).into(),
            error: "",
        },
    ];

    test_scalar_functions(ExpFunction::try_create("exp")?, &tests)
}
