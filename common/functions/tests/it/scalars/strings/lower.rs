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
use common_functions::scalars::LowerFunction;

use crate::scalars::scalar_function2_test::test_scalar_functions;
use crate::scalars::scalar_function2_test::ScalarFunctionTest;

#[test]
fn test_lower_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "lower-abc-passed",
            columns: vec![Series::from_data(vec!["Abc"])],
            expect: Series::from_data(vec!["abc"]),
            error: "",
        },
        ScalarFunctionTest {
            name: "lower-utf8-passed",
            columns: vec![Series::from_data(vec!["Dobrý den"])],
            expect: Series::from_data(vec!["dobrý den"]),
            error: "",
        },
        ScalarFunctionTest {
            name: "lcase-utf8-passed",
            columns: vec![Series::from_data(vec!["Dobrý den"])],
            expect: Series::from_data(vec!["dobrý den"]),
            error: "",
        },
    ];

    test_scalar_functions(LowerFunction::try_create("lower")?, &tests)
}

#[test]
fn test_lower_nullable() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "lcase-null-passed",
        columns: vec![Series::from_data(vec![Option::<Vec<u8>>::None])],
        expect: Series::from_data(vec![Option::<Vec<u8>>::None]),
        error: "",
    }];

    test_scalar_functions(LowerFunction::try_create("lcase")?, &tests)
}
