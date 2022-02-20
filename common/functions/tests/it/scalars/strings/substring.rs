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
use common_functions::scalars::SubstringFunction;

use crate::scalars::scalar_function2_test::test_scalar_functions;
use crate::scalars::scalar_function2_test::ScalarFunctionTest;

#[test]
fn test_substring_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "substring-abcde-passed",
            columns: vec![
                Series::from_data(vec!["abcde"]),
                Series::from_data(vec![2_i64]),
                Series::from_data(vec![3_u64]),
            ],
            expect: Series::from_data(vec!["bcd"]),
            error: "",
        },
        ScalarFunctionTest {
            name: "substring-abcde-passed",
            columns: vec![
                Series::from_data(vec!["abcde"]),
                Series::from_data(vec![1_i64]),
                Series::from_data(vec![3_u64]),
            ],
            expect: Series::from_data(vec!["abc"]),
            error: "",
        },
        ScalarFunctionTest {
            name: "substring-abcde-passed",
            columns: vec![
                Series::from_data(vec!["abcde"]),
                Series::from_data(vec![2_i64]),
            ],
            expect: Series::from_data(vec!["bcde"]),
            error: "",
        },
        ScalarFunctionTest {
            name: "substring-1234567890-passed",
            columns: vec![
                Series::from_data(vec!["1234567890"]),
                Series::from_data(vec![-3_i64]),
                Series::from_data(vec![3_u64]),
            ],
            expect: Series::from_data(vec!["890"]),
            error: "",
        },
    ];

    test_scalar_functions(SubstringFunction::try_create("substring")?, &tests)
}

#[test]
fn test_substring_nullable() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "substring-nullabe-passed",
        columns: vec![
            Series::from_data(vec!["abcde"]),
            Series::from_data(vec![2_i64]),
            Series::from_data(vec![3_u64]),
        ],
        expect: Series::from_data(vec!["bcd"]),
        error: "",
    }];

    test_scalar_functions(SubstringFunction::try_create("substring")?, &tests)
}
