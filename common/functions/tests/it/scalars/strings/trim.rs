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
use common_functions::scalars::LTrimFunction;
use common_functions::scalars::RTrimFunction;
use common_functions::scalars::TrimFunction;

use crate::scalars::scalar_function2_test::test_scalar_functions;
use crate::scalars::scalar_function2_test::ScalarFunctionTest;

#[test]
fn test_ltrim_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "ltrim-abc-passed",
        columns: vec![Series::from_data(vec!["  abc"])],
        expect: Series::from_data(vec!["abc"]),
        error: "",
    }];

    test_scalar_functions(LTrimFunction::try_create("ltrim")?, &tests)
}

#[test]
fn test_rtrim_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "rtrim-abc-passed",
        columns: vec![Series::from_data(vec!["abc  "])],
        expect: Series::from_data(vec!["abc"]),
        error: "",
    }];

    test_scalar_functions(RTrimFunction::try_create("rtrim")?, &tests)
}

#[test]
fn test_trim_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "trim-abc-passed",
            columns: vec![Series::from_data(vec!["   abc  "])],
            expect: Series::from_data(vec!["abc"]),
            error: "",
        },
        ScalarFunctionTest {
            name: "trim-blank-passed",
            columns: vec![Series::from_data(vec!["     "])],
            expect: Series::from_data(vec![""]),
            error: "",
        },
    ];

    test_scalar_functions(TrimFunction::try_create("trim")?, &tests)
}

#[test]
fn test_trim_nullable() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "trim-nullable-passed",
        columns: vec![Series::from_data(vec![Option::<Vec<u8>>::None])],
        expect: Series::from_data(vec![Option::<Vec<u8>>::None]),
        error: "",
    }];

    test_scalar_functions(TrimFunction::try_create("trim")?, &tests)
}
