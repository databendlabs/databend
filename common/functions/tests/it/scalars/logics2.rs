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

use common_datavalues2::prelude::*;
use common_exception::Result;
use common_functions::scalars::LogicAndFunction2;
use common_functions::scalars::LogicOrFunction2;
use common_functions::scalars::LogicNotFunction2;
use common_functions::scalars::LogicXorFunction2;

use crate::scalars::scalar_function2_test::test_scalar_functions2;
use crate::scalars::scalar_function2_test::ScalarFunction2Test;

#[test]
fn test_logic_not_function() -> Result<()> {
    let tests = vec![
        ScalarFunction2Test {
        name: "not-passed",
        columns: vec![Series::from_data(vec![true, false]).into()],
        expect: Series::from_data(vec![false, true]).into(),
        error: "",
    }];
    test_scalar_functions2(LogicNotFunction2::try_create("not")?, &tests)
}

#[test]
fn test_logic_and_function() -> Result<()> {
    let tests = vec![
        ScalarFunction2Test {
        name: "and-passed",
        columns: vec![
            Series::from_data(vec![true, true, true, false]),
            Series::from_data(vec![true, false, true, true]),
        ],
        expect: Series::from_data(vec![true, false, true, false]),
        error: "",
    }];
    test_scalar_functions2(LogicAndFunction2::try_create("and")?, &tests)
}

#[test]
fn test_logic_or_function() -> Result<()> {
    let tests = vec![
        ScalarFunction2Test {
        name: "or-passed",
        columns: vec![
            Series::from_data(vec![true, true, true, false]),
            Series::from_data(vec![true, false, true, false]),
        ],
        expect: Series::from_data(vec![true, true, true, false]),
        error: "",
    }];
    test_scalar_functions2(LogicOrFunction2::try_create("or")?, &tests)
}

#[test]
fn test_logic_xor_function() -> Result<()> {
    let tests = vec![
        ScalarFunction2Test {
        name: "xor-passed",
        columns: vec![
            Series::from_data(vec![true, true, false, false]),
            Series::from_data(vec![true, false, true, false]),
        ],
        expect: Series::from_data(vec![false, true, true, false]),
        error: "",
    }];
    test_scalar_functions2(LogicXorFunction2::try_create("or")?, &tests)
}

