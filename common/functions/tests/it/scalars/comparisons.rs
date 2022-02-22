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

use super::scalar_function2_test::test_scalar_functions;
use super::scalar_function2_test::ScalarFunctionTest;

#[test]
fn test_eq_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "eq-passed",
        columns: vec![
            Series::from_data(vec![4i64, 3, 2, 4]),
            Series::from_data(vec![1i64, 2, 3, 4]),
        ],
        expect: Series::from_data(vec![false, false, false, true]),
        error: "",
    }];

    test_scalar_functions(ComparisonEqFunction::try_create_func("")?, &tests)
}

#[test]
fn test_gt_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "gt-passed",
        columns: vec![
            Series::from_data(vec![4i64, 3, 2, 4]),
            Series::from_data(vec![1i64, 2, 3, 4]),
        ],
        expect: Series::from_data(vec![true, true, false, false]),
        error: "",
    }];

    test_scalar_functions(ComparisonGtFunction::try_create_func("")?, &tests)
}

#[test]
fn test_gt_eq_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "gt-eq-passed",
        columns: vec![
            Series::from_data(vec![4i64, 3, 2, 4]),
            Series::from_data(vec![1i64, 2, 3, 4]),
        ],
        expect: Series::from_data(vec![true, true, false, true]),
        error: "",
    }];

    test_scalar_functions(ComparisonGtEqFunction::try_create_func("")?, &tests)
}

#[test]
fn test_lt_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "lt-passed",
        columns: vec![
            Series::from_data(vec![4i64, 3, 2, 4]),
            Series::from_data(vec![1i64, 2, 3, 4]),
        ],
        expect: Series::from_data(vec![false, false, true, false]),
        error: "",
    }];

    test_scalar_functions(ComparisonLtFunction::try_create_func("")?, &tests)
}

#[test]
fn test_lt_eq_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "lt-eq-passed",
        columns: vec![
            Series::from_data(vec![4i64, 3, 2, 4]),
            Series::from_data(vec![1i64, 2, 3, 4]),
        ],
        expect: Series::from_data(vec![false, false, true, true]),
        error: "",
    }];

    test_scalar_functions(ComparisonLtEqFunction::try_create_func("")?, &tests)
}

#[test]
fn test_not_eq_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "not-eq-passed",
        columns: vec![
            Series::from_data(vec![4i64, 3, 2, 4]),
            Series::from_data(vec![1i64, 2, 3, 4]),
        ],
        expect: Series::from_data(vec![true, true, true, false]),
        error: "",
    }];

    test_scalar_functions(ComparisonNotEqFunction::try_create_func("")?, &tests)
}

#[test]
fn test_like_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "like-passed",
        columns: vec![
            Series::from_data(vec!["abc", "abd", "abe", "abf"]),
            Series::from_data(vec!["a%", "_b_", "abe", "a"]),
        ],
        expect: Series::from_data(vec![true, true, true, false]),
        error: "",
    }];

    test_scalar_functions(ComparisonLikeFunction::try_create_like("")?, &tests)
}

#[test]
fn test_not_like_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "not-like-passed",
        columns: vec![
            Series::from_data(vec!["abc", "abd", "abe", "abf"]),
            Series::from_data(vec!["a%", "_b_", "abe", "a"]),
        ],
        expect: Series::from_data(vec![false, false, false, true]),
        error: "",
    }];

    test_scalar_functions(ComparisonLikeFunction::try_create_nlike("")?, &tests)
}

#[test]
fn test_regexp_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "regexp-passed",
        columns: vec![
            Series::from_data(vec!["abc", "abd", "abe", "abf", "abc", ""]),
            Series::from_data(vec!["^a", "^b", "abe", "a", "", ""]),
        ],
        expect: Series::from_data(vec![true, false, true, true, false, true]),
        error: "",
    }];

    test_scalar_functions(ComparisonRegexpFunction::try_create_regexp("")?, &tests)
}

#[test]
fn test_not_regexp_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "regexp-passed",
        columns: vec![
            Series::from_data(vec!["abc", "abd", "abe", "abf", "abc", ""]),
            Series::from_data(vec!["^a", "^b", "abe", "a", "", ""]),
        ],
        expect: Series::from_data(vec![false, true, false, false, true, false]),
        error: "",
    }];

    test_scalar_functions(ComparisonRegexpFunction::try_create_nregexp("")?, &tests)
}
