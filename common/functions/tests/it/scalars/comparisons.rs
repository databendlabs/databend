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
use common_functions::scalars::*;

use super::scalar_function2_test::test_scalar_functions2;
use super::scalar_function2_test::ScalarFunction2Test;

#[test]
fn test_eq_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunction2Test {
        name: "eq-passed",
        columns: vec![
            Series::from_data(vec![4i64, 3, 2, 4]),
            Series::from_data(vec![1i64, 2, 3, 4]),
        ],
        expect: Series::from_data(vec![false, false, false, true]),
        error: "",
    }];

    test_scalar_functions2(ComparisonEqFunction::try_create_func("")?, &tests)
}

#[test]
fn test_gt_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunction2Test {
        name: "gt-passed",
        columns: vec![
            Series::from_data(vec![4i64, 3, 2, 4]),
            Series::from_data(vec![1i64, 2, 3, 4]),
        ],
        expect: Series::from_data(vec![true, true, false, false]),
        error: "",
    }];

    test_scalar_functions2(ComparisonGtFunction::try_create_func("")?, &tests)
}

#[test]
fn test_gt_eq_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunction2Test {
        name: "gt-eq-passed",
        columns: vec![
            Series::from_data(vec![4i64, 3, 2, 4]),
            Series::from_data(vec![1i64, 2, 3, 4]),
        ],
        expect: Series::from_data(vec![true, true, false, true]),
        error: "",
    }];

    test_scalar_functions2(ComparisonGtEqFunction::try_create_func("")?, &tests)
}

#[test]
fn test_lt_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunction2Test {
        name: "lt-passed",
        columns: vec![
            Series::from_data(vec![4i64, 3, 2, 4]),
            Series::from_data(vec![1i64, 2, 3, 4]),
        ],
        expect: Series::from_data(vec![false, false, true, false]),
        error: "",
    }];

    test_scalar_functions2(ComparisonLtFunction::try_create_func("")?, &tests)
}

#[test]
fn test_lt_eq_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunction2Test {
        name: "lt-eq-passed",
        columns: vec![
            Series::from_data(vec![4i64, 3, 2, 4]),
            Series::from_data(vec![1i64, 2, 3, 4]),
        ],
        expect: Series::from_data(vec![false, false, true, true]),
        error: "",
    }];

    test_scalar_functions2(ComparisonLtEqFunction::try_create_func("")?, &tests)
}

#[test]
fn test_not_eq_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunction2Test {
        name: "not-eq-passed",
        columns: vec![
            Series::from_data(vec![4i64, 3, 2, 4]),
            Series::from_data(vec![1i64, 2, 3, 4]),
        ],
        expect: Series::from_data(vec![true, true, true, false]),
        error: "",
    }];

    test_scalar_functions2(ComparisonNotEqFunction::try_create_func("")?, &tests)
}

#[test]
fn test_like_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunction2Test {
        name: "like-passed",
        columns: vec![
            Series::from_data(vec!["abc", "abd", "abe", "abf"]),
            Series::from_data(vec!["a%", "_b_", "abe", "a"]),
        ],
        expect: Series::from_data(vec![true, true, true, false]),
        error: "",
    }];

    test_scalar_functions2(ComparisonLikeFunction::try_create_like("")?, &tests)
}

#[test]
fn test_not_like_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunction2Test {
        name: "not-like-passed",
        columns: vec![
            Series::from_data(vec!["abc", "abd", "abe", "abf"]),
            Series::from_data(vec!["a%", "_b_", "abe", "a"]),
        ],
        expect: Series::from_data(vec![false, false, false, true]),
        error: "",
    }];

    test_scalar_functions2(ComparisonLikeFunction::try_create_nlike("")?, &tests)
}
