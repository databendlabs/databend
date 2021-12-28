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
fn test_eq_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "eq-passed",
        nullable: false,
        columns: vec![
            Series::new(vec![4i64, 3, 2, 4]).into(),
            Series::new(vec![1i64, 2, 3, 4]).into(),
        ],
        expect: Series::new(vec![false, false, false, true]).into(),
        error: "",
    }];

    test_scalar_functions(ComparisonEqFunction::try_create_func("")?, &tests)
}

#[test]
fn test_gt_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "gt-passed",
        nullable: false,
        columns: vec![
            Series::new(vec![4i64, 3, 2, 4]).into(),
            Series::new(vec![1i64, 2, 3, 4]).into(),
        ],
        expect: Series::new(vec![true, true, false, false]).into(),
        error: "",
    }];

    test_scalar_functions(ComparisonGtFunction::try_create_func("")?, &tests)
}

#[test]
fn test_gt_eq_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "gt-eq-passed",
        nullable: false,
        columns: vec![
            Series::new(vec![4i64, 3, 2, 4]).into(),
            Series::new(vec![1i64, 2, 3, 4]).into(),
        ],
        expect: Series::new(vec![true, true, false, true]).into(),
        error: "",
    }];

    test_scalar_functions(ComparisonGtEqFunction::try_create_func("")?, &tests)
}

#[test]
fn test_lt_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "lt-passed",
        nullable: false,
        columns: vec![
            Series::new(vec![4i64, 3, 2, 4]).into(),
            Series::new(vec![1i64, 2, 3, 4]).into(),
        ],
        expect: Series::new(vec![false, false, true, false]).into(),
        error: "",
    }];

    test_scalar_functions(ComparisonLtFunction::try_create_func("")?, &tests)
}

#[test]
fn test_lt_eq_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "lt-eq-passed",
        nullable: false,
        columns: vec![
            Series::new(vec![4i64, 3, 2, 4]).into(),
            Series::new(vec![1i64, 2, 3, 4]).into(),
        ],
        expect: Series::new(vec![false, false, true, true]).into(),
        error: "",
    }];

    test_scalar_functions(ComparisonLtEqFunction::try_create_func("")?, &tests)
}

#[test]
fn test_not_eq_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "not-eq-passed",
        nullable: false,
        columns: vec![
            Series::new(vec![4i64, 3, 2, 4]).into(),
            Series::new(vec![1i64, 2, 3, 4]).into(),
        ],
        expect: Series::new(vec![true, true, true, false]).into(),
        error: "",
    }];

    test_scalar_functions(ComparisonNotEqFunction::try_create_func("")?, &tests)
}

#[test]
fn test_like_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "like-passed",
        nullable: false,
        columns: vec![
            Series::new(vec!["abc", "abd", "abe", "abf"]).into(),
            Series::new(vec!["a%", "_b_", "abe", "a"]).into(),
        ],
        expect: Series::new(vec![true, true, true, false]).into(),
        error: "",
    }];

    test_scalar_functions(ComparisonLikeFunction::try_create_func("")?, &tests)
}

#[test]
fn test_not_like_comparison_function() -> Result<()> {
    let tests = vec![ScalarFunctionTest {
        name: "not-like-passed",
        nullable: false,
        columns: vec![
            Series::new(vec!["abc", "abd", "abe", "abf"]).into(),
            Series::new(vec!["a%", "_b_", "abe", "a"]).into(),
        ],
        expect: Series::new(vec![false, false, false, true]).into(),
        error: "",
    }];

    test_scalar_functions(ComparisonNotLikeFunction::try_create_func("")?, &tests)
}
