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
use serde_json::json;

use super::scalar_function_test::test_scalar_functions;
use super::scalar_function_test::ScalarFunctionTest;

#[test]
fn test_eq_comparison_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "eq-passed",
            columns: vec![
                Series::from_data(vec![4i64, 3, 2, 4]),
                Series::from_data(vec![1i64, 2, 3, 4]),
            ],
            expect: Series::from_data(vec![false, false, false, true]),
            error: "",
        },
        ScalarFunctionTest {
            name: "variant-int-eq-passed",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!(4i64)),
                    VariantValue::from(json!(3i64)),
                    VariantValue::from(json!(2i64)),
                    VariantValue::from(json!(4i64)),
                ]),
                Series::from_data(vec![1i64, 2, 3, 4]),
            ],
            expect: Series::from_data(vec![false, false, false, true]),
            error: "",
        },
        ScalarFunctionTest {
            name: "variant-string-eq-passed",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!("dd")),
                    VariantValue::from(json!("cc")),
                    VariantValue::from(json!("bb")),
                    VariantValue::from(json!("dd")),
                ]),
                Series::from_data(vec!["aa", "bb", "cc", "dd"]),
            ],
            expect: Series::from_data(vec![false, false, false, true]),
            error: "",
        },
    ];

    test_scalar_functions("=", &tests)
}

#[test]
fn test_gt_comparison_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "gt-passed",
            columns: vec![
                Series::from_data(vec![4i64, 3, 2, 4]),
                Series::from_data(vec![1i64, 2, 3, 4]),
            ],
            expect: Series::from_data(vec![true, true, false, false]),
            error: "",
        },
        ScalarFunctionTest {
            name: "variant-int-gt-passed",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!(4i64)),
                    VariantValue::from(json!(3i64)),
                    VariantValue::from(json!(2i64)),
                    VariantValue::from(json!(4i64)),
                ]),
                Series::from_data(vec![1i64, 2, 3, 4]),
            ],
            expect: Series::from_data(vec![true, true, false, false]),
            error: "",
        },
        ScalarFunctionTest {
            name: "variant-string-gt-passed",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!("dd")),
                    VariantValue::from(json!("cc")),
                    VariantValue::from(json!("bb")),
                    VariantValue::from(json!("dd")),
                ]),
                Series::from_data(vec!["aa", "bb", "cc", "dd"]),
            ],
            expect: Series::from_data(vec![true, true, false, false]),
            error: "",
        },
    ];

    test_scalar_functions(">", &tests)
}

#[test]
fn test_gt_eq_comparison_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "gt-eq-passed",
            columns: vec![
                Series::from_data(vec![4i64, 3, 2, 4]),
                Series::from_data(vec![1i64, 2, 3, 4]),
            ],
            expect: Series::from_data(vec![true, true, false, true]),
            error: "",
        },
        ScalarFunctionTest {
            name: "variant-int-gt-eq-passed",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!(4i64)),
                    VariantValue::from(json!(3i64)),
                    VariantValue::from(json!(2i64)),
                    VariantValue::from(json!(4i64)),
                ]),
                Series::from_data(vec![1i64, 2, 3, 4]),
            ],
            expect: Series::from_data(vec![true, true, false, true]),
            error: "",
        },
        ScalarFunctionTest {
            name: "variant-string-gt-eq-passed",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!("dd")),
                    VariantValue::from(json!("cc")),
                    VariantValue::from(json!("bb")),
                    VariantValue::from(json!("dd")),
                ]),
                Series::from_data(vec!["aa", "bb", "cc", "dd"]),
            ],
            expect: Series::from_data(vec![true, true, false, true]),
            error: "",
        },
    ];

    test_scalar_functions(">=", &tests)
}

#[test]
fn test_lt_comparison_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "lt-passed",
            columns: vec![
                Series::from_data(vec![4i64, 3, 2, 4]),
                Series::from_data(vec![1i64, 2, 3, 4]),
            ],
            expect: Series::from_data(vec![false, false, true, false]),
            error: "",
        },
        ScalarFunctionTest {
            name: "variant-int-lt-passed",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!(4i64)),
                    VariantValue::from(json!(3i64)),
                    VariantValue::from(json!(2i64)),
                    VariantValue::from(json!(4i64)),
                ]),
                Series::from_data(vec![1i64, 2, 3, 4]),
            ],
            expect: Series::from_data(vec![false, false, true, false]),
            error: "",
        },
        ScalarFunctionTest {
            name: "variant-string-lt-passed",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!("dd")),
                    VariantValue::from(json!("cc")),
                    VariantValue::from(json!("bb")),
                    VariantValue::from(json!("dd")),
                ]),
                Series::from_data(vec!["aa", "bb", "cc", "dd"]),
            ],
            expect: Series::from_data(vec![false, false, true, false]),
            error: "",
        },
    ];

    test_scalar_functions("<", &tests)
}

#[test]
fn test_lt_eq_comparison_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "lt-eq-passed",
            columns: vec![
                Series::from_data(vec![4i64, 3, 2, 4]),
                Series::from_data(vec![1i64, 2, 3, 4]),
            ],
            expect: Series::from_data(vec![false, false, true, true]),
            error: "",
        },
        ScalarFunctionTest {
            name: "variant-int-lt-eq-passed",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!(4i64)),
                    VariantValue::from(json!(3i64)),
                    VariantValue::from(json!(2i64)),
                    VariantValue::from(json!(4i64)),
                ]),
                Series::from_data(vec![1i64, 2, 3, 4]),
            ],
            expect: Series::from_data(vec![false, false, true, true]),
            error: "",
        },
        ScalarFunctionTest {
            name: "variant-string-lt-eq-passed",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!("dd")),
                    VariantValue::from(json!("cc")),
                    VariantValue::from(json!("bb")),
                    VariantValue::from(json!("dd")),
                ]),
                Series::from_data(vec!["aa", "bb", "cc", "dd"]),
            ],
            expect: Series::from_data(vec![false, false, true, true]),
            error: "",
        },
    ];

    test_scalar_functions("<=", &tests)
}

#[test]
fn test_not_eq_comparison_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "not-eq-passed",
            columns: vec![
                Series::from_data(vec![4i64, 3, 2, 4]),
                Series::from_data(vec![1i64, 2, 3, 4]),
            ],
            expect: Series::from_data(vec![true, true, true, false]),
            error: "",
        },
        ScalarFunctionTest {
            name: "variant-int-not-eq-passed",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!(4i64)),
                    VariantValue::from(json!(3i64)),
                    VariantValue::from(json!(2i64)),
                    VariantValue::from(json!(4i64)),
                ]),
                Series::from_data(vec![1i64, 2, 3, 4]),
            ],
            expect: Series::from_data(vec![true, true, true, false]),
            error: "",
        },
        ScalarFunctionTest {
            name: "variant-string-not-eq-passed",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!("dd")),
                    VariantValue::from(json!("cc")),
                    VariantValue::from(json!("bb")),
                    VariantValue::from(json!("dd")),
                ]),
                Series::from_data(vec!["aa", "bb", "cc", "dd"]),
            ],
            expect: Series::from_data(vec![true, true, true, false]),
            error: "",
        },
    ];

    test_scalar_functions("<>", &tests)
}

#[test]
fn test_like_comparison_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "like-passed",
            columns: vec![
                Series::from_data(vec!["abc", "abd", "abe", "abf"]),
                Series::from_data(vec!["a%", "_b_", "abe", "a"]),
            ],
            expect: Series::from_data(vec![true, true, true, false]),
            error: "",
        },
        ScalarFunctionTest {
            name: "variant-like-passed",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!("abc")),
                    VariantValue::from(json!("abd")),
                    VariantValue::from(json!("abe")),
                    VariantValue::from(json!("abf")),
                ]),
                Series::from_data(vec!["a%", "_b_", "abe", "a"]),
            ],
            expect: Series::from_data(vec![true, true, true, false]),
            error: "",
        },
    ];

    test_scalar_functions("like", &tests)
}

#[test]
fn test_not_like_comparison_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "not-like-passed",
            columns: vec![
                Series::from_data(vec!["abc", "abd", "abe", "abf"]),
                Series::from_data(vec!["a%", "_b_", "abe", "a"]),
            ],
            expect: Series::from_data(vec![false, false, false, true]),
            error: "",
        },
        ScalarFunctionTest {
            name: "variant-not-like-passed",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!("abc")),
                    VariantValue::from(json!("abd")),
                    VariantValue::from(json!("abe")),
                    VariantValue::from(json!("abf")),
                ]),
                Series::from_data(vec!["a%", "_b_", "abe", "a"]),
            ],
            expect: Series::from_data(vec![false, false, false, true]),
            error: "",
        },
    ];

    test_scalar_functions("not like", &tests)
}

#[test]
fn test_regexp_comparison_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "regexp-passed",
            columns: vec![
                Series::from_data(vec!["abc", "abd", "abe", "abf", "abc", ""]),
                Series::from_data(vec!["^a", "^b", "abe", "a", "", ""]),
            ],
            expect: Series::from_data(vec![true, false, true, true, false, true]),
            error: "",
        },
        ScalarFunctionTest {
            name: "variant-regexp-passed",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!("abc")),
                    VariantValue::from(json!("abd")),
                    VariantValue::from(json!("abe")),
                    VariantValue::from(json!("abf")),
                    VariantValue::from(json!("abc")),
                    VariantValue::from(json!("")),
                ]),
                Series::from_data(vec!["^a", "^b", "abe", "a", "", ""]),
            ],
            expect: Series::from_data(vec![true, false, true, true, false, true]),
            error: "",
        },
    ];

    test_scalar_functions("regexp", &tests)
}

#[test]
fn test_not_regexp_comparison_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "not-regexp-passed",
            columns: vec![
                Series::from_data(vec!["abc", "abd", "abe", "abf", "abc", ""]),
                Series::from_data(vec!["^a", "^b", "abe", "a", "", ""]),
            ],
            expect: Series::from_data(vec![false, true, false, false, true, false]),
            error: "",
        },
        ScalarFunctionTest {
            name: "variant-not-regexp-passed",
            columns: vec![
                Series::from_data(vec![
                    VariantValue::from(json!("abc")),
                    VariantValue::from(json!("abd")),
                    VariantValue::from(json!("abe")),
                    VariantValue::from(json!("abf")),
                    VariantValue::from(json!("abc")),
                    VariantValue::from(json!("")),
                ]),
                Series::from_data(vec!["^a", "^b", "abe", "a", "", ""]),
            ],
            expect: Series::from_data(vec![false, true, false, false, true, false]),
            error: "",
        },
    ];

    test_scalar_functions("not regexp", &tests)
}
