// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::RegexpLikeFunction;

use crate::scalars::scalar_function2_test::test_scalar_functions;
use crate::scalars::scalar_function2_test::ScalarFunctionTest;

#[test]
fn test_regexp_like_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "regexp-like-two-column-passed",
            columns: vec![
                Series::from_data(vec!["abc", "abd", "Abe", "new*\n*line", "fo\nfo", ""]),
                Series::from_data(vec!["^a", "Ab", "abe", "new\\*.\\*line", "^fo$", ""]),
            ],
            expect: Series::from_data(vec![true, true, true, false, false, true]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-like-three-column-passed",
            columns: vec![
                Series::from_data(vec!["abc", "abd", "Abe", "new*\n*line", "fo\nfo", ""]),
                Series::from_data(vec!["^a", "Ab", "abe", "new\\*.\\*line", "^fo$", ""]),
                Series::from_data(vec!["", "c", "i", "n", "m", "c"]),
            ],
            expect: Series::from_data(vec![true, false, true, true, true, true]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-like-type-error",
            columns: vec![
                Series::from_data(vec!["abc", "abd"]),
                Series::from_data(vec![2, 3]),
            ],
            expect: Series::from_data(Vec::<bool>::new()),
            error: "Expected a string type, but got Int32",
        },
        ScalarFunctionTest {
            name: "regexp-like-match-type-error",
            columns: vec![
                Series::from_data(vec!["abc"]),
                Series::from_data(vec!["abc"]),
                Series::from_data(vec!["x"]),
            ],
            expect: Series::from_data(Vec::<bool>::new()),
            error: "Incorrect arguments to regexp_like match type: x",
        },
        ScalarFunctionTest {
            name: "regexp-like-match-type-error2",
            columns: vec![
                Series::from_data(vec!["abc"]),
                Series::from_data(vec!["abc"]),
                Series::from_data(vec!["u"]),
            ],
            expect: Series::from_data(Vec::<bool>::new()),
            error: "Unsupported arguments to regexp_like match type: u",
        },
        ScalarFunctionTest {
            name: "regexp-like-nullable-passed",
            columns: vec![
                Series::from_data(vec![Some("abc"), Some("abc"), None, Some("abc")]),
                Series::from_data(vec![Some("abc"), None, None, Some("abc")]),
                Series::from_data(vec![Some(""), Some("i"), Some("i"), None]),
            ],
            expect: Series::from_data(vec![Some(true), None, None, None]),
            error: "",
        },
    ];

    test_scalar_functions(RegexpLikeFunction::try_create("regexp_like")?, &tests, true)
}

#[test]
fn test_regexp_like_match_type_joiner() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "regexp-like-match-type-joiner-error-1",
            columns: vec![
                Series::from_data(vec!["Abc-", "Abc-"]),
                Series::from_data(vec!["abc-", "abc"]),
                Series::from_data(vec!["i", "-i"]),
            ],
            expect: Series::from_data(Vec::<bool>::new()),
            error: "Incorrect arguments to regexp_like match type: -i",
        },
        ScalarFunctionTest {
            name: "regexp-like-match-type-joiner-error-2",
            columns: vec![
                Series::from_data(vec!["Abc--", "Abc--"]),
                Series::from_data(vec!["abc--", "abc-"]),
                Series::from_data(vec!["", "-"]),
            ],
            expect: Series::from_data(Vec::<bool>::new()),
            error: "Incorrect arguments to regexp_like match type: -",
        },
    ];

    test_scalar_functions(RegexpLikeFunction::try_create("regexp_like")?, &tests, true)
}
