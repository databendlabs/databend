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

use crate::scalars::scalar_function_test::test_scalar_functions;
use crate::scalars::scalar_function_test::ScalarFunctionTest;

#[test]
fn test_regexp_replace_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "regexp-replace-three-column-passed",
            columns: vec![
                Series::from_data(vec!["a b c", "a b c", "a b c", ""]),
                Series::from_data(vec!["b", "x", "", "b"]),
                Series::from_data(vec!["X", "X", "X", "X"]),
            ],
            expect: Series::from_data(vec![Some("a X c"), Some("a b c"), Some("a b c"), Some("")]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-replace-four-column-passed",
            columns: vec![
                Series::from_data(vec![
                    "abc def ghi",
                    "abc def ghi",
                    "abc def ghi",
                    "abc def ghi",
                ]),
                Series::from_data(vec!["[a-z]+", "[a-z]+", "[a-z]+", "[a-z]+"]),
                Series::from_data(vec!["X", "X", "X", "X"]),
                Series::from_data(vec![1, 4, 8, 12]),
            ],
            expect: Series::from_data(vec![
                Some("X X X"),
                Some("abc X X"),
                Some("abc def X"),
                Some("abc def ghi"),
            ]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-replace-five-column-passed",
            columns: vec![
                Series::from_data(vec![
                    "abc def ghi",
                    "abc def ghi",
                    "abc def ghi",
                    "abc def ghi",
                ]),
                Series::from_data(vec!["[a-z]+", "[a-z]+", "[a-z]+", "[a-z]+"]),
                Series::from_data(vec!["X", "X", "X", "X"]),
                Series::from_data(vec![1, 1, 4, 4]),
                Series::from_data(vec![0, 1, 2, 3]),
            ],
            expect: Series::from_data(vec![
                Some("X X X"),
                Some("X def ghi"),
                Some("abc def X"),
                Some("abc def ghi"),
            ]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-replace-six-column-passed",
            columns: vec![
                Series::from_data(vec!["abc def ghi", "abc DEF ghi", "abc DEF ghi"]),
                Series::from_data(vec!["[a-z]+", "[a-z]+", "[a-z]+"]),
                Series::from_data(vec!["X", "X", "X"]),
                Series::from_data(vec![1, 1, 4]),
                Series::from_data(vec![0, 2, 1]),
                Series::from_data(vec!["", "c", "i"]),
            ],
            expect: Series::from_data(vec![Some("X X X"), Some("abc DEF X"), Some("abc X ghi")]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-replace-multi-byte-character-passed",
            columns: vec![
                Series::from_data(vec![
                    "周 周周 周周周 周周周周",
                    "周 周周 周周周 周周周周",
                    "周 周周 周周周 周周周周",
                    "周 周周 周周周 周周周周",
                ]),
                Series::from_data(vec!["周+", "周+", "周+", "周+"]),
                Series::from_data(vec!["唐", "唐", "唐", "唐"]),
                Series::from_data(vec![1, 2, 3, 5]),
                Series::from_data(vec![0, 1, 3, 1]),
            ],
            expect: Series::from_data(vec![
                Some("唐 唐 唐 唐"),
                Some("周 唐 周周周 周周周周"),
                Some("周 周周 周周周 唐"),
                Some("周 周周 唐 周周周周"),
            ]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-replace-position-error",
            columns: vec![
                Series::from_data(vec!["a b c"]),
                Series::from_data(vec!["b"]),
                Series::from_data(vec!["X"]),
                Series::from_data(vec![0]),
            ],
            expect: Series::from_data(Vec::<&str>::new()),
            error: "Incorrect arguments to regexp_replace: position must be positive, but got 0",
        },
        ScalarFunctionTest {
            name: "regexp-replace-occurrence-error",
            columns: vec![
                Series::from_data(vec!["a b c"]),
                Series::from_data(vec!["b"]),
                Series::from_data(vec!["X"]),
                Series::from_data(vec![1]),
                Series::from_data(vec![-1]),
            ],
            expect: Series::from_data(Vec::<&str>::new()),
            error:
                "Incorrect arguments to regexp_replace: occurrence must not be negative, but got -1",
        },
        ScalarFunctionTest {
            name: "regexp-replace-match-type-error",
            columns: vec![
                Series::from_data(vec!["a b c"]),
                Series::from_data(vec!["b"]),
                Series::from_data(vec!["X"]),
                Series::from_data(vec![1]),
                Series::from_data(vec![0]),
                Series::from_data(vec!["-c"]),
            ],
            expect: Series::from_data(Vec::<&str>::new()),
            error: "Incorrect arguments to regexp_replace match type: -c",
        },
    ];

    test_scalar_functions("regexp_replace", &tests)
}

#[test]
fn test_regexp_replace_constant_column() -> Result<()> {
    let data_type = DataValue::String("[a-z]+".as_bytes().into());
    let data_value1 = StringType::new_impl().create_constant_column(&data_type, 3)?;
    let data_value2 = StringType::new_impl().create_constant_column(&data_type, 3)?;

    let tests = vec![
        ScalarFunctionTest {
            name: "regexp-repalce-const-column-passed",
            columns: vec![
                Series::from_data(vec!["abc def ghi", "abc def ghi", "abc def ghi"]),
                data_value1,
                Series::from_data(vec!["X", "X", "X"]),
                Series::from_data(vec![1, 1, 1]),
                Series::from_data(vec![0, 1, 2]),
            ],
            expect: Series::from_data(vec![Some("X X X"), Some("X def ghi"), Some("abc X ghi")]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-instr-const-column-position-error",
            columns: vec![
                Series::from_data(vec!["abc def ghi", "abc def ghi", "abc def ghi"]),
                data_value2,
                Series::from_data(vec!["X", "X", "X"]),
                Series::from_data(vec![1, 0, -1]),
            ],
            expect: Series::from_data(Vec::<&str>::new()),
            error: "Incorrect arguments to regexp_replace: position must be positive, but got 0",
        },
    ];

    test_scalar_functions("regexp_replace", &tests)
}
