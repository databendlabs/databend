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
fn test_regexp_instr_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "regexp-instr-two-column-passed",
            columns: vec![
                Series::from_data(vec!["dog cat dog", "aa aaa aaaa aa aaa aaaa", ""]),
                Series::from_data(vec!["dog", "a{2}", ""]),
            ],
            expect: Series::from_data(vec![Some(1_u64), Some(1), Some(0)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-instr-three-column-passed",
            columns: vec![
                Series::from_data(vec!["dog cat dog", "aa aaa aaaa aa aaa aaaa", ""]),
                Series::from_data(vec!["dog", "a{2}", ""]),
                Series::from_data(vec![1_i64, 2, 1]),
            ],
            expect: Series::from_data(vec![Some(1_u64), Some(4), Some(0)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-instr-four-column-passed",
            columns: vec![
                Series::from_data(vec![
                    "dog cat dog",
                    "aa aa aa aaaa aaaa aaaa",
                    "aa aa aa aaaa aaaa aaaa",
                ]),
                Series::from_data(vec!["dog", "a{2}", "a{4}"]),
                Series::from_data(vec![1_i64, 1, 9]),
                Series::from_data(vec![2_i64, 3, 2]),
            ],
            expect: Series::from_data(vec![Some(9_u64), Some(7), Some(15)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-instr-five-column-passed",
            columns: vec![
                Series::from_data(vec![
                    "dog cat dog",
                    "aa aaa aaaa aa aaa aaaa",
                    "aa aaa aaaa aa aaa aaaa",
                ]),
                Series::from_data(vec!["dog", "a{2}", "a{4}"]),
                Series::from_data(vec![1_i64, 2, 1]),
                Series::from_data(vec![2_i64, 2, 2]),
                Series::from_data(vec![0_i64, 1, 1]),
            ],
            expect: Series::from_data(vec![Some(9_u64), Some(10), Some(24)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-instr-six-column-passed",
            columns: vec![
                Series::from_data(vec![
                    "dog cat dog",
                    "aa aaa aaaa aa aaa aaaa",
                    "aa aaa aaaa aa aaa aaaa",
                ]),
                Series::from_data(vec!["dog", "A{2}", "A{4}"]),
                Series::from_data(vec![1_i64, 2, 1]),
                Series::from_data(vec![2_i64, 2, 2]),
                Series::from_data(vec![0_i64, 1, 1]),
                Series::from_data(vec!["i", "c", "i"]),
            ],
            expect: Series::from_data(vec![Some(9_u64), Some(0), Some(24)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-instr-multi-byte-character-passed",
            columns: vec![
                Series::from_data(vec![
                    "周 周周 周周周 周周周周",
                    "周 周周 周周周 周周周周",
                    "周 周周 周周周 周周周周",
                    "周 周周 周周周 周周周周",
                ]),
                Series::from_data(vec!["周+", "周+", "周+", "周+"]),
                Series::from_data(vec![1_i64, 2, 3, 5]),
                Series::from_data(vec![1_i64, 2, 3, 1]),
                Series::from_data(vec![0_i64, 1, 1, 1]),
            ],
            expect: Series::from_data(vec![Some(1_u64), Some(9), Some(14), Some(9)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-instr-position-error",
            columns: vec![
                Series::from_data(vec!["dog cat dog"]),
                Series::from_data(vec!["dog"]),
                Series::from_data(vec![0_i64]),
                Series::from_data(vec![1_i64]),
                Series::from_data(vec![0_i64]),
            ],
            expect: Series::from_data(Vec::<u64>::new()),
            error: "Incorrect arguments to regexp_instr: position must be positive, but got 0",
        },
        ScalarFunctionTest {
            name: "regexp-instr-return-option-error",
            columns: vec![
                Series::from_data(vec![
                    "dog cat dog",
                    "aa aaa aaaa aa aaa aaaa",
                    "aa aaa aaaa aa aaa aaaa",
                ]),
                Series::from_data(vec!["dog", "A{2}", "A{4}"]),
                Series::from_data(vec![2_i64, 2, 2]),
                Series::from_data(vec![1_i64, 2, 1]),
                Series::from_data(vec![0_i64, 2, 1]),
            ],
            expect: Series::from_data(Vec::<u64>::new()),
            error: "Incorrect arguments to regexp_instr: return_option must be 1 or 0, but got 2",
        },
        ScalarFunctionTest {
            name: "regexp-instr-match-type-error",
            columns: vec![
                Series::from_data(vec![
                    "dog cat dog",
                    "aa aaa aaaa aa aaa aaaa",
                    "aa aaa aaaa aa aaa aaaa",
                ]),
                Series::from_data(vec!["dog", "A{2}", "A{4}"]),
                Series::from_data(vec![1_i64, 2, 1]),
                Series::from_data(vec![2_i64, 2, 2]),
                Series::from_data(vec![0_i64, 1, 1]),
                Series::from_data(vec!["i", "c", "-i"]),
            ],
            expect: Series::from_data(Vec::<u64>::new()),
            error: "Incorrect arguments to regexp_instr match type: -i",
        },
    ];

    test_scalar_functions("regexp_instr", &tests)
}

#[test]
fn test_regexp_instr_constant_column() -> Result<()> {
    let data = DataValue::String("dog".as_bytes().into());
    let data_value1 = StringType::arc().create_constant_column(&data, 3)?;
    let data_value2 = StringType::arc().create_constant_column(&data, 3)?;

    let tests = vec![
        ScalarFunctionTest {
            name: "regexp-instr-const-column-passed",
            columns: vec![
                Series::from_data(vec!["dog cat dog", "cat dog cat", "cat dog cat"]),
                data_value1,
                Series::from_data(vec![1_i64, 2, 1]),
                Series::from_data(vec![2_i64, 1, 1]),
                Series::from_data(vec![0_i64, 0, 1]),
            ],
            expect: Series::from_data(vec![Some(9_u64), Some(5), Some(8)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-instr-const-column-return-option-error",
            columns: vec![
                Series::from_data(vec!["dog cat dog", "cat dog cat", "cat dog cat"]),
                data_value2,
                Series::from_data(vec![1_i64, 2, 1]),
                Series::from_data(vec![2_i64, 1, 1]),
                Series::from_data(vec![2_i64, 0, 1]),
            ],
            expect: Series::from_data(Vec::<u64>::new()),
            error: "Incorrect arguments to regexp_instr: return_option must be 1 or 0, but got 2",
        },
    ];

    test_scalar_functions("regexp_instr", &tests)
}
