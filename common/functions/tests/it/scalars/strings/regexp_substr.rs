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
use common_functions::scalars::RegexpSubStrFunction;

use crate::scalars::scalar_function2_test::test_scalar_functions;
use crate::scalars::scalar_function2_test::ScalarFunctionTest;

#[test]
fn test_regexp_instr_function() -> Result<()> {
    let types = vec![
        vec![StringType::arc(), StringType::arc()],
        vec![StringType::arc(), StringType::arc(), Int64Type::arc()],
        vec![
            StringType::arc(),
            StringType::arc(),
            Int64Type::arc(),
            Int64Type::arc(),
        ],
        vec![
            StringType::arc(),
            StringType::arc(),
            Int64Type::arc(),
            Int64Type::arc(),
            StringType::arc(),
        ],
        vec![
            StringType::arc(),
            StringType::arc(),
            Int64Type::arc(),
            Int64Type::arc(),
        ],
        vec![
            StringType::arc(),
            StringType::arc(),
            Int64Type::arc(),
            Int64Type::arc(),
            StringType::arc(),
        ],
    ];
    let tests = vec![
        ScalarFunctionTest {
            name: "regexp-substr-two-column-passed",
            columns: vec![
                Series::from_data(vec!["abc def ghi", "abc def ghi", ""]),
                Series::from_data(vec!["[a-z]+", "xxx", ""]),
            ],
            expect: Series::from_data(vec![Some("abc"), None, None]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-substr-three-column-passed",
            columns: vec![
                Series::from_data(vec!["abc def ghi", "abc def ghi", "abc def ghi"]),
                Series::from_data(vec!["[a-z]+", "[a-z]+", "[a-z]+"]),
                Series::from_data(vec![1_i64, 4, 12]),
            ],
            expect: Series::from_data(vec![Some("abc"), Some("def"), None]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-substr-four-column-passed",
            columns: vec![
                Series::from_data(vec!["abc def ghi", "abc def ghi"]),
                Series::from_data(vec!["[a-z]+", "[a-z]+"]),
                Series::from_data(vec![1_i64, 4]),
                Series::from_data(vec![3_i64, 2]),
            ],
            expect: Series::from_data(vec![Some("ghi"), Some("ghi")]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-substr-five-column-passed",
            columns: vec![
                Series::from_data(vec!["ABC def ghi", "abc def GHI"]),
                Series::from_data(vec!["[a-z]+", "[a-z]+"]),
                Series::from_data(vec![1_i64, 4]),
                Series::from_data(vec![3_i64, 2]),
                Series::from_data(vec!["c", "i"]),
            ],
            expect: Series::from_data(vec![None, Some("GHI")]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-substr-multi-byte-character-passed",
            columns: vec![
                Series::from_data(vec![
                    "周 周周 周周周 周周周周",
                    "周 周周 周周周 周周周周",
                    "周 周周 周周周 周周周周",
                ]),
                Series::from_data(vec!["周+", "周+", "周+"]),
                Series::from_data(vec![1_i64, 2, 14]),
                Series::from_data(vec![1_i64, 2, 1]),
            ],
            expect: Series::from_data(vec![Some("周"), Some("周周周"), None]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-substr-match-type-error",
            columns: vec![
                Series::from_data(vec!["ABC def ghi", "abc def GHI"]),
                Series::from_data(vec!["[a-z]+", "[a-z]+"]),
                Series::from_data(vec![1_i64, 4]),
                Series::from_data(vec![3_i64, 2]),
                Series::from_data(vec!["c", "-i"]),
            ],
            expect: Series::from_data(Vec::<u64>::new()),
            error: "Incorrect arguments to regexp_substr match type: -i",
        },
    ];

    for (typ, test) in types.iter().zip(tests) {
        test_scalar_functions(
            RegexpSubStrFunction::try_create("regexp_substr", &typ.iter().collect::<Vec<_>>())?,
            &[test],
            true,
        )?;
    }

    Ok(())
}

#[test]
fn test_regexp_substr_constant_column() -> Result<()> {
    let data_type = DataValue::String("[a-z]+".as_bytes().into());
    let mt_type = DataValue::String("-i".as_bytes().into());
    let data_value1 = StringType::arc().create_constant_column(&data_type, 2)?;
    let data_value2 = StringType::arc().create_constant_column(&data_type, 2)?;
    let mt_value = StringType::arc().create_constant_column(&mt_type, 2)?;

    let tests = vec![
        ScalarFunctionTest {
            name: "regexp-substr-const-column-passed",
            columns: vec![
                Series::from_data(vec!["abc def ghi", "abc def ghi"]),
                data_value1,
                Series::from_data(vec![1_i64, 4]),
                Series::from_data(vec![3_i64, 2]),
            ],
            expect: Series::from_data(vec![Some("ghi"), Some("ghi")]),
            error: "",
        },
        ScalarFunctionTest {
            name: "regexp-substr-const-column-match-type-error",
            columns: vec![
                Series::from_data(vec!["ABC def ghi", "abc def GHI"]),
                data_value2,
                Series::from_data(vec![1_i64, 4]),
                Series::from_data(vec![3_i64, 2]),
                mt_value,
            ],
            expect: Series::from_data(Vec::<u64>::new()),
            error: "Incorrect arguments to regexp_substr match type: -",
        },
    ];

    test_scalar_functions(
        RegexpSubStrFunction::try_create("regexp_substr", &[
            &StringType::arc(),
            &StringType::arc(),
            &Int64Type::arc(),
            &Int64Type::arc(),
        ])?,
        &tests,
        true,
    )
}
