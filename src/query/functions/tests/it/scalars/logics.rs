// Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::scalar_function_test::test_scalar_functions;
use crate::scalars::scalar_function_test::ScalarFunctionTest;

#[test]
fn test_logic_not_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "not",
            columns: vec![Series::from_data(vec![true, false])],
            expect: Series::from_data(vec![false, true]),
            error: "",
        },
        ScalarFunctionTest {
            name: "not-nullable",
            columns: vec![Series::from_data(vec![None, Some(true), Some(false)])],
            expect: Series::from_data(vec![None, Some(false), Some(true)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "not-null",
            columns: vec![Arc::new(NullColumn::new(4))],
            expect: Arc::new(NullColumn::new(4)),
            error: "",
        },
    ];
    test_scalar_functions("not", &tests)
}

#[test]
fn test_logic_and_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "and",
            columns: vec![
                Series::from_data(vec![true, true, false, false]),
                Series::from_data(vec![true, false, true, false]),
            ],
            expect: Series::from_data(vec![true, false, false, false]),
            error: "",
        },
        ScalarFunctionTest {
            name: "and-left-nullable",
            columns: vec![
                Series::from_data(vec![
                    Some(true),
                    Some(true),
                    Some(false),
                    Some(false),
                    None,
                    None,
                ]),
                Series::from_data(vec![true, false, true, false, true, false]),
            ],
            expect: Series::from_data(vec![
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                None,
                Some(false),
            ]),
            error: "",
        },
        ScalarFunctionTest {
            name: "and-right-nullable",
            columns: vec![
                Series::from_data(vec![true, false, true, false, true, false]),
                Series::from_data(vec![
                    Some(true),
                    Some(true),
                    Some(false),
                    Some(false),
                    None,
                    None,
                ]),
            ],
            expect: Series::from_data(vec![
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                None,
                Some(false),
            ]),
            error: "",
        },
        ScalarFunctionTest {
            name: "and-both-nullable",
            columns: vec![
                Series::from_data(vec![Some(true), Some(true), Some(false), Some(false), None]),
                Series::from_data(vec![Some(true), Some(false), Some(true), Some(false), None]),
            ],
            expect: Series::from_data(vec![
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                None,
            ]),
            error: "",
        },
    ];
    test_scalar_functions("and", &tests)
}

#[test]
fn test_logic_or_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "or",
            columns: vec![
                Series::from_data(vec![true, true, false, false]),
                Series::from_data(vec![true, false, true, false]),
            ],
            expect: Series::from_data(vec![true, true, true, false]),
            error: "",
        },
        ScalarFunctionTest {
            name: "or-left-nullable",
            columns: vec![
                Series::from_data(vec![
                    Some(true),
                    Some(true),
                    Some(false),
                    Some(false),
                    None,
                    None,
                ]),
                Series::from_data(vec![true, false, true, false, true, false]),
            ],
            expect: Series::from_data(vec![
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                None,
            ]),
            error: "",
        },
        ScalarFunctionTest {
            name: "or-right-nullable",
            columns: vec![
                Series::from_data(vec![true, false, true, false, true, false]),
                Series::from_data(vec![
                    Some(true),
                    Some(true),
                    Some(false),
                    Some(false),
                    None,
                    None,
                ]),
            ],
            expect: Series::from_data(vec![
                Some(true),
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                None,
            ]),
            error: "",
        },
        ScalarFunctionTest {
            name: "or-both-nullable",
            columns: vec![
                Series::from_data(vec![Some(true), Some(true), Some(false), Some(false), None]),
                Series::from_data(vec![Some(true), Some(false), Some(true), Some(false), None]),
            ],
            expect: Series::from_data(vec![Some(true), Some(true), Some(true), Some(false), None]),
            error: "",
        },
    ];
    test_scalar_functions("or", &tests)
}

#[test]
fn test_logic_xor_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "xor",
            columns: vec![
                Series::from_data(vec![true, true, false, false]),
                Series::from_data(vec![true, false, true, false]),
            ],
            expect: Series::from_data(vec![false, true, true, false]),
            error: "",
        },
        ScalarFunctionTest {
            name: "xor-null",
            columns: vec![
                Series::from_data(vec![None, Some(true), Some(false), Some(false)]),
                Series::from_data(vec![Some(true), None, Some(true), Some(false)]),
            ],
            expect: Series::from_data(vec![None, None, Some(true), Some(false)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "xor-null",
            columns: vec![
                Series::from_data(vec![None, Some(true), Some(false), Some(false)]),
                Arc::new(NullColumn::new(4)),
            ],
            expect: Arc::new(NullColumn::new(4)),
            error: "",
        },
    ];
    test_scalar_functions("xor", &tests)
}
