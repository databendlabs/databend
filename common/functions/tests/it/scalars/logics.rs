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
use common_functions::scalars::LogicAndFunction;
use common_functions::scalars::LogicNotFunction;
use common_functions::scalars::LogicOrFunction;
use common_functions::scalars::LogicXorFunction;

use crate::scalars::scalar_function2_test::test_scalar_functions;
use crate::scalars::scalar_function2_test::ScalarFunctionTest;

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
            name: "not-null",
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
    test_scalar_functions(LogicNotFunction::try_create("not")?, &tests)
}

#[test]
fn test_logic_and_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "and",
            columns: vec![
                Series::from_data(vec![true, true, true, false]),
                Series::from_data(vec![true, false, true, true]),
            ],
            expect: Series::from_data(vec![true, false, true, false]),
            error: "",
        },
        ScalarFunctionTest {
            name: "and-null",
            columns: vec![
                Series::from_data(vec![None, Some(true), Some(true), Some(false)]),
                Series::from_data(vec![true, false, true, true]),
            ],
            expect: Series::from_data(vec![None, Some(false), Some(true), Some(false)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "and-null",
            columns: vec![
                Series::from_data(vec![None, Some(true), Some(true), Some(false)]),
                Arc::new(NullColumn::new(4)),
            ],
            expect: Arc::new(NullColumn::new(4)),
            error: "",
        },
    ];
    test_scalar_functions(LogicAndFunction::try_create("and")?, &tests)
}

#[test]
fn test_logic_or_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "or",
            columns: vec![
                Series::from_data(vec![true, true, true, false]),
                Series::from_data(vec![true, false, true, false]),
            ],
            expect: Series::from_data(vec![true, true, true, false]),
            error: "",
        },
        ScalarFunctionTest {
            name: "or-null",
            columns: vec![
                Series::from_data(vec![None, None, None, Some(false)]),
                Series::from_data(vec![Some(true), Some(false), None, Some(true)]),
            ],
            expect: Series::from_data(vec![Some(true), None, None, Some(true)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "or-null",
            columns: vec![
                Series::from_data(vec![None, None, None, Some(false)]),
                Series::from_data(vec![Some(true), Some(false), None, Some(true)]),
            ],
            expect: Series::from_data(vec![Some(true), None, None, Some(true)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "or-null",
            columns: vec![
                Arc::new(NullColumn::new(4)),
                Series::from_data(vec![Some(true), None, None, Some(false)]),
            ],
            expect: Series::from_data(vec![Some(true), None, None, None]),
            error: "",
        },
    ];
    test_scalar_functions(LogicOrFunction::try_create("or")?, &tests)
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
    test_scalar_functions(LogicXorFunction::try_create("or")?, &tests)
}
