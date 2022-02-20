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
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::IfFunction;

use crate::scalars::scalar_function2_test::test_scalar_functions;
use crate::scalars::scalar_function2_test::ScalarFunctionTest;

#[test]
fn test_if_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "if-primitive",
            columns: vec![
                Series::from_data([true, false, false, true]),
                Series::from_data([1u8, 2, 3, 4]),
                Series::from_data([2u8, 3u8, 2u8, 2u8]),
            ],
            expect: Series::from_data(vec![1u8, 3, 2, 4]), // non-nullable
            error: "",
        },
        ScalarFunctionTest {
            name: "if-string",
            columns: vec![
                Series::from_data([true, false, false, true]),
                Series::from_data(["1_aa", "1_bb", "1_cc", "1_dd"]),
                Series::from_data(["2_aa", "2_bb", "2_cc", "2_dd"]),
            ],
            expect: Series::from_data(vec!["1_aa", "2_bb", "2_cc", "1_dd"]), // non-nullable
            error: "",
        },
        ScalarFunctionTest {
            name: "if-bool",
            columns: vec![
                Series::from_data([true, false, false, true]),
                Series::from_data([true, true, true, true]),
                Series::from_data([false, false, false, false]),
            ],
            expect: Series::from_data(vec![true, false, false, true]), // non-nullable
            error: "",
        },
        ScalarFunctionTest {
            name: "if-null-in-predicate",
            columns: vec![
                Series::from_data([Some(true), None, Some(false), Some(true)]),
                Series::from_data([Some(1u8), Some(2u8), Some(3u8), None]),
                Series::from_data([2i32, 3, 2, 2]),
            ],
            expect: Series::from_data(vec![Some(1i32), Some(3i32), Some(2i32), None]), // nullable becase predicate is nullable
            error: "",
        },
        ScalarFunctionTest {
            name: "if-nullable-and-nonnullable",
            columns: vec![
                Series::from_data([Some(1u8), None, None, Some(2)]),
                Series::from_data([Some(2u8), Some(2), Some(2), Some(2)]),
                Series::from_data([Some(3i32), Some(3i32), None, None]),
            ],
            expect: Series::from_data(vec![Some(2i32), Some(3i32), None, Some(2i32)]), // nullable becase predicate and rhs are nullable
            error: "",
        },
        ScalarFunctionTest {
            name: "if-all-nullable",
            columns: vec![
                Series::from_data([Some(true), None, Some(false), Some(false)]),
                Series::from_data([Some(1u8), Some(2), Some(3), Some(4)]),
                Series::from_data([Some(2i32), Some(3), None, Some(2)]),
            ],
            expect: Series::from_data(vec![Some(1i32), Some(3i32), None, Some(2i32)]), // nullable becase all column are nullable
            error: "",
        },
        ScalarFunctionTest {
            name: "if-null",
            columns: vec![
                Series::from_data([true, false, false, true]),
                Series::from_data([Some(1u8), Some(2), Some(3), Some(4)]),
                Arc::new(NullColumn::new(4)),
            ],
            expect: Series::from_data(vec![Some(1u8), None, None, Some(4)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "if-null",
            columns: vec![
                Arc::new(NullColumn::new(4)),
                Series::from_data([0u8, 0, 0, 0]),
                Series::from_data([1u8, 2, 3, 4]),
            ],
            expect: Series::from_data(vec![1u8, 2, 3, 4]),
            error: "",
        },
    ];

    test_scalar_functions(IfFunction::try_create("if")?, &tests)
}
