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

use crate::scalars::scalar_function_test::test_scalar_functions;
use crate::scalars::scalar_function_test::ScalarFunctionTest;

#[test]
fn test_locate_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "none, none, none",
            columns: vec![
                Series::from_data([Option::<&str>::None]),
                Series::from_data([Option::<&str>::None]),
                Series::from_data([Option::<u64>::None]),
            ],
            expect: Series::from_data([Option::<u64>::None]),
            error: "",
        },
        ScalarFunctionTest {
            name: "const, const, const",
            columns: vec![
                Arc::new(ConstColumn::new(Series::from_data(vec![Some("ab")]), 1)),
                Arc::new(ConstColumn::new(
                    Series::from_data(vec![Some("abcdabcd")]),
                    1,
                )),
                Arc::new(ConstColumn::new(Series::from_data(vec![Some(2u64)]), 1)),
            ],
            expect: Arc::new(ConstColumn::new(Series::from_data(vec![Some(5u64)]), 1)),
            error: "",
        },
        ScalarFunctionTest {
            name: "const, const, none",
            columns: vec![
                Arc::new(ConstColumn::new(Series::from_data(vec![Some("ab")]), 1)),
                Arc::new(ConstColumn::new(
                    Series::from_data(vec![Some("abcdabcd")]),
                    1,
                )),
            ],
            expect: Arc::new(ConstColumn::new(Series::from_data(vec![Some(1u64)]), 1)),
            error: "",
        },
        ScalarFunctionTest {
            name: "series, series, const",
            columns: vec![
                Series::from_data(["abcd", "efgh"]),
                Series::from_data(["_abcd_", "__efgh__"]),
                Arc::new(ConstColumn::new(Series::from_data(vec![Some(1u64)]), 2)),
            ],
            expect: Series::from_data([Some(2_u64), Some(3_u64)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "const, series, const",
            columns: vec![
                Arc::new(ConstColumn::new(Series::from_data(vec![Some("11")]), 2)),
                Series::from_data(["_11_", "__11__"]),
                Arc::new(ConstColumn::new(Series::from_data(vec![Some(1u64)]), 2)),
            ],
            expect: Series::from_data([Some(2_u64), Some(3_u64)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "series, const, const",
            columns: vec![
                Series::from_data(["11", "22"]),
                Arc::new(ConstColumn::new(
                    Series::from_data(vec![Some("_11_22_")]),
                    2,
                )),
                Arc::new(ConstColumn::new(Series::from_data(vec![Some(1u64)]), 2)),
            ],
            expect: Series::from_data([Some(2_u64), Some(5_u64)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "const, const, series",
            columns: vec![
                Arc::new(ConstColumn::new(Series::from_data(vec![Some("11")]), 2)),
                Arc::new(ConstColumn::new(
                    Series::from_data(vec![Some("_11_11_")]),
                    2,
                )),
                Series::from_data([1_u64, 3_u64]),
            ],
            expect: Series::from_data([Some(2_u64), Some(5_u64)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "series, const, series",
            columns: vec![
                Series::from_data(["11", "22"]),
                Arc::new(ConstColumn::new(
                    Series::from_data(vec![Some("_11_22_")]),
                    2,
                )),
                Series::from_data([1_u64, 3_u64]),
            ],
            expect: Series::from_data([Some(2_u64), Some(5_u64)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "const, series, series",
            columns: vec![
                Arc::new(ConstColumn::new(Series::from_data(vec![Some("11")]), 2)),
                Series::from_data(["_11_", "__11__"]),
                Series::from_data([1_u64, 2_u64]),
            ],
            expect: Series::from_data([Some(2_u64), Some(3_u64)]),
            error: "",
        },
        ScalarFunctionTest {
            name: "series, series, series",
            columns: vec![
                Series::from_data(["11", "22"]),
                Series::from_data(["_11_", "__22__"]),
                Series::from_data([1_u64, 2_u64]),
            ],
            expect: Series::from_data([2_u64, 3_u64]),
            error: "",
        },
    ];

    test_scalar_functions("locate", &tests)
}
