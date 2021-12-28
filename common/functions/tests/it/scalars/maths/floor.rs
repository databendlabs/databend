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
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::*;
use crate::scalars::scalar_function_test::{ScalarFunctionTest, test_scalar_functions};

#[test]
fn test_floor_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "floor(123)",
            nullable: false,
            columns: vec![Series::new([123]).into()],
            expect: Series::new(vec![123_f64]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "floor(1.7)",
            columns: vec![Series::new([1.7]).into()],
            expect: Series::new(vec![1_f64]).into(),
            nullable: false,
            error: "",
        },
        ScalarFunctionTest {
            name: "floor(-2.1)",
            nullable: false,
            columns: vec![Series::new([-2.1]).into()],
            expect: Series::new(vec![-3_f64]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "floor('123')",
            columns: vec![Series::new(["123"]).into()],
            expect: Series::new(vec![123_f64]).into(),
            nullable: false,
            error: "",
        },
        ScalarFunctionTest {
            name: "floor('+123.8a1')",
            nullable: false,
            columns: vec![Series::new(["+123.8a1"]).into()],
            expect: Series::new(vec![123_f64]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "floor('-123.2a1')",
            nullable: false,
            columns: vec![Series::new(["-123.2a1"]).into()],
            expect: Series::new(vec![-124_f64]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "floor('a')",
            nullable: false,
            columns: vec![Series::new(["a"]).into()],
            expect: Series::new(vec![0_f64]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "floor('a123')",
            nullable: false,
            columns: vec![Series::new(["a123"]).into()],
            expect: Series::new(vec![0_f64]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "floor(true)",
            nullable: false,
            columns: vec![Series::new([true]).into()],
            expect: Series::new([1_u8]).into(),
            error: "Expected numeric types, but got Boolean",
        },
    ];

    test_scalar_functions(FloorFunction::try_create("floor")?, &tests)
}
