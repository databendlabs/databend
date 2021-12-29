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
use common_functions::scalars::*;

use crate::scalars::scalar_function_test::test_scalar_functions;
use crate::scalars::scalar_function_test::ScalarFunctionTest;

#[test]
fn test_sign_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "positive int",
            nullable: false,
            columns: vec![Series::new([11_i8]).into()],
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "negative int",
            nullable: false,
            columns: vec![Series::new([-11_i8]).into()],
            expect: Series::new([-1_i8]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "zero int",
            nullable: false,
            columns: vec![Series::new([0_i8]).into()],
            expect: Series::new([0_i8]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "with null",
            nullable: true,
            columns: vec![Series::new([Some(0_i8), None]).into()],
            expect: Series::new([Some(0_i8), None]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "int as string",
            nullable: false,
            columns: vec![Series::new(["22"]).into()],
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "number with string postfix",
            nullable: false,
            columns: vec![Series::new(["22abc"]).into()],
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "number with string prefix",
            nullable: false,
            columns: vec![Series::new(["abc22def"]).into()],
            expect: Series::new([0_i8]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "i16",
            nullable: false,
            columns: vec![Series::new([11_i16]).into()],
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "i32",
            nullable: false,
            columns: vec![Series::new([11_i32]).into()],
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "i64",
            nullable: false,
            columns: vec![Series::new([11_i64]).into()],
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "u8",
            nullable: false,
            columns: vec![Series::new([11_u8]).into()],
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "u16",
            nullable: false,
            columns: vec![Series::new([11_u16]).into()],
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "u32",
            nullable: false,
            columns: vec![Series::new([11_u32]).into()],
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "u64",
            nullable: false,
            columns: vec![Series::new([11_u64]).into()],
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "f32",
            nullable: false,
            columns: vec![Series::new([11.11_f32]).into()],
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "f64",
            nullable: false,
            columns: vec![Series::new([11.11_f64]).into()],
            expect: Series::new([1_i8]).into(),
            error: "",
        },
    ];

    test_scalar_functions(SignFunction::try_create("sign")?, &tests)
}
