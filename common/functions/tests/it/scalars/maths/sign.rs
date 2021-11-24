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

#[test]
fn test_sign_function() -> Result<()> {
    struct Test {
        name: &'static str,
        display: &'static str,
        columns: DataColumn,
        expect: DataColumn,
        error: &'static str,
    }
    let tests = vec![
        Test {
            name: "positive int",
            display: "SIGN",
            columns: Series::new([11_i8]).into(),
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        Test {
            name: "negative int",
            display: "SIGN",
            columns: Series::new([-11_i8]).into(),
            expect: Series::new([-1_i8]).into(),
            error: "",
        },
        Test {
            name: "zero int",
            display: "SIGN",
            columns: Series::new([0_i8]).into(),
            expect: Series::new([0_i8]).into(),
            error: "",
        },
        Test {
            name: "with null",
            display: "SIGN",
            columns: Series::new([Some(0_i8), None]).into(),
            expect: Series::new([Some(0_i8), None]).into(),
            error: "",
        },
        Test {
            name: "int as string",
            display: "SIGN",
            columns: Series::new(["22"]).into(),
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        Test {
            name: "number with string postfix",
            display: "SIGN",
            columns: Series::new(["22abc"]).into(),
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        Test {
            name: "number with string prefix",
            display: "SIGN",
            columns: Series::new(["abc22def"]).into(),
            expect: Series::new([0_i8]).into(),
            error: "",
        },
        Test {
            name: "i16",
            display: "SIGN",
            columns: Series::new([11_i16]).into(),
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        Test {
            name: "i32",
            display: "SIGN",
            columns: Series::new([11_i32]).into(),
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        Test {
            name: "i64",
            display: "SIGN",
            columns: Series::new([11_i64]).into(),
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        Test {
            name: "u8",
            display: "SIGN",
            columns: Series::new([11_u8]).into(),
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        Test {
            name: "u16",
            display: "SIGN",
            columns: Series::new([11_u16]).into(),
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        Test {
            name: "u32",
            display: "SIGN",
            columns: Series::new([11_u32]).into(),
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        Test {
            name: "u64",
            display: "SIGN",
            columns: Series::new([11_u64]).into(),
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        Test {
            name: "f32",
            display: "SIGN",
            columns: Series::new([11.11_f32]).into(),
            expect: Series::new([1_i8]).into(),
            error: "",
        },
        Test {
            name: "f64",
            display: "SIGN",
            columns: Series::new([11.11_f64]).into(),
            expect: Series::new([1_i8]).into(),
            error: "",
        },
    ];

    for t in tests {
        let func = SignFunction::try_create("sign")?;
        let rows = t.columns.len();

        let columns = vec![DataColumnWithField::new(
            t.columns.clone(),
            DataField::new("dummpy", t.columns.data_type(), false),
        )];

        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        if let Err(e) = func.eval(&columns, rows) {
            assert_eq!(t.error, e.to_string(), "{}", t.name);
        }

        let v = &(func.eval(&columns, rows)?);
        assert_eq!(v, &t.expect);
    }
    Ok(())
}
