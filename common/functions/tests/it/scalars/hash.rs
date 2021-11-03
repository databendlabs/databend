// Copyright 2020 Datafuse Labs.
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

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::SipHashFunction;

#[test]
fn test_siphash_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        input_column: DataColumn,
        expect_output_column: DataColumn,
        error: &'static str,
    }

    let tests = vec![
        Test {
            name: "Int8Array siphash",
            input_column: Series::new(vec![1i8, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                4952851536318644461u64,
                7220060526038107403,
                4952851536318644461,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "Int16Array siphash",
            input_column: Series::new(vec![1i16, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                10500823559348167161u64,
                4091451155859037844,
                10500823559348167161,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "Int32Array siphash",
            input_column: Series::new(vec![1i32, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                1742378985846435984u64,
                16336925911988107921,
                1742378985846435984,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "Int64Array siphash",
            input_column: Series::new(vec![1i64, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                2206609067086327257u64,
                11876854719037224982,
                2206609067086327257,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "UInt8Array siphash",
            input_column: Series::new(vec![1u8, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                4952851536318644461u64,
                7220060526038107403,
                4952851536318644461,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "UInt16Array siphash",
            input_column: Series::new(vec![1u16, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                10500823559348167161u64,
                4091451155859037844,
                10500823559348167161,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "UInt32Array siphash",
            input_column: Series::new(vec![1u32, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                1742378985846435984u64,
                16336925911988107921,
                1742378985846435984,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "UInt64Array siphash",
            input_column: Series::new(vec![1u64, 2, 1]).into(),
            expect_output_column: Series::new(vec![
                2206609067086327257u64,
                11876854719037224982,
                2206609067086327257,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "Float32Array siphash",
            input_column: Series::new(vec![1.0f32, 2., 1.]).into(),
            expect_output_column: Series::new(vec![
                729488449357906283u64,
                9872512741335963328,
                729488449357906283,
            ])
            .into(),
            error: "",
        },
        Test {
            name: "Float64Array siphash",
            input_column: Series::new(vec![1.0f64, 2., 1.]).into(),
            expect_output_column: Series::new(vec![
                13833534234735907638u64,
                12773237290464453619,
                13833534234735907638,
            ])
            .into(),
            error: "",
        },
    ];

    for test in tests {
        let function = SipHashFunction::try_create("siphash")?;

        let rows = test.input_column.len();

        let columns = vec![DataColumnWithField::new(
            test.input_column.clone(),
            DataField::new("dummpy", test.input_column.data_type(), false),
        )];
        match function.eval(&columns, rows) {
            Ok(result_column) => assert_eq!(
                &result_column.get_array_ref()?,
                &test.expect_output_column.get_array_ref()?,
                "failed in the test: {}",
                test.name
            ),
            Err(error) => assert_eq!(
                test.error,
                error.to_string(),
                "failed in the test: {}",
                test.name
            ),
        };
    }

    Ok(())
}
