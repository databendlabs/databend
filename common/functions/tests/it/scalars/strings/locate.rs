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
fn test_locate_function() -> Result<()> {
    let tests = vec![
        ScalarFunctionTest {
            name: "none, none, none",
            nullable: true,
            columns: vec![
                Series::new([Option::<&str>::None]).into(),
                Series::new([Option::<&str>::None]).into(),
                Series::new([Option::<&str>::None]).into(),
            ],
            expect: Series::new([Option::<u64>::None]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "const, const, const",
            nullable: false,
            columns: vec![
                DataColumn::Constant(DataValue::String(Some(b"ab".to_vec())), 1),
                DataColumn::Constant(DataValue::String(Some(b"abcdabcd".to_vec())), 1),
                DataColumn::Constant(DataValue::UInt64(Some(2)), 1),
            ],
            expect: DataColumn::Constant(DataValue::UInt64(Some(5)), 1),
            error: "",
        },
        ScalarFunctionTest {
            name: "const, const, none",
            nullable: false,
            columns: vec![
                DataColumn::Constant(DataValue::String(Some(b"ab".to_vec())), 1),
                DataColumn::Constant(DataValue::String(Some(b"abcdabcd".to_vec())), 1),
            ],
            expect: DataColumn::Constant(DataValue::UInt64(Some(1)), 1),
            error: "",
        },
        ScalarFunctionTest {
            name: "series, series, const",
            nullable: false,
            columns: vec![
                Series::new(["abcd", "efgh"]).into(),
                Series::new(["_abcd_", "__efgh__"]).into(),
                DataColumn::Constant(DataValue::UInt64(Some(1)), 1),
            ],
            expect: Series::new([2_u64, 3_u64]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "const, series, const",
            nullable: false,
            columns: vec![
                DataColumn::Constant(DataValue::String(Some(b"11".to_vec())), 1),
                DataColumn::Array(Series::new(["_11_", "__11__"])),
                DataColumn::Constant(DataValue::UInt64(Some(1)), 1),
            ],
            expect: Series::new([2_u64, 3_u64]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "series, const, const",
            nullable: false,
            columns: vec![
                DataColumn::Array(Series::new(["11", "22"])),
                DataColumn::Constant(DataValue::String(Some(b"_11_22_".to_vec())), 1),
                DataColumn::Constant(DataValue::UInt64(Some(1)), 1),
            ],
            expect: Series::new([2_u64, 5_u64]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "const, const, series",
            nullable: false,
            columns: vec![
                DataColumn::Constant(DataValue::String(Some(b"11".to_vec())), 1),
                DataColumn::Constant(DataValue::String(Some(b"_11_11_".to_vec())), 1),
                DataColumn::Array(Series::new([1_u64, 3_u64])),
            ],
            expect: Series::new([2_u64, 5_u64]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "series, const, series",
            nullable: false,
            columns: vec![
                DataColumn::Array(Series::new(["11", "22"])),
                DataColumn::Constant(DataValue::String(Some(b"_11_22_".to_vec())), 1),
                DataColumn::Array(Series::new([1_u64, 3_u64])),
            ],
            expect: Series::new([2_u64, 5_u64]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "const, series, series",
            nullable: false,
            columns: vec![
                DataColumn::Constant(DataValue::String(Some(b"11".to_vec())), 1),
                DataColumn::Array(Series::new(["_11_", "__11__"])),
                DataColumn::Array(Series::new([1_u64, 2_u64])),
            ],
            expect: Series::new([2_u64, 3_u64]).into(),
            error: "",
        },
        ScalarFunctionTest {
            name: "series, series, series",
            nullable: false,
            columns: vec![
                DataColumn::Array(Series::new(["11", "22"])),
                DataColumn::Array(Series::new(["_11_", "__22__"])),
                DataColumn::Array(Series::new([1_u64, 2_u64])),
            ],
            expect: Series::new([2_u64, 3_u64]).into(),
            error: "",
        },
    ];

    test_scalar_functions(LocateFunction::try_create("locate")?, &tests)
}
