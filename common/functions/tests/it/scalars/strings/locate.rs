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
fn test_locate_function() -> Result<()> {
    struct Test {
        name: &'static str,
        display: &'static str,
        args: Vec<DataColumnWithField>,
        input_rows: usize,
        expect: DataColumn,
        error: &'static str,
    }
    let tests = vec![
        Test {
            name: "none, none, none",
            display: "locate",
            args: vec![
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::String(None), 1),
                    DataField::new("ss", DataType::String, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::String(None), 1),
                    DataField::new("s", DataType::String, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::UInt64(None), 1),
                    DataField::new("p", DataType::UInt64, false),
                ),
            ],
            input_rows: 1,
            expect: DataColumn::Constant(DataValue::Null, 1),
            error: "",
        },
        Test {
            name: "const, const, const",
            display: "locate",
            args: vec![
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::String(Some(b"ab".to_vec())), 1),
                    DataField::new("ss", DataType::String, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::String(Some(b"abcdabcd".to_vec())), 1),
                    DataField::new("s", DataType::String, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::UInt64(Some(2)), 1),
                    DataField::new("p", DataType::UInt64, false),
                ),
            ],
            input_rows: 1,
            expect: DataColumn::Constant(DataValue::UInt64(Some(5)), 1),
            error: "",
        },
        Test {
            name: "const, const, none",
            display: "locate",
            args: vec![
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::String(Some(b"ab".to_vec())), 1),
                    DataField::new("ss", DataType::String, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::String(Some(b"abcdabcd".to_vec())), 1),
                    DataField::new("s", DataType::String, false),
                ),
            ],
            input_rows: 1,
            expect: DataColumn::Constant(DataValue::UInt64(Some(1)), 1),
            error: "",
        },
        Test {
            name: "series, series, const",
            display: "locate",
            args: vec![
                DataColumnWithField::new(
                    DataColumn::Array(Series::new(["abcd", "efgh"])),
                    DataField::new("ss", DataType::String, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Array(Series::new(["_abcd_", "__efgh__"])),
                    DataField::new("s", DataType::String, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::UInt64(Some(1)), 1),
                    DataField::new("p", DataType::UInt64, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([2_u64, 3_u64]).into(),
            error: "",
        },
        Test {
            name: "const, series, const",
            display: "locate",
            args: vec![
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::String(Some(b"11".to_vec())), 1),
                    DataField::new("ss", DataType::String, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Array(Series::new(["_11_", "__11__"])),
                    DataField::new("s", DataType::String, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::UInt64(Some(1)), 1),
                    DataField::new("p", DataType::UInt64, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([2_u64, 3_u64]).into(),
            error: "",
        },
        Test {
            name: "series, const, const",
            display: "locate",
            args: vec![
                DataColumnWithField::new(
                    DataColumn::Array(Series::new(["11", "22"])),
                    DataField::new("s", DataType::String, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::String(Some(b"_11_22_".to_vec())), 1),
                    DataField::new("ss", DataType::String, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::UInt64(Some(1)), 1),
                    DataField::new("p", DataType::UInt64, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([2_u64, 5_u64]).into(),
            error: "",
        },
        Test {
            name: "const, const, series",
            display: "locate",
            args: vec![
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::String(Some(b"11".to_vec())), 1),
                    DataField::new("ss", DataType::String, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::String(Some(b"_11_11_".to_vec())), 1),
                    DataField::new("s", DataType::UInt64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Array(Series::new([1_u64, 3_u64])),
                    DataField::new("p", DataType::String, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([2_u64, 5_u64]).into(),
            error: "",
        },
        Test {
            name: "series, const, series",
            display: "locate",
            args: vec![
                DataColumnWithField::new(
                    DataColumn::Array(Series::new(["11", "22"])),
                    DataField::new("ss", DataType::String, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::String(Some(b"_11_22_".to_vec())), 1),
                    DataField::new("s", DataType::UInt64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Array(Series::new([1_u64, 3_u64])),
                    DataField::new("p", DataType::String, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([2_u64, 5_u64]).into(),
            error: "",
        },
        Test {
            name: "const, series, series",
            display: "locate",
            args: vec![
                DataColumnWithField::new(
                    DataColumn::Constant(DataValue::String(Some(b"11".to_vec())), 1),
                    DataField::new("ss", DataType::UInt64, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Array(Series::new(["_11_", "__11__"])),
                    DataField::new("ss", DataType::String, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Array(Series::new([1_u64, 2_u64])),
                    DataField::new("p", DataType::String, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([2_u64, 3_u64]).into(),
            error: "",
        },
        Test {
            name: "series, series, series",
            display: "locate",
            args: vec![
                DataColumnWithField::new(
                    DataColumn::Array(Series::new(["11", "22"])),
                    DataField::new("ss", DataType::String, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Array(Series::new(["_11_", "__22__"])),
                    DataField::new("ss", DataType::String, false),
                ),
                DataColumnWithField::new(
                    DataColumn::Array(Series::new([1_u64, 2_u64])),
                    DataField::new("p", DataType::String, false),
                ),
            ],
            input_rows: 1,
            expect: Series::new([2_u64, 3_u64]).into(),
            error: "",
        },
    ];

    for t in tests {
        let func = LocateFunction::try_create("locate")?;
        let actual_display = format!("{}", func);
        assert_eq!(t.display.to_string(), actual_display);

        if let Err(e) = func.eval(&t.args, t.input_rows) {
            assert_eq!(t.error, e.to_string(), "{}", t.name);
        }
        let v = &(func.eval(&t.args, t.input_rows)?);
        assert_eq!(v.to_values()?, t.expect.to_values()?, "case: {}", t.name);
    }
    Ok(())
}
