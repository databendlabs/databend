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
use common_datavalues::prelude::*;
use common_exception::Result;
use pretty_assertions::assert_eq;
use super::timeslot::TimeSlotFunction;

use crate::scalars::*;

#[allow(dead_code)]
struct Test {
    name: &'static str,
    display: &'static str,
    nullable: bool,
    columns: Vec<DataColumn>,
    expect: Series,
    error: &'static str,
    func: Result<Box<dyn Function>>,
}

#[test]
fn test_date_function() -> Result<()> {

    let tests = vec![
        Test {
            name: "test-timeSlot",
            display: "timeSlot",
            nullable: false,
            columns: vec![Series::new(vec!["2021-12-21 12:13:15"]).into()],
            func: TimeSlotFunction::try_create("timeSlot"),
            expect: Series::new(vec![1213321231u32]),
            error: "",
        },
    ];

    for t in tests {
        do_test(t);
    }

    Ok(())
}

fn do_test(t: Test) -> Result<()> {
    let dummy = DataField::new("dummy", DataType::String, false);
    let rows = t.columns[0].len();
    let columns: Vec<DataColumnWithField> = t
        .columns
        .iter()
        .map(|c| DataColumnWithField::new(c.clone(), dummy.clone()))
        .collect();

    let func = t.func.unwrap();
    if let Err(e) = func.eval(&columns, rows) {
        assert_eq!(t.error, e.to_string());
    }
    // Display check.
    let expect_display = t.display.to_string();
    let actual_display = format!("{}", func);
    assert_eq!(expect_display, actual_display);

    // Nullable check.
    let expect_null = t.nullable;
    let actual_null = func.nullable(&DataSchema::empty())?;
    assert_eq!(expect_null, actual_null);

    Ok(())
}