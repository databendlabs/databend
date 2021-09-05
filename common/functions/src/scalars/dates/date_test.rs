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
use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Timelike;
use common_datavalues::chrono::Utc;
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
    let tests = vec![Test {
        name: "test-timeSlot-now",
        display: "timeSlot",
        nullable: false,
        columns: vec![Series::new(vec![1630812366u32]).into()],
        func: TimeSlotFunction::try_create("timeSlot"),
        expect: Series::new(vec![1630810806u32]),
        error: "",
    }];
    for t in tests {
        do_test(t);
    }

    Ok(())
}

#[test]
fn test_timeslot_now_function() -> Result<()> {
    let now = Utc::now();
    let now_timestamp = now.timestamp_millis() / 1000;
    let mut minute = 0;
    if now.minute() > 30 {
        minute = 30;
    }
    let new_date = now.with_minute(minute).unwrap();
    let check_timestamp = new_date.timestamp_millis() / 1000;
    let empty: Vec<i32> = Vec::new();

    let tests = vec![Test {
        name: "test-timeSlot",
        display: "timeSlot",
        nullable: false,
        columns: vec![Series::new(vec![now_timestamp]).into()],
        func: TimeSlotFunction::try_create("timeSlot"),
        expect: Series::new(vec![check_timestamp]),
        error: "",
    }, Test {
        name: "test-timeSlot-now",
        display: "timeSlot",
        nullable: false,
        columns: vec![Series::new(empty).into()],
        func: TimeSlotFunction::try_create("timeSlot"),
        expect: Series::new(vec![check_timestamp]),
        error: "",
    }];

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

    let ref v = func.eval(&columns, rows)?;
    let mut eval_result = v.to_values()?;
    let c: DataColumn = t.expect.into();
    let check_result = c.to_values()?;
    assert_eq!(v.len(), c.len());
    for i in 0..v.len() {
        let eval_val = eval_result.get(i).unwrap();
        let check_val = check_result.get(i).unwrap();
        let eval_str = format!("{}", eval_val);
        let check_str = format!("{}", check_val);
        assert_eq!(eval_str, check_str);
    }
    Ok(())
}
