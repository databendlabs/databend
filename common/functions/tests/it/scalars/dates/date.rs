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
use common_functions::scalars::*;
use pretty_assertions::assert_eq;

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
fn test_round_function() -> Result<()> {
    let mut tests = vec![];

    for r in &[1, 60, 60 * 10, 60 * 15, 60 * 30, 60 * 60, 60 * 60 * 24] {
        tests.push(Test {
            name: "test-timeSlot-now",
            display: "toStartOfCustom",
            nullable: false,
            columns: vec![Series::new(vec![1630812366u32, 1630839682u32]).into()],
            func: RoundFunction::try_create("toStartOfCustom", *r),
            expect: Series::new(vec![1630812366u32 / r * r, 1630839682u32 / r * r]),
            error: "",
        })
    }

    for t in tests {
        do_test(t)?;
    }
    Ok(())
}

#[test]
fn test_to_start_of_function() -> Result<()> {
    let test = Test {
        name: "test-timeSlot-now",
        display: "toStartOfWeek()",
        nullable: false,
        columns: vec![Series::new(vec![1631705259u32]).into()],
        func: ToStartOfQuarterFunction::try_create("toStartOfWeek"),
        expect: Series::new(vec![18809u16]),
        error: "",
    };

    do_test(test)?;
    Ok(())
}

fn do_test(t: Test) -> Result<()> {
    let dummy = DataField::new("dummy", DataType::DateTime32(None), false);
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

    //Eq check
    let v = func.eval(&columns, rows)?;
    let expect: DataColumn = t.expect.into();
    assert_eq!(&expect, &v);
    Ok(())
}
