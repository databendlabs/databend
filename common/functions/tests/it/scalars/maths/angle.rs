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

use std::f64::consts::PI;

use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::*;

#[test]
fn test_angle_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        columns: DataColumn,
        expect: DataColumn,
        error: &'static str,
        func: Box<dyn Function>,
    }

    let tests = vec![
        Test {
            name: "degress-passed",
            display: "degrees",
            nullable: false,
            columns: Series::new(vec![PI, PI / 2.0]).into(),
            func: DegressFunction::try_create("degrees")?,
            expect: Series::new(vec![180_f64, 90.0]).into(),
            error: "",
        },
        Test {
            name: "radians-passed",
            display: "radians",
            nullable: false,
            columns: Series::new(vec![180]).into(),
            func: RadiansFunction::try_create("radians")?,
            expect: Series::new(vec![PI]).into(),
            error: "",
        },
    ];

    for t in tests {
        let func = t.func;
        let rows = t.columns.len();

        let columns = vec![DataColumnWithField::new(
            t.columns.clone(),
            DataField::new("dummpy", t.columns.data_type(), false),
        )];

        if let Err(e) = func.eval(&columns, rows) {
            assert_eq!(t.error, e.to_string(), "{}", t.name);
        }

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        let v = &(func.eval(&columns, rows)?);
        assert_eq!(v, &t.expect, "{}", t.name);
    }
    Ok(())
}
