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

#[test]
fn test_database_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        columns: Vec<DataColumnWithField>,
        expect: DataColumn,
        error: &'static str,
        func: Box<dyn Function>,
    }
    let dummy = DataField::new("dummy", DataType::String, false);

    let tests = vec![Test {
        name: "database-function-passed",
        display: "database",
        nullable: false,
        func: DatabaseFunction::try_create("database")?,
        columns: vec![
            DataColumnWithField::new(Series::new(vec!["default"]).into(), dummy.clone()),
            DataColumnWithField::new(Series::new(vec![4]).into(), dummy),
        ],
        expect: Series::new(vec!["default"]).into(),
        error: "",
    }];

    for t in tests {
        let rows = t.columns[0].column().len();
        let func = t.func;

        match func.eval(&t.columns, rows) {
            Ok(v) => {
                // Display check.
                let expect_display = t.display.to_string();
                let actual_display = format!("{}", func);
                assert_eq!(expect_display, actual_display);
                assert_eq!(&v, &t.expect);
            }
            Err(e) => {
                assert_eq!(t.error, e.to_string());
            }
        }
    }

    Ok(())
}
