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

#[test]
fn test_crc32_function() -> Result<()> {
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
            name: "crc-MySQL-passed",
            display: "CRC",
            nullable: false,
            columns: Series::new(vec!["MySQL"]).into(),
            func: CRC32Function::try_create("crc")?,
            expect: Series::new(vec![3259397556u32]).into(),
            error: "",
        },
        Test {
            name: "crc-mysql-passed",
            display: "CRC",
            nullable: false,
            columns: Series::new(vec!["mysql"]).into(),
            func: CRC32Function::try_create("crc")?,
            expect: Series::new(vec![2501908538u32]).into(),
            error: "",
        },
        Test {
            name: "crc-1-passed",
            display: "CRC",
            nullable: true,
            columns: Series::new(vec![1]).into(),
            func: CRC32Function::try_create("crc")?,
            expect: Series::new(vec![2212294583u32]).into(),
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
        assert_eq!(v, &t.expect);
    }
    Ok(())
}
