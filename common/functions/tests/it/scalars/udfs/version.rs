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
fn test_version_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        columns: Vec<DataColumn>,
        expect: DataColumn,
        error: &'static str,
        func: Box<dyn Function>,
    }

    let tests = vec![Test {
        name: "version-function-passed",
        display: "version",
        nullable: false,
        func: VersionFunction::try_create("version")?,
        columns: vec![Series::new(vec![
            "DatabendQuery v-0.1.0-3afb26c(1.54.0-nightly-2021-06-09T07:56:09.461981495+00:00)",
        ])
        .into()],
        expect: Series::new(vec![
            "DatabendQuery v-0.1.0-3afb26c(1.54.0-nightly-2021-06-09T07:56:09.461981495+00:00)",
        ])
        .into(),
        error: "",
    }];

    let dummy = DataField::new("dummy", DataType::String, false);
    for t in tests {
        let rows = t.columns[0].len();
        let func = t.func;
        let columns = vec![DataColumnWithField::new(
            t.columns[0].clone(),
            dummy.clone(),
        )];

        match func.eval(&columns, rows) {
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
