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
use common_datavalues::DataType;
use common_exception::Result;
use common_functions::scalars::*;
use pretty_assertions::assert_eq;

#[test]
fn test_uuid_generator_functions() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        args: Vec<DataType>,
        columns: Vec<DataColumn>,
        expect: Option<Series>,
        error: &'static str,
        func: Box<dyn Function>,
    }

    let schema = DataSchemaRefExt::create(vec![]);

    let tests = vec![
        Test {
            name: "generateUUIDv4-passed",
            display: "generateUUIDv4",
            nullable: false,
            func: GenerateUUIDv4Function::try_create_func("")?,
            args: vec![],
            columns: vec![],
            expect: None,
            error: "",
        },
        Test {
            name: "zeroUUID-passed",
            display: "zeroUUID",
            nullable: false,
            func: ZeroUUIDFunction::try_create_func("")?,
            args: vec![],
            columns: vec![],
            expect: Some(Series::new(vec!["00000000-0000-0000-0000-000000000000"])),
            error: "",
        },
    ];

    for t in tests {
        let func = t.func;

        let columns: Vec<DataColumnWithField> = vec![];

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable(&schema)?;
        assert_eq!(expect_null, actual_null);

        let v = &(func.eval(&columns, 0)?);
        // Type check.
        let expect_type = func.return_type(&t.args)?;
        let actual_type = v.data_type();
        assert_eq!(expect_type, actual_type);

        let series = v.to_array()?;
        assert!(series.data_type() == &DataType::String);

        if let Some(expect) = t.expect {
            assert!(series.eq(&expect)?.all_true());
        }
    }

    Ok(())
}
