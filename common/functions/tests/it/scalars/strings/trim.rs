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
use common_functions::scalars::Function;
use common_functions::scalars::LTrimFunction;
use common_functions::scalars::RTrimFunction;
use common_functions::scalars::TrimFunction;
use pretty_assertions::assert_eq;

#[test]
fn test_trim_function() -> Result<()> {
     struct Test {
        display: &'static str,
        nullable: bool,
        arg_names: Vec<&'static str>,
        columns: Vec<DataColumn>,
        expect: DataColumn,
        error: &'static str,
        func: Box<dyn Function>,
    }

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::String, false),
        DataField::new("b", DataType::Int64, false),
        DataField::new("c", DataType::UInt64, false),
    ]);

    let tests = vec![
        Test {
            display: "ltrim",
            nullable: true,
            arg_names: vec!["a"],
            columns: vec![
                Series::new(vec!["  abc"]).into(),
            ],
            func: LTrimFunction::try_create("ltrim")?,
            expect: Series::new(vec!["abc"]).into(),
            error: "",
        },
        Test {
            display: "rtrim",
            nullable: true,
            arg_names: vec!["a"],
            columns: vec![
                Series::new(vec!["abc  "]).into(),
            ],
            func: RTrimFunction::try_create("rtrim")?,
            expect: Series::new(vec!["abc"]).into(),
            error: "",
        },
        Test {
            display: "trim",
            nullable: true,
            arg_names: vec!["a"],
            columns: vec![
                Series::new(vec!["   abc  "]).into(),
            ],
            func: TrimFunction::try_create("trim")?,
            expect: Series::new(vec!["abc"]).into(),
            error: "",
        },
        Test {
            display: "trim",
            nullable: true,
            arg_names: vec!["a"],
            columns: vec![
                Series::new(vec!["     "]).into(),
            ],
            func: TrimFunction::try_create("trim")?,
            expect: Series::new(vec![""]).into(),
            error: "",
        },
    ];

    for t in tests {
        let func = t.func;
        let rows = t.columns.len();

        // Type check.
        let mut args = vec![];
        let mut fields = vec![];
        for name in t.arg_names {
            args.push(schema.field_with_name(name)?.data_type().clone());
            fields.push(schema.field_with_name(name)?.clone());
        }

        let columns: Vec<DataColumnWithField> = t
            .columns
            .iter()
            .zip(fields.iter())
            .map(|(c, f)| DataColumnWithField::new(c.clone(), f.clone()))
            .collect();

        if let Err(e) = func.eval(&columns, rows) {
            assert_eq!(t.error, e.to_string());
        }
        func.eval(&columns, rows)?;

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable(&schema)?;
        assert_eq!(expect_null, actual_null);

        let v = &(func.eval(&columns, rows)?);

        // Type check.
        let expect_type = func.return_type(&args)?;
        let actual_type = v.data_type();
        assert_eq!(expect_type, actual_type);
        assert_eq!(v, &t.expect);
    }
    Ok(())
}