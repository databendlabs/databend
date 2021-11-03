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
fn test_logic_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        func_name: &'static str,
        display: &'static str,
        nullable: bool,
        arg_names: Vec<&'static str>,
        columns: Vec<DataColumn>,
        expect: DataColumn,
        error: &'static str,
        func: Box<dyn Function>,
    }

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Boolean, false),
        DataField::new("b", DataType::Boolean, false),
    ]);

    let tests = vec![
        Test {
            name: "and-passed",
            func_name: "AndFunction",
            display: "and",
            nullable: false,
            func: LogicAndFunction::try_create_func("")?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec![true, true, true, false]).into(),
                Series::new(vec![true, false, true, true]).into(),
            ],
            expect: Series::new(vec![true, false, true, false]).into(),
            error: "",
        },
        Test {
            name: "or-passed",
            func_name: "OrFunction",
            display: "or",
            nullable: false,
            func: LogicOrFunction::try_create_func("")?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec![true, true, true, false]).into(),
                Series::new(vec![true, false, true, true]).into(),
            ],
            expect: Series::new(vec![true, true, true, true]).into(),
            error: "",
        },
        Test {
            name: "not-passed",
            func_name: "NotFunction",
            display: "not",
            nullable: false,
            func: LogicNotFunction::try_create_func("")?,
            arg_names: vec!["a"],
            columns: vec![Series::new(vec![true, false]).into()],
            expect: Series::new(vec![false, true]).into(),
            error: "",
        },
    ];

    for t in tests {
        let func = t.func;
        let rows = t.columns[0].len();

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

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable(&schema)?;
        assert_eq!(expect_null, actual_null);

        let v = &(func.eval(&columns, rows)?);
        let expect_type = func.return_type(&args)?;
        let actual_type = v.data_type();
        assert_eq!(expect_type, actual_type);
        assert_eq!(v, &t.expect);
    }
    Ok(())
}
