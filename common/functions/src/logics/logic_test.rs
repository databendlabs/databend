// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::logics::*;
use crate::*;

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
        expect: Series,
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
            func: LogicAndFunction::try_create_func("".clone())?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec![true, true, true, false]).into(),
                Series::new(vec![true, false, true, true]).into(),
            ],
            expect: Series::new(vec![true, false, true, false]),
            error: "",
        },
        Test {
            name: "or-passed",
            func_name: "OrFunction",
            display: "or",
            nullable: false,
            func: LogicOrFunction::try_create_func("".clone())?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Series::new(vec![true, true, true, false]).into(),
                Series::new(vec![true, false, true, true]).into(),
            ],
            expect: Series::new(vec![true, true, true, true]),
            error: "",
        },
        Test {
            name: "not-passed",
            func_name: "NotFunction",
            display: "not",
            nullable: false,
            func: LogicNotFunction::try_create_func("".clone())?,
            arg_names: vec!["a"],
            columns: vec![Series::new(vec![true, false]).into()],
            expect: Series::new(vec![false, true]),
            error: "",
        },
    ];

    for t in tests {
        let func = t.func;
        let rows = t.columns[0].len();
        if let Err(e) = func.eval(&t.columns, rows) {
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

        let ref v = func.eval(&t.columns, rows)?;
        // Type check.
        let mut args = vec![];
        for name in t.arg_names {
            args.push(schema.field_with_name(name)?.data_type().clone());
        }

        let expect_type = func.return_type(&args)?;
        let actual_type = v.data_type();
        assert_eq!(expect_type, actual_type);
        assert_eq!(v.to_array()?.as_ref(), t.expect.as_ref());
    }
    Ok(())
}
