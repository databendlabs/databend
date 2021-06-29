// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;
use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::arithmetics::*;
use crate::*;

#[test]
fn test_arithmetic_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        arg_names: Vec<&'static str>,
        columns: Vec<DataColumn>,
        expect: DataColumn,
        error: &'static str,
        func: Box<dyn Function>,
    }

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Int64, false),
        DataField::new("c", DataType::Int16, false),
    ]);

    let tests = vec![
        Test {
            name: "add-int64-passed",
            display: "plus",
            nullable: false,
            arg_names: vec!["a", "b"],
            func: ArithmeticPlusFunction::try_create_func("")?,
            columns: vec![
                Series::new(vec![4i64, 3, 2, 1]).into(),
                Series::new(vec![1i64, 2, 3, 4]).into(),
                Series::new(vec![1i16, 2, 3, 4]).into(),
            ],
            expect: Series::new(vec![5i64, 5, 5, 5]).into(),
            error: "",
        },
        Test {
            name: "add-diff-passed",
            display: "plus",
            nullable: false,
            arg_names: vec!["c", "b"],
            func: ArithmeticPlusFunction::try_create_func("")?,
            columns: vec![
                Series::new(vec![4i64, 3, 2, 1]).into(),
                Series::new(vec![1i64, 2, 3, 4]).into(),
                Series::new(vec![1i16, 2, 3, 4]).into(),
            ],
            expect: Series::new(vec![5i64, 5, 5, 5]).into(),
            error: "",
        },
        Test {
            name: "sub-int64-passed",
            display: "minus",
            arg_names: vec!["a", "b"],
            nullable: false,
            func: ArithmeticMinusFunction::try_create_func("")?,
            columns: vec![
                Series::new(vec![4i64, 3, 2]).into(),
                Series::new(vec![1i64, 2, 3]).into(),
                Series::new(vec![1i16, 2, 3]).into(),
            ],
            expect: Series::new(vec![3i64, 1, -1]).into(),
            error: "",
        },
        Test {
            name: "mul-int64-passed",
            display: "multiply",
            arg_names: vec!["a", "b"],
            nullable: false,
            func: ArithmeticMulFunction::try_create_func("")?,
            columns: vec![
                Series::new(vec![4i64, 3, 2]).into(),
                Series::new(vec![1i64, 2, 3]).into(),
                Series::new(vec![1i16, 2, 3]).into(),
            ],
            expect: Series::new(vec![4i64, 6, 6]).into(),
            error: "",
        },
        Test {
            name: "div-int64-passed",
            display: "divide",
            arg_names: vec!["a", "b"],
            nullable: false,
            func: ArithmeticDivFunction::try_create_func("")?,
            columns: vec![
                Series::new(vec![4i64, 3, 2]).into(),
                Series::new(vec![1i64, 2, 3]).into(),
                Series::new(vec![1i16, 2, 3]).into(),
            ],
            expect: Series::new(vec![4.0, 1.5, 0.6666666666666666]).into(),
            error: "",
        },
        Test {
            name: "mod-int64-passed",
            display: "modulo",
            arg_names: vec!["a", "b"],
            nullable: false,
            func: ArithmeticModuloFunction::try_create_func("")?,
            columns: vec![
                Series::new(vec![4i64, 3, 2]).into(),
                Series::new(vec![1i64, 2, 3]).into(),
                Series::new(vec![1i16, 2, 3]).into(),
            ],
            expect: Series::new(vec![0i64, 1, 2]).into(),
            error: "",
        },
    ];

    for t in tests {
        let rows = t.columns[0].len();
        let func = t.func;
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

        // Type check.
        let mut args = vec![];
        for name in t.arg_names {
            args.push(schema.field_with_name(name)?.data_type().clone());
        }

        let expect_type = func.return_type(&args)?.clone();
        let ref v = func.eval(&t.columns, rows)?;
        let actual_type = v.data_type().clone();
        assert_eq!(expect_type, actual_type);

        assert_eq!(v, &t.expect);
    }
    Ok(())
}
