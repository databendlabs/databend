// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::*;
use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::comparisons::*;
use crate::*;

#[test]
fn test_comparison_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        arg_names: Vec<&'static str>,
        columns: Vec<DataColumnarValue>,
        expect: DataArrayRef,
        error: &'static str,
        func: Box<dyn Function>,
    }

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Int64, false),
    ]);

    let tests = vec![
        Test {
            name: "eq-passed",
            display: "=",
            nullable: false,
            func: ComparisonEqFunction::try_create_func("")?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Arc::new(Int64Array::from(vec![4, 3, 2, 4])).into(),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])).into(),
            ],
            expect: Arc::new(BooleanArray::from(vec![false, false, false, true])),
            error: "",
        },
        Test {
            name: "gt-passed",
            display: ">",
            nullable: false,
            func: ComparisonGtFunction::try_create_func("")?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Arc::new(Int64Array::from(vec![4, 3, 2, 4])).into(),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])).into(),
            ],
            expect: Arc::new(BooleanArray::from(vec![true, true, false, false])),
            error: "",
        },
        Test {
            name: "gt-eq-passed",
            display: ">=",
            nullable: false,
            func: ComparisonGtEqFunction::try_create_func("")?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Arc::new(Int64Array::from(vec![4, 3, 2, 4])).into(),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])).into(),
            ],
            expect: Arc::new(BooleanArray::from(vec![true, true, false, true])),
            error: "",
        },
        Test {
            name: "lt-passed",
            display: "<",
            nullable: false,
            func: ComparisonLtFunction::try_create_func("")?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Arc::new(Int64Array::from(vec![4, 3, 2, 4])).into(),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])).into(),
            ],
            expect: Arc::new(BooleanArray::from(vec![false, false, true, false])),
            error: "",
        },
        Test {
            name: "lt-eq-passed",
            display: "<=",
            nullable: false,
            func: ComparisonLtEqFunction::try_create_func("")?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Arc::new(Int64Array::from(vec![4, 3, 2, 4])).into(),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])).into(),
            ],
            expect: Arc::new(BooleanArray::from(vec![false, false, true, true])),
            error: "",
        },
        Test {
            name: "not-eq-passed",
            display: "!=",
            nullable: false,
            func: ComparisonNotEqFunction::try_create_func("")?,
            arg_names: vec!["a", "b"],
            columns: vec![
                Arc::new(Int64Array::from(vec![4, 3, 2, 4])).into(),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])).into(),
            ],
            expect: Arc::new(BooleanArray::from(vec![true, true, true, false])),
            error: "",
        },
    ];

    for t in tests {
        let rows = t.columns[0].len();
        let func = t.func;
        if let Err(e) = func.eval(&t.columns, rows) {
            assert_eq!(t.error, e.to_string());
        }
        func.eval(&t.columns, rows)?;

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        // Type check.
        let mut args = vec![];
        for name in t.arg_names {
            args.push(schema.field_with_name(name)?.data_type().clone());
        }

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable(&schema)?;
        assert_eq!(expect_null, actual_null);

        let ref v = func.eval(&t.columns, rows)?;
        // Type check.
        let expect_type = func.return_type(&args)?;
        let actual_type = v.data_type();
        assert_eq!(expect_type, actual_type);

        assert_eq!(v.to_array()?.as_ref(), t.expect.as_ref());
    }
    Ok(())
}
