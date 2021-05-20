// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::ArrayRef;
use common_exception::Result;

#[test]
fn test_arithmetic_function() -> Result<()> {
    use std::sync::Arc;

    use common_datablocks::DataBlock;
    use common_datavalues::*;
    use pretty_assertions::assert_eq;

    use crate::arithmetics::*;
    use crate::*;

    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        nullable: bool,
        arg_names: Vec<&'static str>,
        columns: Vec<DataColumnarValue>,
        expect: DataArrayRef,
        error: &'static str,
        func: Box<dyn IFunction>
    }

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Int64, false),
        DataField::new("c", DataType::Int16, false),
    ]);

    let field_a = ColumnFunction::try_create("a")?;
    let field_b = ColumnFunction::try_create("b")?;
    let field_c = ColumnFunction::try_create("c")?;

    let tests = vec![
        Test {
            name: "add-int64-passed",
            display: "plus(a, b)",
            nullable: false,
            arg_names: vec!["a", "b"],
            func: ArithmeticPlusFunction::try_create_func("")?,
            columns: vec![
                ((Arc::new(Int64Array::from(vec![4, 3, 2, 1]))) as ArrayRef).into(),
                ((Arc::new(Int64Array::from(vec![1, 2, 3, 4]))) as ArrayRef).into(),
                ((Arc::new(Int64Array::from(vec![1, 2, 3, 4]))) as ArrayRef).into(),
            ],
            expect: Arc::new(Int64Array::from(vec![5, 5, 5, 5])),
            error: ""
        },
        Test {
            name: "add-diff-passed",
            display: "plus(c, a)",
            nullable: false,
            arg_names: vec!["c", "b"],
            func: ArithmeticPlusFunction::try_create_func("")?,
            columns: vec![
                ((Arc::new(Int64Array::from(vec![4, 3, 2, 1]))) as ArrayRef).into(),
                ((Arc::new(Int64Array::from(vec![1, 2, 3, 4]))) as ArrayRef).into(),
                ((Arc::new(Int64Array::from(vec![1, 2, 3, 4]))) as ArrayRef).into(),
            ],
            expect: Arc::new(Int64Array::from(vec![5, 5, 5, 5])),
            error: ""
        },
        Test {
            name: "sub-int64-passed",
            display: "minus(a, b)",
            arg_names: vec!["a", "b"],
            nullable: false,
            func: ArithmeticMinusFunction::try_create_func("")?,
            columns: vec![
                ((Arc::new(Int64Array::from(vec![4, 3, 2]))) as ArrayRef).into(),
                ((Arc::new(Int64Array::from(vec![1, 2, 3]))) as ArrayRef).into(),
                ((Arc::new(Int64Array::from(vec![1, 2, 3]))) as ArrayRef).into(),
            ],
            expect: Arc::new(Int64Array::from(vec![3, 1, -1])),
            error: ""
        },
        Test {
            name: "mul-int64-passed",
            display: "multiply(a, b)",
            arg_names: vec!["a", "b"],
            nullable: false,
            func: ArithmeticMulFunction::try_create_func("")?,
            columns: vec![
                ((Arc::new(Int64Array::from(vec![4, 3, 2]))) as ArrayRef).into(),
                ((Arc::new(Int64Array::from(vec![1, 2, 3]))) as ArrayRef).into(),
                ((Arc::new(Int64Array::from(vec![1, 2, 3]))) as ArrayRef).into(),
            ],
            expect: Arc::new(Int64Array::from(vec![4, 6, 6])),
            error: ""
        },
        Test {
            name: "div-int64-passed",
            display: "divide(a, b)",
            arg_names: vec!["a", "b"],
            nullable: false,
            func: ArithmeticDivFunction::try_create_func("")?,
            columns: vec![
                ((Arc::new(Int64Array::from(vec![4, 3, 2]))) as ArrayRef).into(),
                ((Arc::new(Int64Array::from(vec![1, 2, 3]))) as ArrayRef).into(),
                ((Arc::new(Int64Array::from(vec![1, 2, 3]))) as ArrayRef).into(),
            ],
            expect: Arc::new(Float64Array::from(vec![4.0, 1.5, 0.6666666666666666])),
            error: ""
        },
        Test {
            name: "mod-int64-passed",
            display: "modulo(a, b)",
            arg_names: vec!["a", "b"],
            nullable: false,
            func: ArithmeticModuloFunction::try_create_func("")?,
            columns: vec![
                ((Arc::new(Int64Array::from(vec![4, 3, 2]))) as ArrayRef).into(),
                ((Arc::new(Int64Array::from(vec![1, 2, 3]))) as ArrayRef).into(),
                ((Arc::new(Int64Array::from(vec![1, 2, 3]))) as ArrayRef).into(),
            ],
            expect: Arc::new(Int64Array::from(vec![0, 1, 2])),
            error: ""
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

        let args = t
            .arg_names
            .iter()
            .map(|name| schema.field_with_name(name).map(|f| f.data_type().clone()))
            .collect::<Result<Vec<DataType>>>()?;

        let expect_type = func.return_type(&args)?.clone();
        let ref v = func.eval(&t.columns, rows)?;
        let actual_type = v.data_type().clone();
        assert_eq!(expect_type, actual_type);

        assert_eq!(v.to_array()?.as_ref(), t.expect.as_ref());
    }
    Ok(())
}
