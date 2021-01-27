// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_arithmetic_function() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;

    use crate::datablocks::DataBlock;
    use crate::datavalues::*;
    use crate::functions::*;

    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        evals: usize,
        display: &'static str,
        nullable: bool,
        block: DataBlock,
        expect: DataArrayRef,
        error: &'static str,
        func: Function,
    }

    let schema = Arc::new(DataSchema::new(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Int64, false),
        DataField::new("c", DataType::Int16, false),
    ]));

    let field_a = FieldFunction::try_create("a")?;
    let field_b = FieldFunction::try_create("b")?;
    let field_c = FieldFunction::try_create("c")?;

    let tests = vec![
        Test {
            name: "add-int64-passed",
            evals: 2,
            display: "(a + b)",
            nullable: false,
            func: ArithmeticFunction::try_create_add_func(&[field_a.clone(), field_b.clone()])?,
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                    Arc::new(Int16Array::from(vec![1, 2, 3, 4])),
                ],
            ),
            expect: Arc::new(Int64Array::from(vec![5, 5, 5, 5])),
            error: "",
        },
        Test {
            name: "add-diff-passed",
            evals: 2,
            display: "(c + a)",
            nullable: false,
            func: ArithmeticFunction::try_create_add_func(&[field_c.clone(), field_a.clone()])?,
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int16Array::from(vec![1, 2, 3, 4])),
                    Arc::new(Int16Array::from(vec![1, 2, 3, 4])),
                ],
            ),
            expect: Arc::new(Int64Array::from(vec![5, 5, 5, 5])),
            error: "",
        },
        Test {
            name: "sub-int64-passed",
            evals: 2,
            display: "(a - b)",
            nullable: false,
            func: ArithmeticFunction::try_create_sub_func(&[field_a.clone(), field_b.clone()])?,
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2])),
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(Int16Array::from(vec![1, 2, 3])),
                ],
            ),
            expect: Arc::new(Int64Array::from(vec![3, 1, -1])),
            error: "",
        },
        Test {
            name: "mul-int64-passed",
            evals: 2,
            display: "(a * b)",
            nullable: false,
            func: ArithmeticFunction::try_create_mul_func(&[field_a.clone(), field_b.clone()])?,
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2])),
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(Int16Array::from(vec![1, 2, 3])),
                ],
            ),
            expect: Arc::new(Int64Array::from(vec![4, 6, 6])),
            error: "",
        },
        Test {
            name: "div-int64-passed",
            evals: 2,
            display: "(a / b)",
            nullable: false,
            func: ArithmeticFunction::try_create_div_func(&[field_a.clone(), field_b.clone()])?,
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2])),
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(Int16Array::from(vec![1, 2, 3])),
                ],
            ),
            expect: Arc::new(Float64Array::from(vec![4.0, 1.5, 0.6666666666666666])),
            error: "",
        },
    ];

    for t in tests {
        let mut func = t.func;
        if let Err(e) = func.eval(&t.block) {
            assert_eq!(t.error, e.to_string());
        }

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{:?}", func);
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable(t.block.schema())?;
        assert_eq!(expect_null, actual_null);

        let expect_type = func.return_type(t.block.schema())?.clone();
        let ref v = func.eval(&t.block)?;
        let actual_type = v.data_type().clone();
        assert_eq!(expect_type, actual_type);

        assert_eq!(v.to_array(0)?.as_ref(), t.expect.as_ref());
    }
    Ok(())
}
