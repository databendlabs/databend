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
        args: Vec<Function>,
        display: &'static str,
        nullable: bool,
        block: DataBlock,
        expect: DataArrayRef,
        error: &'static str,
        op: DataValueArithmeticOperator,
    }

    let schema = Arc::new(DataSchema::new(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Int64, false),
        DataField::new("c", DataType::Int16, false),
    ]));

    let field_a = VariableFunction::try_create("a")?;
    let field_b = VariableFunction::try_create("b")?;
    let field_c = VariableFunction::try_create("c")?;

    let tests = vec![
        Test {
            name: "add-int64-passed",
            evals: 2,
            args: vec![field_a.clone(), field_b.clone()],
            display: "a + b",
            nullable: false,
            op: DataValueArithmeticOperator::Add,
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
            args: vec![field_c.clone(), field_a.clone()],
            display: "c + a",
            nullable: false,
            op: DataValueArithmeticOperator::Add,
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
            args: vec![field_a.clone(), field_b.clone()],
            display: "a - b",
            nullable: false,
            op: DataValueArithmeticOperator::Sub,
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
            args: vec![field_a.clone(), field_b.clone()],
            display: "a * b",
            nullable: false,
            op: DataValueArithmeticOperator::Mul,
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
            args: vec![field_a.clone(), field_b.clone()],
            display: "a / b",
            nullable: false,
            op: DataValueArithmeticOperator::Div,
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2])),
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(Int16Array::from(vec![1, 2, 3])),
                ],
            ),
            expect: Arc::new(Int64Array::from(vec![4, 1, 0])),
            error: "",
        },
        Test {
            name: "(sum(a)+1)-passed",
            evals: 3,
            args: vec![
                ConstantFunction::try_create(DataValue::Int64(Some(1))).unwrap(),
                AggregatorFunction::try_create(
                    DataValueAggregateOperator::Sum,
                    &[VariableFunction::try_create("a").unwrap()],
                )
                .unwrap(),
            ],
            display: "1 + Sum(a)",
            nullable: false,
            op: DataValueArithmeticOperator::Add,
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                    Arc::new(Int16Array::from(vec![1, 2, 3, 4])),
                ],
            ),
            expect: Arc::new(Int64Array::from(vec![31])),
            error: "",
        },
    ];

    for t in tests {
        let mut func =
            ArithmeticFunction::try_create(t.op, &[t.args[0].clone(), t.args[1].clone()])?;
        if let Err(e) = func.eval(&t.block) {
            assert_eq!(t.error, e.to_string());
        }
        for _ in 1..t.evals {
            func.eval(&t.block)?;
        }

        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{:?}", func);
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable(t.block.schema())?;
        assert_eq!(expect_null, actual_null);

        let ref v = func.result()?;
        // Type check.
        let expect_type = func.return_type(t.block.schema())?.clone();
        let actual_type = v.data_type().clone();
        assert_eq!(expect_type, actual_type);

        if !v.to_array(0)?.equals(&*t.expect) {
            println!("{}, expect:\n{:?} \nactual:\n{:?}", t.name, t.expect, v);
            assert!(false);
        }
    }
    Ok(())
}
