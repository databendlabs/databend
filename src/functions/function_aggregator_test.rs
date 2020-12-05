// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_aggregator_function() -> crate::error::FuseQueryResult<()> {
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
        expect: DataValue,
        error: &'static str,
        func: Function,
    }

    let schema = Arc::new(DataSchema::new(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Int64, false),
    ]));

    let field_a = FieldFunction::try_create("a").unwrap();
    let field_b = FieldFunction::try_create("b").unwrap();

    let tests = vec![
        Test {
            name: "count-passed",
            evals: 2,
            args: vec![field_a.clone(), field_b.clone()],
            display: "Count(a)",
            nullable: false,
            func: AggregatorFunction::try_create(
                DataValueAggregateOperator::Count,
                &[FieldFunction::try_create("a").unwrap()],
            )
            .unwrap(),
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                ],
            ),
            expect: DataValue::UInt64(Some(8)),
            error: "",
        },
        Test {
            name: "max-passed",
            evals: 2,
            args: vec![field_a.clone(), field_b.clone()],
            display: "Max(a)",
            nullable: false,
            func: AggregatorFunction::try_create(
                DataValueAggregateOperator::Max,
                &[FieldFunction::try_create("a").unwrap()],
            )
            .unwrap(),
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![14, 3, 2, 1])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                ],
            ),
            expect: DataValue::Int64(Some(14)),
            error: "",
        },
        Test {
            name: "min-passed",
            evals: 2,
            args: vec![field_a.clone(), field_b.clone()],
            display: "Min(a)",
            nullable: false,
            func: AggregatorFunction::try_create(
                DataValueAggregateOperator::Min,
                &[FieldFunction::try_create("a").unwrap()],
            )
            .unwrap(),
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![14, 3, -2, 1])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                ],
            ),
            expect: DataValue::Int64(Some(-2)),
            error: "",
        },
        Test {
            name: "sum-passed",
            evals: 2,
            args: vec![field_a.clone(), field_b.clone()],
            display: "Sum(a)",
            nullable: false,
            func: AggregatorFunction::try_create(
                DataValueAggregateOperator::Sum,
                &[FieldFunction::try_create("a").unwrap()],
            )
            .unwrap(),
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                ],
            ),
            expect: DataValue::Int64(Some(20)),
            error: "",
        },
        Test {
            name: "sum(b+1)-passed",
            evals: 1,
            args: vec![field_a.clone(), field_b.clone()],
            display: "Sum(b + 1)",
            nullable: false,
            func: AggregatorFunction::try_create(
                DataValueAggregateOperator::Sum,
                &[ArithmeticFunction::try_create(
                    DataValueArithmeticOperator::Add,
                    &[
                        FieldFunction::try_create("b").unwrap(),
                        ConstantFunction::try_create(DataValue::Int64(Some(1))).unwrap(),
                    ],
                )
                .unwrap()],
            )
            .unwrap(),
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                ],
            ),
            expect: DataValue::Int64(Some(14)),
            error: "",
        },
    ];

    for mut t in tests {
        if let Err(e) = (t.func).eval(&t.block) {
            assert_eq!(t.error, e.to_string());
        }
        for _ in 1..t.evals {
            (t.func).eval(&t.block)?;
        }


        // Display check.
        let expect_display = t.display.to_string();
        let actual_display = format!("{:?}", (t.func));
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = (t.func).nullable(t.block.schema())?;
        assert_eq!(expect_null, actual_null);

        if let DataColumnarValue::Scalar(ref v) = (t.func).result()? {
            // Type check.
            let expect_type = (t.func).return_type(t.block.schema())?;
            let actual_type = v.data_type();
            assert_eq!(expect_type, actual_type);

            assert_eq!(&t.expect, v);
        }
    }
    Ok(())
}
