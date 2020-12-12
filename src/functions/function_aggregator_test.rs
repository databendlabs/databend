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
    let block = DataBlock::create(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
        ],
    );

    let field_a = FieldFunction::try_create("a")?;
    let field_b = FieldFunction::try_create("b")?;

    let tests = vec![
        Test {
            name: "count-passed",
            evals: 1,
            args: vec![field_a.clone(), field_b.clone()],
            display: "Count(a)",
            nullable: false,
            func: AggregatorFunction::try_create(
                DataValueAggregateOperator::Count,
                &[FieldFunction::try_create("a")?],
            )?,
            block: block.clone(),
            expect: DataValue::UInt64(Some(4)),
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
                &[FieldFunction::try_create("a")?],
            )?,
            block: block.clone(),
            expect: DataValue::Int64(Some(4)),
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
                &[FieldFunction::try_create("a")?],
            )?,
            block: block.clone(),
            expect: DataValue::Int64(Some(1)),
            error: "",
        },
        Test {
            name: "sum-passed",
            evals: 1,
            args: vec![field_a.clone(), field_b.clone()],
            display: "Sum(a)",
            nullable: false,
            func: AggregatorFunction::try_create(
                DataValueAggregateOperator::Sum,
                &[FieldFunction::try_create("a")?],
            )?,
            block: block.clone(),
            expect: DataValue::Int64(Some(10)),
            error: "",
        },
        Test {
            name: "sum(a)+1-merge-passed",
            evals: 4,
            args: vec![field_a.clone(), field_b.clone()],
            display: "Sum(a) + 1",
            nullable: false,
            func: ArithmeticFunction::try_create(
                DataValueArithmeticOperator::Add,
                &[
                    AggregatorFunction::try_create(
                        DataValueAggregateOperator::Sum,
                        &[FieldFunction::try_create("a")?],
                    )?,
                    ConstantFunction::try_create(DataValue::Int64(Some(1)))?,
                ],
            )?,
            block: block.clone(),
            expect: DataValue::Int64(Some(71)),
            error: "",
        },
        Test {
            name: "sum(a)/count(a)-merge-passed",
            evals: 4,
            args: vec![field_a.clone(), field_b.clone()],
            display: "Sum(a)/Count(a)",
            nullable: false,
            func: ArithmeticFunction::try_create(
                DataValueArithmeticOperator::Div,
                &[
                    AggregatorFunction::try_create(
                        DataValueAggregateOperator::Sum,
                        &[FieldFunction::try_create("a")?],
                    )?,
                    AggregatorFunction::try_create(
                        DataValueAggregateOperator::Count,
                        &[FieldFunction::try_create("a")?],
                    )?,
                ],
            )?,
            block,
            expect: DataValue::Int64(Some(2)),
            error: "",
        },
    ];

    for t in tests {
        let mut func1 = t.func.clone();
        for _ in 0..t.evals {
            func1.accumulate(&t.block)?;
        }
        let state1 = func1.accumulate_result()?;

        let mut func2 = t.func.clone();
        for _ in 1..t.evals {
            func2.accumulate(&t.block)?;
        }
        let state2 = func2.accumulate_result()?;

        let mut final_func = t.func.clone();
        final_func.set_depth(0);
        final_func.merge_state(&*state1)?;
        final_func.merge_state(&*state2)?;

        let result = final_func.merge_result()?;

        assert_eq!(&t.expect, &result);
    }
    Ok(())
}
