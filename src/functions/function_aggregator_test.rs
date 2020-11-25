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

    let field_a = VariableFunction::create("a").unwrap();
    let field_b = VariableFunction::create("b").unwrap();

    let tests = vec![
        Test {
            name: "count-passed",
            args: vec![field_a.clone(), field_b.clone()],
            display: "CountAggregatorFunction",
            nullable: false,
            func: AggregatorFunction::create(
                "count",
                Arc::new(VariableFunction::create("a").unwrap()),
                &DataType::UInt64,
            )
            .unwrap(),
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                ],
            ),
            expect: DataValue::UInt64(Some(4)),
            error: "",
        },
        Test {
            name: "max-passed",
            args: vec![field_a.clone(), field_b.clone()],
            display: "MaxAggregatorFunction",
            nullable: false,
            func: AggregatorFunction::create(
                "max",
                Arc::new(VariableFunction::create("a").unwrap()),
                &DataType::Int64,
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
            args: vec![field_a.clone(), field_b.clone()],
            display: "MinAggregatorFunction",
            nullable: false,
            func: AggregatorFunction::create(
                "min",
                Arc::new(VariableFunction::create("a").unwrap()),
                &DataType::Int64,
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
            args: vec![field_a.clone(), field_b.clone()],
            display: "SumAggregatorFunction",
            nullable: false,
            func: AggregatorFunction::create(
                "sum",
                Arc::new(VariableFunction::create("a").unwrap()),
                &DataType::Int64,
            )
            .unwrap(),
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                ],
            ),
            expect: DataValue::Int64(Some(10)),
            error: "",
        },
    ];

    for mut t in tests {
        (t.func).accumulate(&t.block).unwrap();
        let result = (t.func).aggregate();
        match result {
            Ok(ref v) => {
                // Display check.
                let expect_display = t.display.to_string();
                let actual_display = format!("{:?}", (t.func));
                assert_eq!(expect_display, actual_display);

                // Nullable check.
                let expect_null = t.nullable;
                let actual_null = (t.func).nullable(t.block.schema())?;
                assert_eq!(expect_null, actual_null);

                // Type check.
                let expect_type = (t.func).return_type(t.block.schema())?;
                let actual_type = v.data_type();
                assert_eq!(expect_type, actual_type);

                assert_eq!(&t.expect, v);
            }
            Err(e) => assert_eq!(t.error, e.to_string()),
        }
    }
    Ok(())
}
