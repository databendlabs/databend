// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::datablocks::DataBlock;
use crate::datavalues::DataArrayRef;
use crate::functions::Function;

#[allow(dead_code)]
struct Test {
    name: &'static str,
    args: Vec<Function>,
    display: &'static str,
    nullable: bool,
    block: DataBlock,
    expect: DataArrayRef,
    error: &'static str,
    func: Function,
}

#[test]
fn test_cases() {
    use std::sync::Arc;

    use crate::datavalues::{DataField, DataSchema, DataType, Int64Array, UInt64Array};
    use crate::functions::{aggregators::AggregatorFunction, VariableFunction};

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
            expect: Arc::new(UInt64Array::from(vec![4])),
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
            expect: Arc::new(Int64Array::from(vec![14])),
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
            expect: Arc::new(Int64Array::from(vec![-2])),
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
            expect: Arc::new(Int64Array::from(vec![10])),
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
                let actual_null = (t.func).nullable(t.block.schema()).unwrap();
                assert_eq!(expect_null, actual_null);

                // Type check.
                let expect_type = &(t.func).return_type(t.block.schema()).unwrap();
                let actual_type = v.data_type();
                assert_eq!(expect_type, actual_type);

                // Result check.
                if !v.equals(&*t.expect) {
                    println!("expect:\n{:?} \nactual:\n{:?}", t.expect, v);
                    assert!(false);
                }
            }
            Err(e) => assert_eq!(t.error, e.to_string()),
        }
    }
}
