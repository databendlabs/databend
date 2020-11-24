// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::datablocks::DataBlock;
use crate::datavalues::DataArrayRef;
use crate::functions::Function;

#[allow(dead_code)]
struct Test {
    name: &'static str,
    is_aggregate: bool,
    fun: &'static str,
    args: Vec<Function>,
    block: DataBlock,
    expect: DataArrayRef,
    error: &'static str,
}

#[test]
fn test_factory() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;

    use crate::datavalues::*;
    use crate::functions::*;

    let schema = Arc::new(DataSchema::new(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Int64, false),
    ]));

    let field_a = VariableFunction::create("a").unwrap();
    let field_b = VariableFunction::create("b").unwrap();

    let tests = vec![
        Test {
            name: "add-function-passed",
            is_aggregate: false,
            fun: "+",
            args: vec![field_a.clone(), field_b.clone()],
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                ],
            ),
            expect: Arc::new(Int64Array::from(vec![5, 5, 5, 5])),
            error: "",
        },
        Test {
            name: "sub-function-passed",
            is_aggregate: false,
            fun: "-",
            args: vec![field_a.clone(), field_b.clone()],
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                ],
            ),
            expect: Arc::new(Int64Array::from(vec![3, 1, -1, -3])),
            error: "",
        },
        Test {
            name: "mul-function-passed",
            is_aggregate: false,
            fun: "*",
            args: vec![field_a.clone(), field_b.clone()],
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                ],
            ),
            expect: Arc::new(Int64Array::from(vec![4, 6, 6, 4])),
            error: "",
        },
        Test {
            name: "div-function-passed",
            is_aggregate: false,
            fun: "/",
            args: vec![field_a.clone(), field_b.clone()],
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                ],
            ),
            expect: Arc::new(Int64Array::from(vec![4, 1, 0, 0])),
            error: "",
        },
        Test {
            name: "count-function-passed",
            is_aggregate: true,
            fun: "count",
            args: vec![field_a.clone(), field_b.clone()],
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
    ];
    for t in tests {
        if !t.is_aggregate {
            let fun = ScalarFunctionFactory::get(t.fun, &*t.args)?;
            let result = fun.evaluate(&t.block)?.to_array(0);
            match result {
                Ok(ref v) => {
                    // Result check.
                    if !v.equals(&*t.expect) {
                        println!("expect:\n{:?} \nactual:\n{:?}", t.expect, v);
                        assert!(false);
                    }
                }
                Err(e) => assert_eq!(t.error, e.to_string()),
            }
        } else {
            let mut fun = AggregateFunctionFactory::get(
                t.fun,
                Arc::new(VariableFunction::create("a")?),
                &DataType::Int64,
            )
            .unwrap();
            fun.accumulate(&t.block).unwrap();
            let result = fun.aggregate()?;
            if !result.to_array(1)?.equals(&*t.expect) {
                println!("expect:\n{:?} \nactual:\n{:?}", t.expect, result);
                assert!(false);
            }
        }
    }
    Ok(())
}
