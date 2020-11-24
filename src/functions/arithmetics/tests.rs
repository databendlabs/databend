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
fn test_cases() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;

    use crate::datavalues::*;
    use crate::functions::{arithmetics::*, *};

    let schema = Arc::new(DataSchema::new(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Int64, false),
    ]));

    let field_a = VariableFunction::create("a").unwrap();
    let field_b = VariableFunction::create("b").unwrap();

    let tests = vec![
        Test {
            name: "add-int64-passed",
            args: vec![field_a.clone(), field_b.clone()],
            display: "\"a\" + \"b\"",
            nullable: false,
            func: AddFunction::create(&[field_a.clone(), field_b.clone()]).unwrap(),
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
            name: "sub-int64-passed",
            args: vec![field_a.clone(), field_b.clone()],
            display: "\"a\" - \"b\"",
            nullable: false,
            func: SubFunction::create(&[field_a.clone(), field_b.clone()]).unwrap(),
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2])),
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                ],
            ),
            expect: Arc::new(Int64Array::from(vec![3, 1, -1])),
            error: "",
        },
        Test {
            name: "mul-int64-passed",
            args: vec![field_a.clone(), field_b.clone()],
            display: "\"a\" * \"b\"",
            nullable: false,
            func: MulFunction::create(&[field_a.clone(), field_b.clone()]).unwrap(),
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2])),
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                ],
            ),
            expect: Arc::new(Int64Array::from(vec![4, 6, 6])),
            error: "",
        },
        Test {
            name: "div-int64-passed",
            args: vec![field_a.clone(), field_b.clone()],
            display: "\"a\" / \"b\"",
            nullable: false,
            func: DivFunction::create(&[field_a.clone(), field_b.clone()]).unwrap(),
            block: DataBlock::create(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2])),
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                ],
            ),
            expect: Arc::new(Int64Array::from(vec![4, 1, 0])),
            error: "",
        },
    ];

    for t in tests {
        let result = (t.func).evaluate(&t.block);
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
                let expect_type = (t.func).return_type(t.block.schema())?.clone();
                let actual_type = v.data_type().clone();
                assert_eq!(expect_type, actual_type);

                if !v.to_array(0)?.equals(&*t.expect) {
                    println!("expect:\n{:?} \nactual:\n{:?}", t.expect, v);
                    assert!(false);
                }
            }
            Err(e) => assert_eq!(t.error, e.to_string()),
        }
    }
    Ok(())
}
