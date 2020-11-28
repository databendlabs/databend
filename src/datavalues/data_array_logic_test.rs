// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_array_logic() {
    use std::sync::Arc;

    use super::*;

    #[allow(dead_code)]
    struct ArrayTest {
        name: &'static str,
        args: Vec<Vec<DataArrayRef>>,
        expect: Vec<DataArrayRef>,
        error: Vec<&'static str>,
        op: DataValueLogicOperator,
    }

    let tests = vec![
        ArrayTest {
            name: "and-passed",
            args: vec![vec![
                Arc::new(BooleanArray::from(vec![true, true])),
                Arc::new(BooleanArray::from(vec![true, false])),
            ]],
            op: DataValueLogicOperator::And,
            expect: vec![Arc::new(BooleanArray::from(vec![true, false]))],
            error: vec![""],
        },
        ArrayTest {
            name: "or-passed",
            args: vec![vec![
                Arc::new(BooleanArray::from(vec![true, true])),
                Arc::new(BooleanArray::from(vec![true, false])),
            ]],
            op: DataValueLogicOperator::Or,
            expect: vec![Arc::new(BooleanArray::from(vec![true, true]))],
            error: vec![""],
        },
    ];

    for t in tests {
        for (i, args) in t.args.iter().enumerate() {
            let result = data_array_logic_op(
                t.op.clone(),
                &DataColumnarValue::Array(args[0].clone()),
                &DataColumnarValue::Array(args[1].clone()),
            );
            match result {
                Ok(ref v) => {
                    // Result check.
                    if !v.equals(&*t.expect[i]) {
                        println!("{}, expect:\n{:?} \nactual:\n{:?}", t.name, t.expect[i], v);
                        assert!(false);
                    }
                }
                Err(e) => assert_eq!(t.error[i], e.to_string()),
            }
        }
    }
}
