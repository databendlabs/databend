// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_array_logic() {
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    use crate::*;

    #[allow(dead_code)]
    struct ArrayTest {
        name: &'static str,
        args: Vec<Vec<Series>>,
        expect: Vec<Series>,
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
        ArrayTest {
            name: "not-passed",
            args: vec![vec![Arc::new(BooleanArray::from(vec![true, false]))]],
            op: DataValueLogicOperator::Not,
            expect: vec![Arc::new(BooleanArray::from(vec![false, true]))],
            error: vec![""],
        },
    ];

    for t in tests {
        for (i, args) in t.args.iter().enumerate() {
            let arrays = args
                .iter()
                .map(|x| DataColumnarValue::Array(x.clone()))
                .collect::<Vec<_>>();

            let result = DataArrayLogic::data_array_logic_op(t.op.clone(), &arrays);
            match result {
                Ok(v) => assert_eq!(v.as_ref(), t.expect[i].as_ref()),
                Err(e) => assert_eq!(t.error[i], e.to_string()),
            }
        }
    }
}
