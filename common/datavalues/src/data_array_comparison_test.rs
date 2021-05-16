// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_array_comparison() {
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    use super::*;

    #[allow(dead_code)]
    struct ArrayTest {
        name: &'static str,
        args: Vec<Vec<DataArrayRef>>,
        expect: Vec<DataArrayRef>,
        error: Vec<&'static str>,
        op: DataValueComparisonOperator
    }

    let tests = vec![
        ArrayTest {
            name: "eq-passed",
            args: vec![
                vec![
                    Arc::new(StringArray::from(vec!["x1", "x2"])),
                    Arc::new(StringArray::from(vec!["x2", "x2"])),
                ],
                vec![
                    Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
                ],
                vec![
                    Arc::new(Int16Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int16Array::from(vec![1, 2, 2, 4])),
                ],
                vec![
                    Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int32Array::from(vec![4, 3, 2, 1])),
                ],
            ],
            op: DataValueComparisonOperator::Eq,
            expect: vec![
                Arc::new(BooleanArray::from(vec![false, true])),
                Arc::new(BooleanArray::from(vec![true, true, true, true])),
                Arc::new(BooleanArray::from(vec![false, false, true, false])),
                Arc::new(BooleanArray::from(vec![true, true, true, true])),
            ],
            error: vec![""]
        },
        ArrayTest {
            name: "lt-passed",
            args: vec![vec![
                Arc::new(Int8Array::from(vec![4, 3, 1, 1])),
                Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
            ]],
            op: DataValueComparisonOperator::Lt,
            expect: vec![Arc::new(BooleanArray::from(vec![
                false, false, true, false,
            ]))],
            error: vec![""]
        },
        ArrayTest {
            name: "lt-eq-passed",
            args: vec![vec![
                Arc::new(Int8Array::from(vec![4, 3, 1, 2])),
                Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
            ]],
            op: DataValueComparisonOperator::LtEq,
            expect: vec![Arc::new(BooleanArray::from(vec![true, true, true, false]))],
            error: vec![""]
        },
        ArrayTest {
            name: "gt-passed",
            args: vec![vec![
                Arc::new(Int8Array::from(vec![4, 3, 1, 2])),
                Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
            ]],
            op: DataValueComparisonOperator::Gt,
            expect: vec![Arc::new(BooleanArray::from(vec![
                false, false, false, true,
            ]))],
            error: vec![""]
        },
        ArrayTest {
            name: "gt-eq-passed",
            args: vec![vec![
                Arc::new(Int8Array::from(vec![4, 3, 1, 2])),
                Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
            ]],
            op: DataValueComparisonOperator::GtEq,
            expect: vec![Arc::new(BooleanArray::from(vec![true, true, false, true]))],
            error: vec![""]
        },
        ArrayTest {
            name: "not-eq-passed",
            args: vec![vec![
                Arc::new(Int8Array::from(vec![4, 3, 1, 2])),
                Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
            ]],
            op: DataValueComparisonOperator::NotEq,
            expect: vec![Arc::new(BooleanArray::from(vec![false, false, true, true]))],
            error: vec![""]
        },
    ];

    for t in tests {
        for (i, args) in t.args.iter().enumerate() {
            let result = DataArrayComparison::data_array_comparison_op(
                t.op.clone(),
                &DataColumnarValue::Array(args[0].clone()),
                &DataColumnarValue::Array(args[1].clone())
            );
            match result {
                Ok(v) => assert_eq!(v.as_ref(), t.expect[i].as_ref()),
                Err(e) => assert_eq!(t.error[i], e.to_string())
            }
        }
    }
}

#[test]
fn test_array_scalar_comparison() {
    use std::sync::Arc;

    use super::*;

    #[allow(dead_code)]
    struct ArrayTest {
        name: &'static str,
        array: DataArrayRef,
        scalar: DataValue,
        expect: DataArrayRef,
        error: &'static str,
        op: DataValueComparisonOperator
    }

    let tests = vec![
        ArrayTest {
            name: "eq-passed",
            array: Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
            scalar: DataValue::Int8(Some(3)),
            op: DataValueComparisonOperator::Eq,
            expect: Arc::new(BooleanArray::from(vec![false, true, false, false])),
            error: ""
        },
        ArrayTest {
            name: "eq-different-type-passed",
            array: Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
            scalar: DataValue::Int32(Some(3)),
            op: DataValueComparisonOperator::Eq,
            expect: Arc::new(BooleanArray::from(vec![false, true, false, false])),
            error: ""
        },
        ArrayTest {
            name: "lt-passed",
            array: Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
            scalar: DataValue::Int8(Some(3)),
            op: DataValueComparisonOperator::Lt,
            expect: Arc::new(BooleanArray::from(vec![false, false, true, true])),
            error: ""
        },
        ArrayTest {
            name: "lt-eq-passed",
            array: Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
            scalar: DataValue::Int8(Some(3)),
            op: DataValueComparisonOperator::LtEq,
            expect: Arc::new(BooleanArray::from(vec![false, true, true, true])),
            error: ""
        },
        ArrayTest {
            name: "gt-passed",
            array: Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
            scalar: DataValue::Int8(Some(3)),
            op: DataValueComparisonOperator::Gt,
            expect: Arc::new(BooleanArray::from(vec![true, false, false, false])),
            error: ""
        },
        ArrayTest {
            name: "gt-eq-passed",
            array: Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
            scalar: DataValue::Int8(Some(3)),
            op: DataValueComparisonOperator::GtEq,
            expect: Arc::new(BooleanArray::from(vec![true, true, false, false])),
            error: ""
        },
    ];

    for t in tests {
        let result = DataArrayComparison::data_array_comparison_op(
            t.op.clone(),
            &DataColumnarValue::Array(t.array),
            &DataColumnarValue::Constant(t.scalar, t.array.len())
        );
        match result {
            Ok(v) => assert_eq!(v.as_ref(), t.expect.as_ref()),
            Err(e) => assert_eq!(t.error, e.to_string())
        }
    }
}

#[test]
fn test_scalar_array_comparison() {
    use std::sync::Arc;

    use super::*;

    #[allow(dead_code)]
    struct ArrayTest {
        name: &'static str,
        array: DataArrayRef,
        scalar: DataValue,
        expect: DataArrayRef,
        error: &'static str,
        op: DataValueComparisonOperator
    }

    let tests = vec![
        ArrayTest {
            name: "eq-passed",
            array: Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
            scalar: DataValue::Int8(Some(3)),
            op: DataValueComparisonOperator::Eq,
            expect: Arc::new(BooleanArray::from(vec![false, true, false, false])),
            error: ""
        },
        ArrayTest {
            name: "eq-different-type-passed",
            array: Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
            scalar: DataValue::Int32(Some(3)),
            op: DataValueComparisonOperator::Eq,
            expect: Arc::new(BooleanArray::from(vec![false, true, false, false])),
            error: ""
        },
        ArrayTest {
            name: "lt-passed",
            array: Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
            scalar: DataValue::Int8(Some(3)),
            op: DataValueComparisonOperator::Lt,
            expect: Arc::new(BooleanArray::from(vec![true, false, false, false])),
            error: ""
        },
        ArrayTest {
            name: "lt-eq-passed",
            array: Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
            scalar: DataValue::Int8(Some(3)),
            op: DataValueComparisonOperator::LtEq,
            expect: Arc::new(BooleanArray::from(vec![true, true, false, false])),
            error: ""
        },
        ArrayTest {
            name: "gt-passed",
            array: Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
            scalar: DataValue::Int8(Some(3)),
            op: DataValueComparisonOperator::Gt,
            expect: Arc::new(BooleanArray::from(vec![false, false, true, true])),
            error: ""
        },
        ArrayTest {
            name: "gt-eq-passed",
            array: Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
            scalar: DataValue::Int8(Some(3)),
            op: DataValueComparisonOperator::GtEq,
            expect: Arc::new(BooleanArray::from(vec![false, true, true, true])),
            error: ""
        },
    ];

    for t in tests {
        let result = DataArrayComparison::data_array_comparison_op(
            t.op.clone(),
            &DataColumnarValue::Constant(t.scalar, t.array.len()),
            &DataColumnarValue::Array(t.array)
        );
        match result {
            Ok(v) => assert_eq!(v.as_ref(), t.expect.as_ref()),
            Err(e) => assert_eq!(t.error, e.to_string())
        }
    }
}
