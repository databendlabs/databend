// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_array_arithmetic() {
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    use super::*;

    #[allow(dead_code)]
    struct ArrayTest {
        name: &'static str,
        args: Vec<Vec<DataArrayRef>>,
        expect: Vec<DataArrayRef>,
        error: Vec<&'static str>,
        op: DataValueArithmeticOperator,
    }

    let tests = vec![
        ArrayTest {
            name: "plus-passed",
            args: vec![
                vec![
                    Arc::new(StringArray::from(vec!["xx"])),
                    Arc::new(StringArray::from(vec!["yy"])),
                ],
                vec![
                    Arc::new(Int16Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int8Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int8Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(Int16Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int16Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(Int32Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(UInt8Array::from(vec![4, 3, 2, 1])),
                    Arc::new(UInt8Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(UInt16Array::from(vec![4, 3, 2, 1])),
                    Arc::new(UInt16Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(UInt32Array::from(vec![4, 3, 2, 1])),
                    Arc::new(UInt32Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(UInt64Array::from(vec![4, 3, 2, 1])),
                    Arc::new(UInt64Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(Float32Array::from(vec![4.0, 3.0, 2.0, 1.0])),
                    Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                ],
                vec![
                    Arc::new(Float64Array::from(vec![4.0, 3.0, 2.0, 1.0])),
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                ],
            ],
            op: DataValueArithmeticOperator::Plus,
            expect: vec![
                Arc::new(StringArray::from(vec![""])),
                Arc::new(Int32Array::from(vec![5, 5, 5, 5])),
                Arc::new(Int16Array::from(vec![5, 5, 5, 5])),
                Arc::new(Int32Array::from(vec![5, 5, 5, 5])),
                Arc::new(Int64Array::from(vec![5, 5, 5, 5])),
                Arc::new(Int64Array::from(vec![5, 5, 5, 5])),
                Arc::new(UInt16Array::from(vec![5, 5, 5, 5])),
                Arc::new(UInt32Array::from(vec![5, 5, 5, 5])),
                Arc::new(UInt64Array::from(vec![5, 5, 5, 5])),
                Arc::new(UInt64Array::from(vec![5, 5, 5, 5])),
                Arc::new(Float64Array::from(vec![5.0, 5.0, 5.0, 5.0])),
                Arc::new(Float64Array::from(vec![5.0, 5.0, 5.0, 5.0])),
            ],
            error: vec!["DataValue Internal Error: Unsupported (Utf8) plus (Utf8)"],
        },
        ArrayTest {
            name: "minus-passed",
            args: vec![
                vec![
                    Arc::new(StringArray::from(vec!["xx"])),
                    Arc::new(StringArray::from(vec!["yy"])),
                ],
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int8Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int8Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(Int16Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int16Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(Int32Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(UInt8Array::from(vec![4, 3, 2, 1])),
                    Arc::new(UInt8Array::from(vec![3, 2, 1, 0])),
                ],
                vec![
                    Arc::new(UInt16Array::from(vec![4, 3, 2, 1])),
                    Arc::new(UInt16Array::from(vec![3, 2, 1, 0])),
                ],
                vec![
                    Arc::new(UInt32Array::from(vec![4, 3, 2, 1])),
                    Arc::new(UInt32Array::from(vec![3, 2, 1, 0])),
                ],
                vec![
                    Arc::new(UInt64Array::from(vec![4, 3, 2, 1])),
                    Arc::new(UInt64Array::from(vec![3, 2, 1, 0])),
                ],
                vec![
                    Arc::new(Float32Array::from(vec![4.0, 3.0, 2.0, 1.0])),
                    Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                ],
                vec![
                    Arc::new(Float64Array::from(vec![4.0, 3.0, 2.0, 1.0])),
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                ],
            ],
            op: DataValueArithmeticOperator::Minus,
            expect: vec![
                Arc::new(StringArray::from(vec![""])),
                Arc::new(Int64Array::from(vec![3, 1, -1, -3])),
                Arc::new(Int16Array::from(vec![3, 1, -1, -3])),
                Arc::new(Int32Array::from(vec![3, 1, -1, -3])),
                Arc::new(Int64Array::from(vec![3, 1, -1, -3])),
                Arc::new(Int64Array::from(vec![3, 1, -1, -3])),
                Arc::new(Int16Array::from(vec![1, 1, 1, 1])),
                Arc::new(Int32Array::from(vec![1, 1, 1, 1])),
                Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
                Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
                Arc::new(Float64Array::from(vec![3.0, 1.0, -1.0, -3.0])),
                Arc::new(Float64Array::from(vec![3.0, 1.0, -1.0, -3.0])),
            ],
            error: vec!["DataValue Internal Error: Unsupported (Utf8) minus (Utf8)"],
        },
        ArrayTest {
            name: "mul-passed",
            args: vec![
                vec![
                    Arc::new(StringArray::from(vec!["xx"])),
                    Arc::new(StringArray::from(vec!["yy"])),
                ],
                vec![
                    Arc::new(Int8Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int8Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(Int16Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int16Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(Int32Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                    Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(UInt8Array::from(vec![4, 3, 2, 1])),
                    Arc::new(UInt8Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(UInt16Array::from(vec![4, 3, 2, 1])),
                    Arc::new(UInt16Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(UInt32Array::from(vec![4, 3, 2, 1])),
                    Arc::new(UInt32Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(UInt64Array::from(vec![4, 3, 2, 1])),
                    Arc::new(UInt64Array::from(vec![1, 2, 3, 4])),
                ],
                vec![
                    Arc::new(Float32Array::from(vec![4.0, 3.0, 2.0, 1.0])),
                    Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                ],
                vec![
                    Arc::new(Float64Array::from(vec![4.0, 3.0, 2.0, 1.0])),
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                ],
            ],
            op: DataValueArithmeticOperator::Mul,
            expect: vec![
                Arc::new(StringArray::from(vec![""])),
                Arc::new(Int16Array::from(vec![4, 6, 6, 4])),
                Arc::new(Int32Array::from(vec![4, 6, 6, 4])),
                Arc::new(Int64Array::from(vec![4, 6, 6, 4])),
                Arc::new(Int64Array::from(vec![4, 6, 6, 4])),
                Arc::new(UInt16Array::from(vec![4, 6, 6, 4])),
                Arc::new(UInt32Array::from(vec![4, 6, 6, 4])),
                Arc::new(UInt64Array::from(vec![4, 6, 6, 4])),
                Arc::new(UInt64Array::from(vec![4, 6, 6, 4])),
                Arc::new(Float64Array::from(vec![4.0, 6.0, 6.0, 4.0])),
                Arc::new(Float64Array::from(vec![4.0, 6.0, 6.0, 4.0])),
            ],
            error: vec!["DataValue Internal Error: Unsupported (Utf8) multiply (Utf8)"],
        },
        ArrayTest {
            name: "div-passed",
            args: vec![
                vec![
                    Arc::new(StringArray::from(vec!["xx"])),
                    Arc::new(StringArray::from(vec!["yy"])),
                ],
                vec![
                    Arc::new(Float64Array::from(vec![4.0, 3.0, 2.0, 1.0])),
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                ],
                vec![
                    Arc::new(Float64Array::from(vec![4.0, 3.0, 2.0, 1.0])),
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                ],
                vec![
                    Arc::new(Float64Array::from(vec![4.0, 3.0, 2.0, 1.0])),
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                ],
                vec![
                    Arc::new(Float64Array::from(vec![4.0, 3.0, 2.0, 1.0])),
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                ],
                vec![
                    Arc::new(Float64Array::from(vec![4.0, 3.0, 2.0, 1.0])),
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                ],
                vec![
                    Arc::new(Float64Array::from(vec![4.0, 3.0, 2.0, 1.0])),
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                ],
                vec![
                    Arc::new(Float64Array::from(vec![4.0, 3.0, 2.0, 1.0])),
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                ],
                vec![
                    Arc::new(Float64Array::from(vec![4.0, 3.0, 2.0, 1.0])),
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                ],
                vec![
                    Arc::new(Float64Array::from(vec![4.0, 3.0, 2.0, 1.0])),
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                ],
                vec![
                    Arc::new(Float64Array::from(vec![4.0, 3.0, 2.0, 1.0])),
                    Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                ],
            ],
            op: DataValueArithmeticOperator::Div,
            expect: vec![
                Arc::new(StringArray::from(vec![""])),
                Arc::new(Float64Array::from(vec![4.0, 1.5, 0.6666666666666666, 0.25])),
                Arc::new(Float64Array::from(vec![4.0, 1.5, 0.6666666666666666, 0.25])),
                Arc::new(Float64Array::from(vec![4.0, 1.5, 0.6666666666666666, 0.25])),
                Arc::new(Float64Array::from(vec![4.0, 1.5, 0.6666666666666666, 0.25])),
                Arc::new(Float64Array::from(vec![4.0, 1.5, 0.6666666666666666, 0.25])),
                Arc::new(Float64Array::from(vec![4.0, 1.5, 0.6666666666666666, 0.25])),
                Arc::new(Float64Array::from(vec![4.0, 1.5, 0.6666666666666666, 0.25])),
                Arc::new(Float64Array::from(vec![4.0, 1.5, 0.6666666666666666, 0.25])),
                Arc::new(Float64Array::from(vec![4.0, 1.5, 0.6666666666666666, 0.25])),
                Arc::new(Float64Array::from(vec![4.0, 1.5, 0.6666666666666666, 0.25])),
            ],
            error: vec!["DataValue Internal Error: Unsupported (Utf8) divide (Utf8)"],
        },
    ];

    for t in tests {
        for (i, args) in t.args.iter().enumerate() {
            let result = data_array_arithmetic_op(
                t.op.clone(),
                &DataColumnarValue::Array(args[0].clone()),
                &DataColumnarValue::Array(args[1].clone()),
            );
            match result {
                Ok(v) => assert_eq!(
                    v.as_ref(),
                    t.expect[i].as_ref(),
                    "failed in the test: {}, case: {}",
                    t.name,
                    i
                ),
                Err(e) => assert_eq!(
                    t.error[i],
                    e.to_string(),
                    "failed in the test: {}, case: {}",
                    t.name,
                    i
                ),
            }
        }
    }
}

#[test]
fn test_array_scalar_arithmetic() {
    use std::sync::Arc;

    use super::*;

    #[allow(dead_code)]
    struct ArrayTest {
        name: &'static str,
        array: DataArrayRef,
        op: DataValueArithmeticOperator,
        scalar: DataValue,
        expect: DataArrayRef,
        error: &'static str,
    }

    let tests = vec![
        ArrayTest {
            name: "add-passed",
            array: Arc::new(Int8Array::from(vec![4, 4, 4, 4])),
            scalar: DataValue::Int8(Some(1)),
            op: DataValueArithmeticOperator::Plus,
            expect: Arc::new(Int16Array::from(vec![5, 5, 5, 5])),
            error: "",
        },
        ArrayTest {
            name: "sub-passed",
            array: Arc::new(Int8Array::from(vec![4, 4, 4, 4])),
            scalar: DataValue::Int8(Some(1)),
            op: DataValueArithmeticOperator::Minus,
            expect: Arc::new(Int16Array::from(vec![3, 3, 3, 3])),
            error: "",
        },
        ArrayTest {
            name: "mul-passed",
            array: Arc::new(Int8Array::from(vec![4, 4, 4, 4])),
            scalar: DataValue::Int8(Some(1)),
            op: DataValueArithmeticOperator::Mul,
            expect: Arc::new(Int16Array::from(vec![4, 4, 4, 4])),
            error: "",
        },
        ArrayTest {
            name: "div-passed",
            array: Arc::new(Int8Array::from(vec![4, 4, 4, 4])),
            scalar: DataValue::Int8(Some(2)),
            op: DataValueArithmeticOperator::Div,
            expect: Arc::new(Float64Array::from(vec![2.0, 2.0, 2.0, 2.0])),
            error: "",
        },
    ];

    for t in tests {
        let result = data_array_arithmetic_op(
            t.op.clone(),
            &DataColumnarValue::Array(t.array),
            &DataColumnarValue::Scalar(t.scalar),
        );
        match result {
            Ok(v) => assert_eq!(
                v.as_ref(),
                t.expect.as_ref(),
                "failed in the test: {}",
                t.name
            ),
            Err(e) => assert_eq!(t.error, e.to_string(), "failed in the test: {}", t.name),
        }
    }
}

#[test]
fn test_scalar_array_arithmetic() {
    use std::sync::Arc;

    use super::*;

    #[allow(dead_code)]
    struct ArrayTest {
        name: &'static str,
        array: DataArrayRef,
        op: DataValueArithmeticOperator,
        scalar: DataValue,
        expect: DataArrayRef,
        error: &'static str,
    }

    let tests = vec![
        ArrayTest {
            name: "add-passed",
            array: Arc::new(Int8Array::from(vec![4, 4, 4, 4])),
            scalar: DataValue::Int8(Some(1)),
            op: DataValueArithmeticOperator::Plus,
            expect: Arc::new(Int16Array::from(vec![5, 5, 5, 5])),
            error: "",
        },
        ArrayTest {
            name: "sub-passed",
            array: Arc::new(Int8Array::from(vec![4, 4, 4, 4])),
            scalar: DataValue::Int8(Some(1)),
            op: DataValueArithmeticOperator::Minus,
            expect: Arc::new(Int16Array::from(vec![-3, -3, -3, -3])),
            error: "",
        },
        ArrayTest {
            name: "mul-passed",
            array: Arc::new(Int8Array::from(vec![4, 4, 4, 4])),
            scalar: DataValue::Int8(Some(1)),
            op: DataValueArithmeticOperator::Mul,
            expect: Arc::new(Int16Array::from(vec![4, 4, 4, 4])),
            error: "",
        },
        ArrayTest {
            name: "div-passed",
            array: Arc::new(Int8Array::from(vec![4, 4, 4, 4])),
            scalar: DataValue::Int8(Some(8)),
            op: DataValueArithmeticOperator::Div,
            expect: Arc::new(Float64Array::from(vec![2.0, 2.0, 2.0, 2.0])),
            error: "",
        },
    ];

    for t in tests {
        let result = data_array_arithmetic_op(
            t.op.clone(),
            &DataColumnarValue::Scalar(t.scalar),
            &DataColumnarValue::Array(t.array),
        );
        match result {
            Ok(v) => assert_eq!(
                v.as_ref(),
                t.expect.as_ref(),
                "failed in the test: {}",
                t.name
            ),
            Err(e) => assert_eq!(t.error, e.to_string(), "failed in the test: {}", t.name),
        }
    }
}
