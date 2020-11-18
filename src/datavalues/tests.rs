// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[test]
fn test_array_arithmetic() {
    use std::sync::Arc;

    use super::*;
    use crate::error::Result;

    #[allow(dead_code)]
    struct ArrayTest {
        name: &'static str,
        args: Vec<Vec<DataArrayRef>>,
        expect: Vec<DataArrayRef>,
        error: Vec<&'static str>,
        func: Box<dyn Fn(DataArrayRef, DataArrayRef) -> Result<DataArrayRef>>,
    }

    let tests = vec![
        ArrayTest {
            name: "add-passed",
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
            func: Box::new(data_array_add),
            expect: vec![
                Arc::new(StringArray::from(vec![""])),
                Arc::new(Int8Array::from(vec![5, 5, 5, 5])),
                Arc::new(Int16Array::from(vec![5, 5, 5, 5])),
                Arc::new(Int32Array::from(vec![5, 5, 5, 5])),
                Arc::new(Int64Array::from(vec![5, 5, 5, 5])),
                Arc::new(UInt8Array::from(vec![5, 5, 5, 5])),
                Arc::new(UInt16Array::from(vec![5, 5, 5, 5])),
                Arc::new(UInt32Array::from(vec![5, 5, 5, 5])),
                Arc::new(UInt64Array::from(vec![5, 5, 5, 5])),
                Arc::new(Float32Array::from(vec![5.0, 5.0, 5.0, 5.0])),
                Arc::new(Float64Array::from(vec![5.0, 5.0, 5.0, 5.0])),
            ],
            error: vec![
                "Unsupported Error: Unsupported arithmetic_compute::add for data type: Utf8",
            ],
        },
        ArrayTest {
            name: "sub-passed",
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
            func: Box::new(data_array_sub),
            expect: vec![
                Arc::new(StringArray::from(vec![""])),
                Arc::new(Int8Array::from(vec![3, 1, -1, -3])),
                Arc::new(Int16Array::from(vec![3, 1, -1, -3])),
                Arc::new(Int32Array::from(vec![3, 1, -1, -3])),
                Arc::new(Int64Array::from(vec![3, 1, -1, -3])),
                Arc::new(UInt8Array::from(vec![1, 1, 1, 1])),
                Arc::new(UInt16Array::from(vec![1, 1, 1, 1])),
                Arc::new(UInt32Array::from(vec![1, 1, 1, 1])),
                Arc::new(UInt64Array::from(vec![1, 1, 1, 1])),
                Arc::new(Float32Array::from(vec![3.0, 1.0, -1.0, -3.0])),
                Arc::new(Float64Array::from(vec![3.0, 1.0, -1.0, -3.0])),
            ],
            error: vec![
                "Unsupported Error: Unsupported arithmetic_compute::subtract for data type: Utf8",
            ],
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
            func: Box::new(data_array_mul),
            expect: vec![
                Arc::new(StringArray::from(vec![""])),
                Arc::new(Int8Array::from(vec![4, 6, 6, 4])),
                Arc::new(Int16Array::from(vec![4, 6, 6, 4])),
                Arc::new(Int32Array::from(vec![4, 6, 6, 4])),
                Arc::new(Int64Array::from(vec![4, 6, 6, 4])),
                Arc::new(UInt8Array::from(vec![4, 6, 6, 4])),
                Arc::new(UInt16Array::from(vec![4, 6, 6, 4])),
                Arc::new(UInt32Array::from(vec![4, 6, 6, 4])),
                Arc::new(UInt64Array::from(vec![4, 6, 6, 4])),
                Arc::new(Float32Array::from(vec![4.0, 6.0, 6.0, 4.0])),
                Arc::new(Float64Array::from(vec![4.0, 6.0, 6.0, 4.0])),
            ],
            error: vec![
                "Unsupported Error: Unsupported arithmetic_compute::multiply for data type: Utf8",
            ],
        },
        ArrayTest {
            name: "div-passed",
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
            func: Box::new(data_array_div),
            expect: vec![
                Arc::new(StringArray::from(vec![""])),
                Arc::new(Int8Array::from(vec![4, 1, 0, 0])),
                Arc::new(Int16Array::from(vec![4, 1, 0, 0])),
                Arc::new(Int32Array::from(vec![4, 1, 0, 0])),
                Arc::new(Int64Array::from(vec![4, 1, 0, 0])),
                Arc::new(UInt8Array::from(vec![4, 1, 0, 0])),
                Arc::new(UInt16Array::from(vec![4, 1, 0, 0])),
                Arc::new(UInt32Array::from(vec![4, 1, 0, 0])),
                Arc::new(UInt64Array::from(vec![4, 1, 0, 0])),
                Arc::new(Float32Array::from(vec![4.0, 1.5, 0.6666666666666666, 0.25])),
                Arc::new(Float64Array::from(vec![4.0, 1.5, 0.6666666666666666, 0.25])),
            ],
            error: vec![
                "Unsupported Error: Unsupported arithmetic_compute::divide for data type: Utf8",
            ],
        },
    ];

    for t in tests {
        for (i, args) in t.args.iter().enumerate() {
            let result = (t.func)(args[0].clone(), args[1].clone());
            match result {
                Ok(ref v) => {
                    // Result check.
                    if !v.equals(&*t.expect[i]) {
                        println!("expect:\n{:?} \nactual:\n{:?}", t.expect, v);
                        assert!(false);
                    }
                }
                Err(e) => assert_eq!(t.error[i], e.to_string()),
            }
        }
    }
}

#[test]
fn test_array_aggregate() {
    use std::sync::Arc;

    use super::*;
    use crate::error::Result;

    #[allow(dead_code)]
    struct ArrayTest {
        name: &'static str,
        args: Vec<DataArrayRef>,
        expect: Vec<DataValue>,
        error: Vec<&'static str>,
        func: Box<dyn Fn(DataArrayRef) -> Result<DataValue>>,
    }

    let tests = vec![
        ArrayTest {
            name: "min-passed",
            args: vec![
                Arc::new(StringArray::from(vec!["xx"])),
                Arc::new(Int8Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int16Array::from(vec![4, 3, 2, 1])),
                Arc::new(Int32Array::from(vec![4, 3, 2, 1])),
                Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                Arc::new(UInt8Array::from(vec![4, 3, 2, 1])),
                Arc::new(UInt16Array::from(vec![4, 3, 2, 1])),
                Arc::new(UInt32Array::from(vec![4, 3, 2, 1])),
                Arc::new(UInt64Array::from(vec![4, 3, 2, 1])),
                Arc::new(Float32Array::from(vec![4.0, 3.0, 2.0, 1.0])),
                Arc::new(Float64Array::from(vec![4.0, 3.0, 2.0, 1.0])),
            ],
            func: Box::new(data_array_min),
            expect: vec![
                DataValue::String(Some("xx".to_string())),
                DataValue::Int8(Some(1)),
                DataValue::Int16(Some(1)),
                DataValue::Int32(Some(1)),
                DataValue::Int64(Some(1)),
                DataValue::UInt8(Some(1)),
                DataValue::UInt16(Some(1)),
                DataValue::UInt32(Some(1)),
                DataValue::UInt64(Some(1)),
                DataValue::Float32(Some(1.0)),
                DataValue::Float64(Some(1.0)),
            ],
            error: vec!["Unsupported Error: Unsupported data_array_min() for data type: Utf8"],
        },
        ArrayTest {
            name: "max-passed",
            args: vec![
                Arc::new(StringArray::from(vec!["xx"])),
                Arc::new(Int8Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int16Array::from(vec![4, 3, 2, 1])),
                Arc::new(Int32Array::from(vec![4, 3, 2, 1])),
                Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                Arc::new(UInt8Array::from(vec![4, 3, 2, 1])),
                Arc::new(UInt16Array::from(vec![4, 3, 2, 1])),
                Arc::new(UInt32Array::from(vec![4, 3, 2, 1])),
                Arc::new(UInt64Array::from(vec![4, 3, 2, 1])),
                Arc::new(Float32Array::from(vec![4.0, 3.0, 2.0, 1.0])),
                Arc::new(Float64Array::from(vec![4.0, 3.0, 2.0, 1.0])),
            ],
            func: Box::new(data_array_max),
            expect: vec![
                DataValue::String(Some("xx".to_string())),
                DataValue::Int8(Some(4)),
                DataValue::Int16(Some(4)),
                DataValue::Int32(Some(4)),
                DataValue::Int64(Some(4)),
                DataValue::UInt8(Some(4)),
                DataValue::UInt16(Some(4)),
                DataValue::UInt32(Some(4)),
                DataValue::UInt64(Some(4)),
                DataValue::Float32(Some(4.0)),
                DataValue::Float64(Some(4.0)),
            ],
            error: vec!["Unsupported Error: Unsupported data_array_max() for data type: Utf8"],
        },
        ArrayTest {
            name: "sum-passed",
            args: vec![
                Arc::new(StringArray::from(vec!["xx"])),
                Arc::new(Int8Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int16Array::from(vec![4, 3, 2, 1])),
                Arc::new(Int32Array::from(vec![4, 3, 2, 1])),
                Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                Arc::new(UInt8Array::from(vec![4, 3, 2, 1])),
                Arc::new(UInt16Array::from(vec![4, 3, 2, 1])),
                Arc::new(UInt32Array::from(vec![4, 3, 2, 1])),
                Arc::new(UInt64Array::from(vec![4, 3, 2, 1])),
                Arc::new(Float32Array::from(vec![4.0, 3.0, 2.0, 1.0])),
                Arc::new(Float64Array::from(vec![4.0, 3.0, 2.0, 1.0])),
            ],
            func: Box::new(data_array_sum),
            expect: vec![
                DataValue::String(Some("xx".to_string())),
                DataValue::Int8(Some(10)),
                DataValue::Int16(Some(10)),
                DataValue::Int32(Some(10)),
                DataValue::Int64(Some(10)),
                DataValue::UInt8(Some(10)),
                DataValue::UInt16(Some(10)),
                DataValue::UInt32(Some(10)),
                DataValue::UInt64(Some(10)),
                DataValue::Float32(Some(10.0)),
                DataValue::Float64(Some(10.0)),
            ],
            error: vec!["Unsupported Error: Unsupported data_array_sum() for data type: Utf8"],
        },
    ];

    for t in tests {
        for (i, args) in t.args.iter().enumerate() {
            let result = (t.func)(args.clone());
            match result {
                Ok(ref v) => {
                    // Result check.
                    if *v != t.expect[i] {
                        println!("expect:\n{:?} \nactual:\n{:?}", t.expect, v);
                        assert!(false);
                    }
                }
                Err(e) => assert_eq!(t.error[i], e.to_string()),
            }
        }
    }
}

#[test]
fn test_scalar_arithmetic() {
    use super::*;
    use crate::error::Result;

    #[allow(dead_code)]
    struct ScalarTest {
        name: &'static str,
        args: Vec<Vec<DataValue>>,
        expect: Vec<DataValue>,
        error: Vec<&'static str>,
        func: Box<dyn Fn(DataValue, DataValue) -> Result<DataValue>>,
    }

    let tests = vec![ScalarTest {
        name: "add-passed",
        args: vec![
            vec![
                DataValue::String(Some("xx".to_string())),
                DataValue::Int8(Some(2)),
            ],
            vec![DataValue::Int8(Some(1)), DataValue::Int8(Some(2))],
            vec![DataValue::Int16(Some(1)), DataValue::Int16(Some(2))],
            vec![DataValue::Int32(Some(1)), DataValue::Int32(Some(2))],
            vec![DataValue::Int64(Some(1)), DataValue::Int64(Some(2))],
            vec![DataValue::UInt8(Some(1)), DataValue::UInt8(Some(2))],
            vec![DataValue::UInt16(Some(1)), DataValue::UInt16(Some(2))],
            vec![DataValue::UInt32(Some(1)), DataValue::UInt32(Some(2))],
            vec![DataValue::UInt64(Some(1)), DataValue::UInt64(Some(2))],
            vec![DataValue::Float32(Some(1.0)), DataValue::Float32(Some(2.0))],
            vec![DataValue::Float64(Some(1.0)), DataValue::Float64(Some(2.0))],
        ],
        func: Box::new(data_value_add),
        expect: vec![
            DataValue::String(Some("xx".to_string())),
            DataValue::Int8(Some(3)),
            DataValue::Int16(Some(3)),
            DataValue::Int32(Some(3)),
            DataValue::Int64(Some(3)),
            DataValue::UInt8(Some(3)),
            DataValue::UInt16(Some(3)),
            DataValue::UInt32(Some(3)),
            DataValue::UInt64(Some(3)),
            DataValue::Float32(Some(3.0)),
            DataValue::Float64(Some(3.0)),
        ],
        error: vec![
            "Unsupported Error: Unsupported data_value_add() for data type: left:Utf8, right:Int8",
        ],
    }];

    for t in tests {
        for (i, args) in t.args.iter().enumerate() {
            let result = (t.func)(args[0].clone(), args[1].clone());
            match result {
                Ok(ref v) => {
                    // Result check.
                    if *v != t.expect[i] {
                        println!("expect:\n{:?} \nactual:\n{:?}", t.expect, v);
                        assert!(false);
                    }
                }
                Err(e) => assert_eq!(t.error[i], e.to_string()),
            }
        }
    }
}
