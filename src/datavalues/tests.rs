// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::error::Result;

use crate::datavalues::{DataArrayRef, DataValue};

#[allow(dead_code)]
struct ArrayTest {
    name: &'static str,
    args: Vec<DataArrayRef>,
    expect: DataArrayRef,
    error: &'static str,
    func: Box<dyn Fn(DataArrayRef, DataArrayRef) -> Result<DataArrayRef>>,
}

#[test]
fn test_array_arithmetic() {
    use super::*;

    use std::sync::Arc;

    let tests = vec![
        ArrayTest {
            name: "add-int64-passed",
            args: vec![
                Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            ],
            func: Box::new(data_array_add),
            expect: Arc::new(Int64Array::from(vec![5, 5, 5, 5])),
            error: "",
        },
        ArrayTest {
            name: "sub-int64-passed",
            args: vec![
                Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            ],
            func: Box::new(data_array_sub),
            expect: Arc::new(Int64Array::from(vec![3, 1, -1, -3])),
            error: "",
        },
        ArrayTest {
            name: "mul-int64-passed",
            args: vec![
                Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            ],
            func: Box::new(data_array_mul),
            expect: Arc::new(Int64Array::from(vec![4, 6, 6, 4])),
            error: "",
        },
        ArrayTest {
            name: "div-int64-passed",
            args: vec![
                Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            ],
            func: Box::new(data_array_div),
            expect: Arc::new(Int64Array::from(vec![4, 1, 0, 0])),
            error: "",
        },
    ];

    for t in tests {
        let result = (t.func)(t.args[0].clone(), t.args[1].clone());
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
    }
}

#[allow(dead_code)]
struct ScalarTest {
    name: &'static str,
    args: Vec<DataValue>,
    expect: DataValue,
    error: &'static str,
    func: Box<dyn Fn(DataValue, DataValue) -> Result<DataValue>>,
}

#[test]
fn test_scalar_arithmetic() {
    use super::*;

    let tests = vec![
        ScalarTest {
            name: "add-int64-passed",
            args: vec![DataValue::Int64(Some(1)), DataValue::Int64(Some(2))],
            func: Box::new(data_value_add),
            expect: DataValue::Int64(Some(3)),
            error: "",
        },
        ScalarTest {
            name: "add-none-int64-passed",
            args: vec![DataValue::Int64(None), DataValue::Int64(Some(2))],
            func: Box::new(data_value_add),
            expect: DataValue::Int64(Some(2)),
            error: "",
        },
        ScalarTest {
            name: "add-uint64-passed",
            args: vec![DataValue::UInt64(Some(1)), DataValue::UInt64(Some(2))],
            func: Box::new(data_value_add),
            expect: DataValue::UInt64(Some(3)),
            error: "",
        },
        ScalarTest {
            name: "add-float64-passed",
            args: vec![DataValue::Float64(Some(1.0)), DataValue::Float64(Some(2.0))],
            func: Box::new(data_value_add),
            expect: DataValue::Float64(Some(3.0)),
            error: "",
        },
    ];

    for t in tests {
        let result = (t.func)(t.args[0].clone(), t.args[1].clone());
        match result {
            Ok(ref v) => {
                // Result check.
                if *v != t.expect {
                    println!("expect:\n{:?} \nactual:\n{:?}", t.expect, v);
                    assert!(false);
                }
            }
            Err(e) => assert_eq!(t.error, e.to_string()),
        }
    }
}
