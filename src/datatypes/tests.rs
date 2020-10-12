// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use super::*;

#[allow(dead_code)]
struct Test {
    name: &'static str,
    is_aggregate: bool,
    args: Vec<DataArrayRef>,
    expect: DataArrayRef,
    error: &'static str,
    func: Box<dyn Fn(DataArrayRef, DataArrayRef) -> Result<DataArrayRef>>,
}

#[test]
fn test_cases() {
    use super::*;

    use std::sync::Arc;

    let tests = vec![Test {
        name: "add-int64-passed",
        is_aggregate: false,
        args: vec![
            Arc::new(Int64Array::from(vec![4, 3, 2, 1])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
        ],
        func: Box::new(array_add),
        expect: Arc::new(Int64Array::from(vec![5, 5, 5, 5])),
        error: "",
    }];

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
