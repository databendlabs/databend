// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_arrow::arrow::array::ArrayRef;
use common_datavalues::*;
use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::*;

// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[test]
fn test_array_common_aggregate() {
    use std::sync::Arc;

    use pretty_assertions::assert_eq;

    use crate::*;

    #[allow(dead_code)]
    struct ArrayTest {
        name: &'static str,
        args: Vec<ArrayRef>,
        expect: Vec<DataValue>,
        error: Vec<&'static str>,
        func: Box<dyn Fn(DataColumnarValue) -> Result<DataValue>>,
    }

    let tests = vec![
        ArrayTest {
            name: "min-passed",
            args: vec![
                Arc::new(StringArray::from(vec!["x1", "x2"])),
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
            func: Box::new(AggregateMinFunction::min_batch),
            expect: vec![
                DataValue::Utf8(Some("x1".to_string())),
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
            error: vec![""]
        },
        ArrayTest {
            name: "max-passed",
            args: vec![
                Arc::new(StringArray::from(vec!["x1", "x2"])),
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
            func: Box::new(AggregateMaxFunction::max_batch),
            expect: vec![
                DataValue::Utf8(Some("x2".to_string())),
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
            error: vec![""]
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
            func: Box::new(AggregateSumFunction::sum_batch),
            expect: vec![
                DataValue::Utf8(Some("xx".to_string())),
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
            error: vec!["Code: 10, displayText = DataValue Error: Unsupported aggregate operation: sum for data type: Utf8."]
        },
    ];

    for t in tests.iter() {
        for (i, args) in t.args.iter().enumerate() {
            let result = (*(t.func))(args.clone().into());
            match result {
                Ok(v) => assert_eq!(v, t.expect[i]),
                Err(e) => assert_eq!(t.error[i], e.to_string()),
            }
        }
    }
}

#[test]
fn test_aggregate_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        eval_nums: usize,
        args: Vec<DataField>,
        display: &'static str,
        nullable: bool,
        columns: Vec<DataColumnarValue>,
        expect: DataValue,
        error: &'static str,
        func_name: &'static str,
    }

    let columns: Vec<DataColumnarValue> = vec![
        Arc::new(Int64Array::from(vec![4, 3, 2, 1])).into(),
        Arc::new(Int64Array::from(vec![1, 2, 3, 4])).into(),
    ];

    let args = vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Int64, false),
    ];

    let tests = vec![
        Test {
            name: "count-passed",
            eval_nums: 1,
            args: vec![args[0].clone()],
            display: "count",
            nullable: false,
            func_name: "count",
            columns: vec![columns[0].clone()],
            expect: DataValue::UInt64(Some(4)),
            error: "",
        },
        Test {
            name: "max-passed",
            eval_nums: 2,
            args: vec![args[0].clone()],
            display: "max",
            nullable: false,
            func_name: "max",
            columns: vec![columns[0].clone()],
            expect: DataValue::Int64(Some(4)),
            error: "",
        },
        Test {
            name: "min-passed",
            eval_nums: 2,
            args: vec![args[0].clone()],
            display: "min",
            nullable: false,
            func_name: "min",
            columns: vec![columns[0].clone()],
            expect: DataValue::Int64(Some(1)),
            error: "",
        },
        Test {
            name: "avg-passed",
            eval_nums: 1,
            args: vec![args[0].clone()],
            display: "avg",
            nullable: false,
            func_name: "avg",
            columns: vec![columns[0].clone()],
            expect: DataValue::Float64(Some(2.5)),
            error: "",
        },
        Test {
            name: "sum-passed",
            eval_nums: 1,
            args: vec![args[0].clone()],
            display: "sum",
            nullable: false,
            func_name: "sum",
            columns: vec![columns[0].clone()],
            expect: DataValue::Int64(Some(10)),
            error: "",
        },
        Test {
            name: "argMax-passed",
            eval_nums: 1,
            args: args.clone(),
            display: "argmax",
            nullable: false,
            func_name: "argmax",
            columns: columns.clone(),
            expect: DataValue::Int64(Some(1)),
            error: "",
        },
        Test {
            name: "argMin-passed",
            eval_nums: 1,
            args: args.clone(),
            display: "argmin",
            nullable: false,
            func_name: "argmin",
            columns: columns.clone(),
            expect: DataValue::Int64(Some(4)),
            error: "",
        },
        Test {
            name: "argMin-notpassed",
            eval_nums: 1,
            args: vec![args[0].clone()],
            display: "argmin",
            nullable: false,
            func_name: "argmin",
            columns: columns.clone(),
            expect: DataValue::Int64(Some(4)),
            error: "Code: 28, displayText = argmin expect to have two arguments, but got 1.",
        },
        Test {
            name: "uniq-passed",
            eval_nums: 1,
            args: vec![args[0].clone()],
            display: "uniq",
            nullable: false,
            func_name: "uniq",
            columns: vec![columns[0].clone()],
            expect: DataValue::UInt64(Some(4)),
            error: "",
        },
    ];

    for t in tests {
        let rows = t.columns[0].len();

        let func = || -> Result<()> {
            let mut func1 = AggregateFunctionFactory::get(t.func_name, t.args.clone())?;

            for _ in 0..t.eval_nums {
                func1.accumulate(&t.columns, rows)?;
            }
            let state1 = func1.accumulate_result()?;

            let mut func2 = AggregateFunctionFactory::get(t.func_name, t.args.clone())?;
            for _ in 1..t.eval_nums {
                func2.accumulate(&t.columns, rows)?;
            }
            let state2 = func2.accumulate_result()?;

            let mut final_func = AggregateFunctionFactory::get(t.func_name, t.args.clone())?;
            final_func.merge(&*state1)?;
            final_func.merge(&*state2)?;

            let result = final_func.merge_result()?;
            assert_eq!(&t.expect, &result);
            assert_eq!(t.display, format!("{:}", final_func));
            Ok(())
        };

        if let Err(e) = func() {
            assert_eq!(t.error, e.to_string());
        }
    }
    Ok(())
}
