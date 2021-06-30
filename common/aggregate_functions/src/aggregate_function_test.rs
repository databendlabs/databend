// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;
use common_datavalues::DFBooleanArray;
use common_datavalues::DFInt64Array;
use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::*;

#[test]
fn test_aggregate_function() -> Result<()> {
    struct Test {
        name: &'static str,
        eval_nums: usize,
        args: Vec<DataField>,
        display: &'static str,
        columns: Vec<DataColumn>,
        expect: DataValue,
        error: &'static str,
        func_name: &'static str,
    }

    let columns: Vec<DataColumn> = vec![
        Series::new(vec![4i64, 3, 2, 1]).into(),
        Series::new(vec![1i64, 2, 3, 4]).into(),
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
            assert_eq!(&t.expect, &result, "{}", t.name);
            assert_eq!(t.display, format!("{:}", final_func), "{}", t.name);
            Ok(())
        };

        if let Err(e) = func() {
            assert_eq!(t.error, e.to_string());
        }
    }
    Ok(())
}

#[test]
fn test_aggregate_function_on_empty_data() -> Result<()> {
    struct Test {
        name: &'static str,
        eval_nums: usize,
        args: Vec<DataField>,
        display: &'static str,
        columns: Vec<DataColumn>,
        expect: DataValue,
        error: &'static str,
        func_name: &'static str,
    }

    let columns: Vec<DataColumn> = vec![
        DFInt64Array::new_from_slice(&vec![]).into(),
        DFBooleanArray::new_from_slice(&vec![]).into(),
    ];

    let args = vec![
        DataField::new("a", DataType::Int64, true),
        DataField::new("b", DataType::Int64, true),
    ];

    let tests = vec![
        Test {
            name: "count-passed",
            eval_nums: 1,
            args: vec![args[0].clone()],
            display: "count",
            func_name: "count",
            columns: vec![columns[0].clone()],
            expect: DataValue::UInt64(Some(0)),
            error: "",
        },
        Test {
            name: "max-passed",
            eval_nums: 2,
            args: vec![args[0].clone()],
            display: "max",
            func_name: "max",
            columns: vec![columns[0].clone()],
            expect: DataValue::Int64(None),
            error: "",
        },
        Test {
            name: "min-passed",
            eval_nums: 2,
            args: vec![args[0].clone()],
            display: "min",
            func_name: "min",
            columns: vec![columns[0].clone()],
            expect: DataValue::Int64(None),
            error: "",
        },
        Test {
            name: "avg-passed",
            eval_nums: 1,
            args: vec![args[0].clone()],
            display: "avg",
            func_name: "avg",
            columns: vec![columns[0].clone()],
            expect: DataValue::Float64(None),
            error: "",
        },
        Test {
            name: "sum-passed",
            eval_nums: 1,
            args: vec![args[0].clone()],
            display: "sum",
            func_name: "sum",
            columns: vec![columns[0].clone()],
            expect: DataValue::Int64(None),
            error: "",
        },
        Test {
            name: "argMax-passed",
            eval_nums: 1,
            args: args.clone(),
            display: "argmax",
            func_name: "argmax",
            columns: columns.clone(),
            expect: DataValue::Int64(None),
            error: "",
        },
        Test {
            name: "argMin-passed",
            eval_nums: 1,
            args: args.clone(),
            display: "argmin",
            func_name: "argmin",
            columns: columns.clone(),
            expect: DataValue::Int64(None),
            error: "",
        },
        Test {
            name: "uniq-passed",
            eval_nums: 1,
            args: vec![args[0].clone()],
            display: "uniq",
            func_name: "uniq",
            columns: vec![columns[0].clone()],
            expect: DataValue::UInt64(Some(0)),
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

            assert_eq!(&t.expect, &result, "{}", t.name);
            assert_eq!(t.display, format!("{:}", final_func), "{}", t.name);
            Ok(())
        };

        if let Err(e) = func() {
            assert_eq!(t.error, e.to_string());
        }
    }
    Ok(())
}
