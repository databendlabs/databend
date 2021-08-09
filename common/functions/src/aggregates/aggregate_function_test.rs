// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use bumpalo::Bump;
use common_datavalues::prelude::*;
use common_datavalues::DFBooleanArray;
use common_datavalues::DFInt64Array;
use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::aggregates::*;

#[test]
fn test_aggregate_function() -> Result<()> {
    struct Test {
        name: &'static str,
        eval_nums: usize,
        args: Vec<DataField>,
        display: &'static str,
        arrays: Vec<Series>,
        expect: DataValue,
        error: &'static str,
        func_name: &'static str,
    }

    let arrays: Vec<Series> = vec![
        Series::new(vec![4i64, 3, 2, 1]),
        Series::new(vec![1i64, 2, 3, 4]),
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
            arrays: vec![arrays[0].clone()],
            expect: DataValue::UInt64(Some(4)),
            error: "",
        },
        Test {
            name: "max-passed",
            eval_nums: 2,
            args: vec![args[0].clone()],
            display: "max",
            func_name: "max",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::Int64(Some(4)),
            error: "",
        },
        Test {
            name: "min-passed",
            eval_nums: 2,
            args: vec![args[0].clone()],
            display: "min",
            func_name: "min",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::Int64(Some(1)),
            error: "",
        },
        Test {
            name: "avg-passed",
            eval_nums: 1,
            args: vec![args[0].clone()],
            display: "avg",
            func_name: "avg",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::Float64(Some(2.5)),
            error: "",
        },
        Test {
            name: "sum-passed",
            eval_nums: 1,
            args: vec![args[0].clone()],
            display: "sum",
            func_name: "sum",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::Int64(Some(10)),
            error: "",
        },
        Test {
            name: "argMax-passed",
            eval_nums: 1,
            args: args.clone(),
            display: "argmax",
            func_name: "argmax",
            arrays: arrays.clone(),
            expect: DataValue::Int64(Some(1)),
            error: "",
        },
        Test {
            name: "argMin-passed",
            eval_nums: 1,
            args: args.clone(),
            display: "argmin",
            func_name: "argmin",
            arrays: arrays.clone(),
            expect: DataValue::Int64(Some(4)),
            error: "",
        },
        Test {
            name: "argMin-notpassed",
            eval_nums: 1,
            args: vec![args[0].clone()],
            display: "argmin",
            func_name: "argmin",
            arrays: arrays.clone(),
            expect: DataValue::Int64(Some(4)),
            error: "Code: 28, displayText = argmin expect to have two arguments, but got 1.",
        },
        Test {
            name: "uniq-passed",
            eval_nums: 1,
            args: vec![args[0].clone()],
            display: "uniq",
            func_name: "uniq",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::UInt64(Some(4)),
            error: "",
        },
    ];

    for t in tests {
        let arena = Bump::new();
        let rows = t.arrays[0].len();

        let func = || -> Result<()> {
            let func = AggregateFunctionFactory::get(t.func_name, t.args.clone())?;

            let place1 = func.allocate_state(&arena);

            for _ in 0..t.eval_nums {
                func.accumulate(place1, &t.arrays, rows)?;
            }

            let place2 = func.allocate_state(&arena);

            for _ in 1..t.eval_nums {
                func.accumulate(place2, &t.arrays, rows)?;
            }

            func.merge(place1, place2)?;
            let result = func.merge_result(place1)?;
            assert_eq!(&t.expect, &result, "{}", t.name);
            assert_eq!(t.display, format!("{:}", func), "{}", t.name);
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
        arrays: Vec<Series>,
        expect: DataValue,
        error: &'static str,
        func_name: &'static str,
    }

    let arrays: Vec<Series> = vec![
        DFInt64Array::new_from_slice(&vec![]).into_series(),
        DFBooleanArray::new_from_slice(&vec![]).into_series(),
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
            arrays: vec![arrays[0].clone()],
            expect: DataValue::UInt64(Some(0)),
            error: "",
        },
        Test {
            name: "max-passed",
            eval_nums: 2,
            args: vec![args[0].clone()],
            display: "max",
            func_name: "max",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::Int64(None),
            error: "",
        },
        Test {
            name: "min-passed",
            eval_nums: 2,
            args: vec![args[0].clone()],
            display: "min",
            func_name: "min",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::Int64(None),
            error: "",
        },
        Test {
            name: "avg-passed",
            eval_nums: 1,
            args: vec![args[0].clone()],
            display: "avg",
            func_name: "avg",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::Float64(None),
            error: "",
        },
        Test {
            name: "sum-passed",
            eval_nums: 1,
            args: vec![args[0].clone()],
            display: "sum",
            func_name: "sum",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::Int64(None),
            error: "",
        },
        Test {
            name: "argMax-passed",
            eval_nums: 1,
            args: args.clone(),
            display: "argmax",
            func_name: "argmax",
            arrays: arrays.clone(),
            expect: DataValue::Int64(None),
            error: "",
        },
        Test {
            name: "argMin-passed",
            eval_nums: 1,
            args: args.clone(),
            display: "argmin",
            func_name: "argmin",
            arrays: arrays.clone(),
            expect: DataValue::Int64(None),
            error: "",
        },
        Test {
            name: "uniq-passed",
            eval_nums: 1,
            args: vec![args[0].clone()],
            display: "uniq",
            func_name: "uniq",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::UInt64(Some(0)),
            error: "",
        },
    ];

    for t in tests {
        let arena = Bump::new();
        let rows = t.arrays[0].len();

        let func = || -> Result<()> {
            let func = AggregateFunctionFactory::get(t.func_name, t.args.clone())?;
            let place1 = func.allocate_state(&arena);
            for _ in 0..t.eval_nums {
                func.accumulate(place1, &t.arrays, rows)?;
            }

            let place2 = func.allocate_state(&arena);
            for _ in 1..t.eval_nums {
                func.accumulate(place2, &t.arrays, rows)?;
            }

            func.merge(place1, place2)?;
            let result = func.merge_result(place1)?;

            assert_eq!(&t.expect, &result, "{}", t.name);
            assert_eq!(t.display, format!("{:}", func), "{}", t.name);
            Ok(())
        };

        if let Err(e) = func() {
            assert_eq!(t.error, e.to_string());
        }
    }
    Ok(())
}
