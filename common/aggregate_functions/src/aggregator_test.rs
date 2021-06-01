// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::*;
use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::*;

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
    ];

    for t in tests {
        let rows = t.columns[0].len();

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
    }
    Ok(())
}
