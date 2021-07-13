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
fn test_aggregate_combinator_function() -> Result<()> {
    struct Test {
        name: &'static str,
        args: Vec<DataField>,
        display: &'static str,
        columns: Vec<DataColumn>,
        expect: DataValue,
        error: &'static str,
        func_name: &'static str,
    }

    let columns: Vec<DataColumn> = vec![
        Series::new(vec![4 as i64, 3, 2, 1, 3, 4]).into(),
        Series::new(vec![true, true, false, true, true, true]).into(),
    ];

    let args = vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Boolean, false),
    ];

    let tests = vec![
        Test {
            name: "count-distinct-passed",
            args: vec![args[0].clone()],
            display: "count",
            func_name: "countdistinct",
            columns: vec![columns[0].clone()],
            expect: DataValue::UInt64(Some(4)),
            error: "",
        },
        Test {
            name: "sum-distinct-passed",
            args: vec![args[0].clone()],
            display: "sum",
            func_name: "sumdistinct",
            columns: vec![columns[0].clone()],
            expect: DataValue::UInt64(Some(10)),
            error: "",
        },
        Test {
            name: "count-if-passed",
            args: args.clone(),
            display: "count",
            func_name: "countif",
            columns: columns.clone(),
            expect: DataValue::UInt64(Some(10)),
            error: "",
        },
        Test {
            name: "min-if-passed",
            args: args.clone(),
            display: "min",
            func_name: "minif",
            columns: columns.clone(),
            expect: DataValue::UInt64(Some(1)),
            error: "",
        },
        Test {
            name: "max-if-passed",
            args: args.clone(),
            display: "max",
            func_name: "maxif",
            columns: columns.clone(),
            expect: DataValue::UInt64(Some(4)),
            error: "",
        },
        Test {
            name: "sum-if-passed",
            args: args.clone(),
            display: "sum",
            func_name: "sumif",
            columns: columns.clone(),
            expect: DataValue::UInt64(Some(30)),
            error: "",
        },
        Test {
            name: "avg-if-passed",
            args: args.clone(),
            display: "avg",
            func_name: "avgif",
            columns: columns.clone(),
            expect: DataValue::UInt64(Some(3)),
            error: "",
        },
    ];

    for t in tests {
        let rows = t.columns[0].len();

        let arena = Bump::new();
        let func = || -> Result<()> {
            // First.
            let func = AggregateFunctionFactory::get(t.func_name, t.args.clone())?;
            let place1 = func.allocate_state(&arena);
            func.accumulate(place1, &t.columns, rows)?;

            // Second.
            let place2 = func.allocate_state(&arena);
            func.accumulate(place2, &t.columns, rows)?;

            func.merge(place1, place2)?;
            let result = func.merge_result(place1)?;
            assert_eq!(
                format!("{:?}", t.expect),
                format!("{:?}", result),
                "{}",
                t.name
            );
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
fn test_aggregate_combinator_function_on_empty_data() -> Result<()> {
    struct Test {
        name: &'static str,
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
        DataField::new("b", DataType::Boolean, true),
    ];

    let tests = vec![
        Test {
            name: "count-distinct-passed",
            args: vec![args[0].clone()],
            display: "count",
            func_name: "countdistinct",
            columns: vec![columns[0].clone()],
            expect: DataValue::UInt64(Some(0)),
            error: "",
        },
        Test {
            name: "sum-distinct-passed",
            args: vec![args[0].clone()],
            display: "sum",
            func_name: "sumdistinct",
            columns: vec![columns[0].clone()],
            expect: DataValue::Int64(None),
            error: "",
        },
        Test {
            name: "count-if-passed",
            args: args.clone(),
            display: "count",
            func_name: "countif",
            columns: columns.clone(),
            expect: DataValue::UInt64(Some(0)),
            error: "",
        },
        Test {
            name: "min-if-passed",
            args: args.clone(),
            display: "min",
            func_name: "minif",
            columns: columns.clone(),
            expect: DataValue::Int64(None),
            error: "",
        },
        Test {
            name: "max-if-passed",
            args: args.clone(),
            display: "max",
            func_name: "maxif",
            columns: columns.clone(),
            expect: DataValue::Int64(None),
            error: "",
        },
        Test {
            name: "sum-if-passed",
            args: args.clone(),
            display: "sum",
            func_name: "sumif",
            columns: columns.clone(),
            expect: DataValue::Int64(None),
            error: "",
        },
        Test {
            name: "avg-if-passed",
            args: args.clone(),
            display: "avg",
            func_name: "avgif",
            columns: columns.clone(),
            expect: DataValue::Float64(None),
            error: "",
        },
    ];

    for t in tests {
        let rows = t.columns[0].len();

        let arena = Bump::new();
        let func = || -> Result<()> {
            // First.
            let func = AggregateFunctionFactory::get(t.func_name, t.args.clone())?;
            let place1 = func.allocate_state(&arena);
            func.accumulate(place1, &t.columns, rows)?;

            // Second.
            let place2 = func.allocate_state(&arena);
            func.accumulate(place2, &t.columns, rows)?;
            func.merge(place1, place2)?;

            let result = func.merge_result(place1)?;
            assert_eq!(
                format!("{:?}", t.expect),
                format!("{:?}", result),
                "{}",
                t.name
            );
            assert_eq!(t.display, format!("{:}", func), "{}", t.name);
            Ok(())
        };

        if let Err(e) = func() {
            assert_eq!(t.error, e.to_string());
        }
    }
    Ok(())
}
