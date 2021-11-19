// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bumpalo::Bump;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use pretty_assertions::assert_eq;

#[test]
fn test_aggregate_combinator_function() -> Result<()> {
    struct Test {
        name: &'static str,
        params: Vec<DataValue>,
        args: Vec<DataField>,
        display: &'static str,
        arrays: Vec<Series>,
        expect: DataValue,
        error: &'static str,
        func_name: &'static str,
    }

    let arrays: Vec<Series> = vec![
        Series::new(vec![4_i64, 3, 2, 1, 3, 4]),
        Series::new(vec![true, true, false, true, true, true]),
    ];

    let args = vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Boolean, false),
    ];

    let tests = vec![
        Test {
            name: "count-distinct-passed",
            params: vec![],
            args: vec![args[0].clone()],
            display: "count",
            func_name: "countdistinct",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::UInt64(Some(4)),
            error: "",
        },
        Test {
            name: "sum-distinct-passed",
            params: vec![],
            args: vec![args[0].clone()],
            display: "sum",
            func_name: "sumdistinct",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::UInt64(Some(10)),
            error: "",
        },
        Test {
            name: "count-if-passed",
            params: vec![],
            args: args.clone(),
            display: "count",
            func_name: "countif",
            arrays: arrays.clone(),
            expect: DataValue::UInt64(Some(10)),
            error: "",
        },
        Test {
            name: "min-if-passed",
            params: vec![],
            args: args.clone(),
            display: "min",
            func_name: "minif",
            arrays: arrays.clone(),
            expect: DataValue::UInt64(Some(1)),
            error: "",
        },
        Test {
            name: "max-if-passed",
            params: vec![],
            args: args.clone(),
            display: "max",
            func_name: "maxif",
            arrays: arrays.clone(),
            expect: DataValue::UInt64(Some(4)),
            error: "",
        },
        Test {
            name: "sum-if-passed",
            params: vec![],
            args: args.clone(),
            display: "sum",
            func_name: "sumif",
            arrays: arrays.clone(),
            expect: DataValue::UInt64(Some(30)),
            error: "",
        },
        Test {
            name: "avg-if-passed",
            params: vec![],
            args: args.clone(),
            display: "avg",
            func_name: "avgif",
            arrays: arrays.clone(),
            expect: DataValue::UInt64(Some(3)),
            error: "",
        },
    ];

    for t in tests {
        let rows = t.arrays[0].len();

        let arena = Bump::new();
        let func = || -> Result<()> {
            // First.
            let factory = AggregateFunctionFactory::instance();
            let func = factory.get(t.func_name, t.params.clone(), t.args.clone())?;
            let addr = arena.alloc_layout(func.state_layout());
            func.init_state(addr.into());
            func.accumulate(addr.into(), &t.arrays, rows)?;

            // Second.
            let addr2 = arena.alloc_layout(func.state_layout());
            func.init_state(addr2.into());
            func.accumulate(addr2.into(), &t.arrays, rows)?;

            func.merge(addr.into(), addr2.into())?;
            let result = func.merge_result(addr.into())?;
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
        params: Vec<DataValue>,
        args: Vec<DataField>,
        display: &'static str,
        arrays: Vec<Series>,
        expect: DataValue,
        error: &'static str,
        func_name: &'static str,
    }

    let arrays: Vec<Series> = vec![
        DFInt64Array::new_from_slice(&[]).into_series(),
        DFBooleanArray::new_from_slice(&[]).into_series(),
    ];

    let args = vec![
        DataField::new("a", DataType::Int64, true),
        DataField::new("b", DataType::Boolean, true),
    ];

    let tests = vec![
        Test {
            name: "count-distinct-passed",
            params: vec![],
            args: vec![args[0].clone()],
            display: "count",
            func_name: "countdistinct",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::UInt64(Some(0)),
            error: "",
        },
        Test {
            name: "sum-distinct-passed",
            params: vec![],
            args: vec![args[0].clone()],
            display: "sum",
            func_name: "sumdistinct",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::Int64(None),
            error: "",
        },
        Test {
            name: "count-if-passed",
            params: vec![],
            args: args.clone(),
            display: "count",
            func_name: "countif",
            arrays: arrays.clone(),
            expect: DataValue::UInt64(Some(0)),
            error: "",
        },
        Test {
            name: "min-if-passed",
            params: vec![],
            args: args.clone(),
            display: "min",
            func_name: "minif",
            arrays: arrays.clone(),
            expect: DataValue::Int64(None),
            error: "",
        },
        Test {
            name: "max-if-passed",
            params: vec![],
            args: args.clone(),
            display: "max",
            func_name: "maxif",
            arrays: arrays.clone(),
            expect: DataValue::Int64(None),
            error: "",
        },
        Test {
            name: "sum-if-passed",
            params: vec![],
            args: args.clone(),
            display: "sum",
            func_name: "sumif",
            arrays: arrays.clone(),
            expect: DataValue::Int64(None),
            error: "",
        },
        Test {
            name: "avg-if-passed",
            params: vec![],
            args: args.clone(),
            display: "avg",
            func_name: "avgif",
            arrays: arrays.clone(),
            expect: DataValue::Float64(None),
            error: "",
        },
    ];

    for t in tests {
        let rows = t.arrays[0].len();

        let arena = Bump::new();
        let func = || -> Result<()> {
            // First.
            let factory = AggregateFunctionFactory::instance();
            let func = factory.get(t.func_name, t.params.clone(), t.args.clone())?;
            let addr1 = arena.alloc_layout(func.state_layout());
            func.init_state(addr1.into());

            func.accumulate(addr1.into(), &t.arrays, rows)?;

            // Second.
            let addr2 = arena.alloc_layout(func.state_layout());
            func.init_state(addr2.into());
            func.accumulate(addr2.into(), &t.arrays, rows)?;
            func.merge(addr1.into(), addr2.into())?;

            let result = func.merge_result(addr1.into())?;
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
