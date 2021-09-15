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
use pretty_assertions::assert_eq;

use crate::aggregates::*;

#[test]
fn test_aggregate_function() -> Result<()> {
    struct Test {
        name: &'static str,
        eval_nums: usize,
        params: Vec<DataValue>,
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
            params: vec![],
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
            params: vec![],
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
            params: vec![],
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
            params: vec![],
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
            params: vec![],
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
            params: vec![],
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
            params: vec![],
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
            params: vec![],
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
            params: vec![],
            args: vec![args[0].clone()],
            display: "uniq",
            func_name: "uniq",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::UInt64(Some(4)),
            error: "",
        },
        Test {
            name: "std-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "std",
            func_name: "std",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::Float64(Some(1.118033988749895)),
            error: "",
        },
        Test {
            name: "stddev-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "stddev",
            func_name: "stddev",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::Float64(Some(1.118033988749895)),
            error: "",
        },
        Test {
            name: "stddev-pop-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "stddev_pop",
            func_name: "stddev_pop",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::Float64(Some(1.118033988749895)),
            error: "",
        },
    ];

    for t in tests {
        let arena = Bump::new();
        let rows = t.arrays[0].len();

        let func = || -> Result<()> {
            let func =
                AggregateFunctionFactory::get(t.func_name, t.params.clone(), t.args.clone())?;

            let addr1 = arena.alloc_layout(func.state_layout());
            func.init_state(addr1.into());

            for _ in 0..t.eval_nums {
                func.accumulate(addr1.into(), &t.arrays, rows)?;
            }

            let addr2 = arena.alloc_layout(func.state_layout());
            func.init_state(addr2.into());

            for _ in 1..t.eval_nums {
                func.accumulate(addr2.into(), &t.arrays, rows)?;
            }

            func.merge(addr1.into(), addr2.into())?;
            let result = func.merge_result(addr1.into())?;
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
        DataField::new("b", DataType::Int64, true),
    ];

    let tests = vec![
        Test {
            name: "count-passed",
            eval_nums: 1,
            params: vec![],
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
            params: vec![],
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
            params: vec![],
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
            params: vec![],
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
            params: vec![],
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
            params: vec![],
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
            params: vec![],
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
            params: vec![],
            args: vec![args[0].clone()],
            display: "uniq",
            func_name: "uniq",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::UInt64(Some(0)),
            error: "",
        },
        Test {
            name: "std-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "std",
            func_name: "std",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::Float64(None),
            error: "",
        },
        Test {
            name: "stddev-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "stddev",
            func_name: "stddev",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::Float64(None),
            error: "",
        },
        Test {
            name: "stddev-pop-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "stddev_pop",
            func_name: "stddev_pop",
            arrays: vec![arrays[0].clone()],
            expect: DataValue::Float64(None),
            error: "",
        },
    ];

    for t in tests {
        let arena = Bump::new();
        let rows = t.arrays[0].len();

        let func = || -> Result<()> {
            let func =
                AggregateFunctionFactory::get(t.func_name, t.params.clone(), t.args.clone())?;
            let addr1 = arena.alloc_layout(func.state_layout());
            func.init_state(addr1.into());

            for _ in 0..t.eval_nums {
                func.accumulate(addr1.into(), &t.arrays, rows)?;
            }

            let addr2 = arena.alloc_layout(func.state_layout());
            func.init_state(addr2.into());
            for _ in 1..t.eval_nums {
                func.accumulate(addr2.into(), &t.arrays, rows)?;
            }

            func.merge(addr1.into(), addr2.into())?;
            let result = func.merge_result(addr1.into())?;

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
