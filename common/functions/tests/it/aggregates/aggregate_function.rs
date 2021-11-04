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
use common_functions::aggregates::*;
use float_cmp::approx_eq;
use pretty_assertions::assert_eq;

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
        // arrays for window funnel function
        Series::new(vec![1, 0u32, 2, 3]),
        Series::new(vec![true, false, false, false]),
        Series::new(vec![false, false, true, false]),
        Series::new(vec![false, false, false, true]),
    ];

    let args = vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Int64, false),
        // args for window funnel function
        DataField::new("dt", DataType::DateTime32(None), false),
        DataField::new("event = 1001", DataType::Boolean, false),
        DataField::new("event = 1002", DataType::Boolean, false),
        DataField::new("event = 1003", DataType::Boolean, false),
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
            eval_nums: 2,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "argmax",
            func_name: "argmax",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            expect: DataValue::Int64(Some(1)),
            error: "",
        },
        Test {
            name: "argMin-passed",
            eval_nums: 2,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "argmin",
            func_name: "argmin",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
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
        Test {
            name: "covar-sample-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "covar_samp",
            func_name: "covar_samp",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            expect: DataValue::Float64(Some(-1.6666666666666667)),
            error: "",
        },
        Test {
            name: "covar-pop-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "covar_pop",
            func_name: "covar_pop",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            expect: DataValue::Float64(Some(-1.25000)),
            error: "",
        },
        Test {
            name: "windowFunnel-passed",
            eval_nums: 2,
            params: vec![DataValue::UInt64(Some(2))],
            args: vec![
                args[2].clone(),
                args[3].clone(),
                args[4].clone(),
                args[5].clone(),
            ],
            display: "windowFunnel",
            func_name: "windowFunnel",
            arrays: vec![
                arrays[2].clone(),
                arrays[3].clone(),
                arrays[4].clone(),
                arrays[5].clone(),
            ],
            expect: DataValue::UInt8(Some(3)),
            error: "",
        },
    ];

    for t in tests {
        let arena = Bump::new();
        let rows = t.arrays[0].len();

        let func = || -> Result<()> {
            let factory = AggregateFunctionFactory::instance();
            let func = factory.get(t.func_name, t.params.clone(), t.args.clone())?;

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
fn test_aggregate_function_with_grpup_by() -> Result<()> {
    struct Test {
        name: &'static str,
        eval_nums: usize,
        params: Vec<DataValue>,
        args: Vec<DataField>,
        display: &'static str,
        arrays: Vec<Series>,
        expect: Vec<DataValue>,
        error: &'static str,
        func_name: &'static str,
    }

    let arrays: Vec<Series> = vec![
        Series::new(vec![4i64, 3, 2, 1]),
        Series::new(vec![1i64, 2, 3, 4]),
        Series::new(vec!["a", "b", "c", "d"]),
        // arrays for window funnel function
        Series::new(vec![0u32, 2, 1, 3]),
        Series::new(vec![true, true, false, false]),
        Series::new(vec![false, false, true, false]),
        Series::new(vec![false, false, false, false]),
    ];

    let args = vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Int64, false),
        DataField::new("c", DataType::String, false),
        // args for window funnel function
        DataField::new("dt", DataType::DateTime32(None), false),
        DataField::new("event = 1001", DataType::Boolean, false),
        DataField::new("event = 1002", DataType::Boolean, false),
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
            expect: vec![DataValue::UInt64(Some(2)), DataValue::UInt64(Some(2))],
            error: "",
        },
        Test {
            name: "max-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "max",
            func_name: "max",
            arrays: vec![arrays[0].clone()],
            expect: vec![DataValue::Int64(Some(4)), DataValue::Int64(Some(3))],
            error: "",
        },
        Test {
            name: "min-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "min",
            func_name: "min",
            arrays: vec![arrays[0].clone()],
            expect: vec![DataValue::Int64(Some(2)), DataValue::Int64(Some(1))],
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
            expect: vec![DataValue::Float64(Some(3.0)), DataValue::Float64(Some(2.0))],
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
            expect: vec![DataValue::Int64(Some(6)), DataValue::Int64(Some(4))],
            error: "",
        },
        Test {
            name: "argMax-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "argmax",
            func_name: "argmax",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            expect: vec![DataValue::Int64(Some(2)), DataValue::Int64(Some(1))],
            error: "",
        },
        Test {
            name: "argMax-string-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[2].clone()],
            display: "argmax",
            func_name: "argmax",
            arrays: vec![arrays[0].clone(), arrays[2].clone()],
            expect: vec![DataValue::Int64(Some(2)), DataValue::Int64(Some(1))],
            error: "",
        },
        Test {
            name: "argMin-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "argmin",
            func_name: "argmin",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            expect: vec![DataValue::Int64(Some(4)), DataValue::Int64(Some(3))],
            error: "",
        },
        Test {
            name: "argMin-string-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "argmin",
            func_name: "argmin",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            expect: vec![DataValue::Int64(Some(4)), DataValue::Int64(Some(3))],
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
            expect: vec![DataValue::UInt64(Some(2)), DataValue::UInt64(Some(2))],
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
            expect: vec![DataValue::Float64(Some(1.0)), DataValue::Float64(Some(1.0))],
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
            expect: vec![DataValue::Float64(Some(1.0)), DataValue::Float64(Some(1.0))],
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
            expect: vec![DataValue::Float64(Some(1.0)), DataValue::Float64(Some(1.0))],
            error: "",
        },
        Test {
            name: "covar-sample-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "covar_samp",
            func_name: "covar_samp",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            expect: vec![
                DataValue::Float64(Some(-2.0)),
                DataValue::Float64(Some(-2.0)),
            ],
            error: "",
        },
        Test {
            name: "covar-pop-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "covar_pop",
            func_name: "covar_pop",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            expect: vec![
                DataValue::Float64(Some(-1.0)),
                DataValue::Float64(Some(-1.0)),
            ],
            error: "",
        },
        Test {
            name: "windowFunnel-passed",
            eval_nums: 1,
            params: vec![DataValue::UInt64(Some(2))],
            args: vec![args[3].clone(), args[4].clone(), args[5].clone()],
            display: "windowFunnel",
            func_name: "windowFunnel",
            arrays: vec![
                arrays[3].clone(),
                arrays[4].clone(),
                arrays[5].clone(),
                arrays[6].clone(),
            ],
            expect: vec![DataValue::UInt8(Some(2)), DataValue::UInt8(Some(1))],
            error: "",
        },
    ];

    for t in tests {
        let arena = Bump::new();
        let rows = t.arrays[0].len();

        let func = || -> Result<()> {
            let factory = AggregateFunctionFactory::instance();
            let func = factory.get(t.func_name, t.params.clone(), t.args.clone())?;

            let addr1 = arena.alloc_layout(func.state_layout());
            func.init_state(addr1.into());
            let addr2 = arena.alloc_layout(func.state_layout());
            func.init_state(addr2.into());
            let places = vec![addr1.into(), addr2.into(), addr1.into(), addr2.into()];

            for _ in 0..t.eval_nums {
                func.accumulate_keys(&places, 0, &t.arrays, rows)?;
            }

            let result = vec![
                func.merge_result(addr1.into())?,
                func.merge_result(addr2.into())?,
            ];
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
        Test {
            name: "covar-sample-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "covar_samp",
            func_name: "covar_samp",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            expect: DataValue::Float64(Some(f64::INFINITY)),
            error: "",
        },
        Test {
            name: "covar-pop-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "covar_pop",
            func_name: "covar_pop",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            expect: DataValue::Float64(Some(f64::INFINITY)),
            error: "",
        },
    ];

    for t in tests {
        let arena = Bump::new();
        let rows = t.arrays[0].len();

        let func = || -> Result<()> {
            let factory = AggregateFunctionFactory::instance();
            let func = factory.get(t.func_name, t.params.clone(), t.args.clone())?;
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
fn test_covariance_with_comparable_data_sets() -> Result<()> {
    let arena = Bump::new();

    let mut v0 = Vec::with_capacity(2000);
    for _i in 0..2000 {
        v0.push(1.0_f32)
    }

    let mut v1 = Vec::with_capacity(2000);
    for i in 0..2000 {
        v1.push(i as i16);
    }

    let arrays: Vec<Series> = vec![Series::new(v0), Series::new(v1)];

    let args = vec![
        DataField::new("a", DataType::Float32, false),
        DataField::new("b", DataType::Int16, false),
    ];

    let factory = AggregateFunctionFactory::instance();

    let run_test = |func_name: &'static str| -> Result<f64> {
        let func = factory.get(func_name, vec![], args.clone())?;
        let addr = arena.alloc_layout(func.state_layout());
        func.init_state(addr.into());
        func.accumulate(addr.into(), &arrays, 2000)?;
        let result = func.merge_result(addr.into())?;
        match result {
            DataValue::Float64(Some(val)) => Ok(val),
            _ => {
                panic!();
            }
        }
    };

    let r = run_test("covar_samp")?;
    approx_eq!(f64, 0.0, r, epsilon = 0.000001);

    let r = run_test("covar_pop")?;
    approx_eq!(f64, 0.0, r, epsilon = 0.000001);

    Ok(())
}
