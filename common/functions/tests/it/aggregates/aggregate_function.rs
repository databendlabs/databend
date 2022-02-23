// Copyright 2021 Datafuse Labs.
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

use std::borrow::BorrowMut;

use bumpalo::Bump;
use common_datavalues::prelude::*;
use common_datavalues::with_match_primitive_type_id;
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
        arrays: Vec<ColumnRef>,
        error: &'static str,
        func_name: &'static str,
        input_array: Box<dyn MutableColumn>,
        expect_array: Box<dyn MutableColumn>,
    }

    let arrays: Vec<ColumnRef> = vec![
        Series::from_data(vec![4i64, 3, 2, 1]),
        Series::from_data(vec![1i64, 2, 3, 4]),
        // arrays for window funnel function
        Series::from_data(vec![1, 0u32, 2, 3]),
        Series::from_data(vec![true, false, false, false]),
        Series::from_data(vec![false, false, false, false]),
        Series::from_data(vec![false, false, false, false]),
    ];

    let args = vec![
        DataField::new("a", i64::to_data_type()),
        DataField::new("b", i64::to_data_type()),
        // args for window funnel function
        DataField::new("dt", DateTime32Type::arc(None)),
        DataField::new("event = 1001", bool::to_data_type()),
        DataField::new("event = 1002", bool::to_data_type()),
        DataField::new("event = 1003", bool::to_data_type()),
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
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<u64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<u64>::from_data(
                u64::to_data_type(),
                Vec::from([4u64]),
            )),
        },
        Test {
            name: "max-passed",
            eval_nums: 2,
            params: vec![],
            args: vec![args[0].clone()],
            display: "max",
            func_name: "max",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([4i64]),
            )),
        },
        Test {
            name: "min-passed",
            eval_nums: 2,
            params: vec![],
            args: vec![args[0].clone()],
            display: "min",
            func_name: "min",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([1i64]),
            )),
        },
        Test {
            name: "avg-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "avg",
            func_name: "avg",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<f64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<f64>::from_data(
                f64::to_data_type(),
                Vec::from([2.5f64]),
            )),
        },
        Test {
            name: "sum-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "sum",
            func_name: "sum",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([10i64]),
            )),
        },
        Test {
            name: "argMax-passed",
            eval_nums: 2,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "argmax",
            func_name: "argmax",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([1i64]),
            )),
        },
        Test {
            name: "argMin-passed",
            eval_nums: 2,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "argmin",
            func_name: "argmin",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([4i64]),
            )),
        },
        Test {
            name: "argMin-notpassed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "argmin",
            func_name: "argmin",
            arrays: arrays.clone(),
            error: "Code: 1028, displayText = argmin expect to have two arguments, but got 1.",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([4i64]),
            )),
        },
        Test {
            name: "uniq-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "uniq",
            func_name: "uniq",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<u64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<u64>::from_data(
                u64::to_data_type(),
                Vec::from([4u64]),
            )),
        },
        Test {
            name: "std-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "std",
            func_name: "std",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<f64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<f64>::from_data(
                f64::to_data_type(),
                Vec::from([1.118033988749895f64]),
            )),
        },
        Test {
            name: "stddev-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "stddev",
            func_name: "stddev",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<f64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<f64>::from_data(
                f64::to_data_type(),
                Vec::from([1.118033988749895f64]),
            )),
        },
        Test {
            name: "stddev-pop-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "stddev_pop",
            func_name: "stddev_pop",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<f64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<f64>::from_data(
                f64::to_data_type(),
                Vec::from([1.118033988749895f64]),
            )),
        },
        Test {
            name: "covar-sample-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "covar_samp",
            func_name: "covar_samp",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<f64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<f64>::from_data(
                f64::to_data_type(),
                Vec::from([-1.6666666666666667f64]),
            )),
        },
        Test {
            name: "covar-pop-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "covar_pop",
            func_name: "covar_pop",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<f64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<f64>::from_data(
                f64::to_data_type(),
                Vec::from([-1.25000f64]),
            )),
        },
        Test {
            name: "windowFunnel-passed",
            eval_nums: 2,
            params: vec![DataValue::UInt64(2)],
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
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<u8>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<u8>::from_data(
                u8::to_data_type(),
                Vec::from([1u8]),
            )),
        },
    ];

    for mut t in tests {
        let arena = Bump::new();
        let rows = t.arrays[0].len();

        let mut func = || -> Result<()> {
            let factory = AggregateFunctionFactory::instance();
            let func = factory.get(t.func_name, t.params.clone(), t.args.clone())?;

            let addr1 = arena.alloc_layout(func.state_layout());
            func.init_state(addr1.into());

            for _ in 0..t.eval_nums {
                func.accumulate(addr1.into(), &t.arrays, None, rows)?;
            }

            let addr2 = arena.alloc_layout(func.state_layout());
            func.init_state(addr2.into());

            for _ in 1..t.eval_nums {
                func.accumulate(addr2.into(), &t.arrays, None, rows)?;
            }

            func.merge(addr1.into(), addr2.into())?;
            {
                let array: &mut dyn MutableColumn = t.input_array.borrow_mut();
                let _ = func.merge_result(addr1.into(), array)?;
            }

            let datatype = t.input_array.data_type();
            with_match_primitive_type_id!(datatype.data_type_id(), |$T| {
                let array = t
                        .input_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveColumn<$T>>()
                        .unwrap();
                let expect = t
                        .expect_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveColumn<$T>>()
                        .unwrap();

                assert_eq!(array.data_type(), expect.data_type(), "{}", t.name);
                assert_eq!(array.values(), expect.values(), "{}", t.name);
            },
            {
                panic!("shoud never reach this way");
            });

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
        arrays: Vec<ColumnRef>,
        error: &'static str,
        func_name: &'static str,
        input_array: Box<dyn MutableColumn>,
        expect_array: Box<dyn MutableColumn>,
    }

    let arrays: Vec<ColumnRef> = vec![
        Series::from_data(vec![4i64, 3, 2, 1]),
        Series::from_data(vec![1i64, 2, 3, 4]),
        Series::from_data(vec!["a", "b", "c", "d"]),
        // arrays for window funnel function
        Series::from_data(vec![0u32, 2, 1, 3]),
        Series::from_data(vec![true, false, false, false]),
        Series::from_data(vec![false, false, false, false]),
        Series::from_data(vec![false, false, false, false]),
    ];

    let args = vec![
        DataField::new("a", i64::to_data_type()),
        DataField::new("b", i64::to_data_type()),
        DataField::new("c", Vu8::to_data_type()),
        // args for window funnel function
        DataField::new("dt", DateTime32Type::arc(None)),
        DataField::new("event = 1001", bool::to_data_type()),
        DataField::new("event = 1002", bool::to_data_type()),
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
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<u64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<u64>::from_data(
                u64::to_data_type(),
                Vec::from([2u64, 2u64]),
            )),
        },
        Test {
            name: "max-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "max",
            func_name: "max",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([4i64, 3i64]),
            )),
        },
        Test {
            name: "min-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "min",
            func_name: "min",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([2i64, 1i64]),
            )),
        },
        Test {
            name: "avg-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "avg",
            func_name: "avg",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<f64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<f64>::from_data(
                f64::to_data_type(),
                Vec::from([3.0f64, 2.0f64]),
            )),
        },
        Test {
            name: "sum-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "sum",
            func_name: "sum",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([6i64, 4i64]),
            )),
        },
        Test {
            name: "argMax-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "argmax",
            func_name: "argmax",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([2i64, 1i64]),
            )),
        },
        Test {
            name: "argMax-string-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[2].clone()],
            display: "argmax",
            func_name: "argmax",
            arrays: vec![arrays[0].clone(), arrays[2].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([2i64, 1i64]),
            )),
        },
        Test {
            name: "argMin-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "argmin",
            func_name: "argmin",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([4i64, 3i64]),
            )),
        },
        Test {
            name: "argMin-string-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "argmin",
            func_name: "argmin",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([4i64, 3i64]),
            )),
        },
        Test {
            name: "uniq-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "uniq",
            func_name: "uniq",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<u64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<u64>::from_data(
                u64::to_data_type(),
                Vec::from([2u64, 2u64]),
            )),
        },
        Test {
            name: "std-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "std",
            func_name: "std",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<f64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<f64>::from_data(
                f64::to_data_type(),
                Vec::from([1.0f64, 1.0f64]),
            )),
        },
        Test {
            name: "stddev-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "stddev",
            func_name: "stddev",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<f64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<f64>::from_data(
                f64::to_data_type(),
                Vec::from([1.0f64, 1.0f64]),
            )),
        },
        Test {
            name: "stddev-pop-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "stddev_pop",
            func_name: "stddev_pop",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<f64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<f64>::from_data(
                f64::to_data_type(),
                Vec::from([1.0f64, 1.0f64]),
            )),
        },
        Test {
            name: "covar-sample-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "covar_samp",
            func_name: "covar_samp",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<f64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<f64>::from_data(
                f64::to_data_type(),
                Vec::from([-2.0f64, -2.0f64]),
            )),
        },
        Test {
            name: "covar-pop-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "covar_pop",
            func_name: "covar_pop",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<f64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<f64>::from_data(
                f64::to_data_type(),
                Vec::from([-1.0f64, -1.0f64]),
            )),
        },
        Test {
            name: "windowFunnel-passed",
            eval_nums: 1,
            params: vec![DataValue::UInt64(2)],
            args: vec![args[3].clone(), args[4].clone(), args[5].clone()],
            display: "windowFunnel",
            func_name: "windowFunnel",
            arrays: vec![
                arrays[3].clone(),
                arrays[4].clone(),
                arrays[5].clone(),
                arrays[6].clone(),
            ],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<u8>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<u8>::from_data(
                u8::to_data_type(),
                Vec::from([1u8, 0u8]),
            )),
        },
    ];

    for mut t in tests {
        let arena = Bump::new();
        let rows = t.arrays[0].len();

        let mut func = || -> Result<()> {
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

            let array: &mut dyn MutableColumn = t.input_array.borrow_mut();

            let _ = func.merge_result(addr1.into(), array)?;
            let _ = func.merge_result(addr2.into(), array)?;

            let datatype = t.input_array.data_type();
            with_match_primitive_type_id!(datatype.data_type_id(), |$T| {
                let array = t
                        .input_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveColumn<$T>>()
                        .unwrap();
                let expect = t
                        .expect_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveColumn<$T>>()
                        .unwrap();

                assert_eq!(array.data_type(), expect.data_type(), "{}", t.name);
                assert_eq!(array.values(), expect.values(), "{}", t.name);
            },
            {
                panic!("shoud never reach this way");
            });

            assert_eq!(t.display, format!("{:}", func), "{}", t.name);
            Ok(())
        };

        if let Err(e) = func() {
            assert_eq!(t.error, e.to_string(), "{}", t.name);
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
        arrays: Vec<ColumnRef>,
        error: &'static str,
        func_name: &'static str,
        input_array: Box<dyn MutableColumn>,
        expect_array: Box<dyn MutableColumn>,
    }

    let arrays: Vec<ColumnRef> = vec![
        Int64Column::from_slice(&[]).arc(),
        BooleanColumn::from_slice(&[]).arc(),
    ];

    let args = vec![
        DataField::new("a", i64::to_data_type()),
        DataField::new("b", i64::to_data_type()),
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
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<u64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<u64>::from_data(
                u64::to_data_type(),
                Vec::from([0u64]),
            )),
        },
        Test {
            name: "max-passed",
            eval_nums: 2,
            params: vec![],
            args: vec![args[0].clone()],
            display: "max",
            func_name: "max",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([0i64]),
            )),
        },
        Test {
            name: "min-passed",
            eval_nums: 2,
            params: vec![],
            args: vec![args[0].clone()],
            display: "min",
            func_name: "min",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([0i64]),
            )),
        },
        Test {
            name: "sum-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "sum",
            func_name: "sum",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([0i64]),
            )),
        },
        Test {
            name: "argMax-passed",
            eval_nums: 1,
            params: vec![],
            args: args.clone(),
            display: "argmax",
            func_name: "argmax",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([0i64]),
            )),
        },
        Test {
            name: "argMin-passed",
            eval_nums: 1,
            params: vec![],
            args: args.clone(),
            display: "argmin",
            func_name: "argmin",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([0i64]),
            )),
        },
        Test {
            name: "uniq-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone()],
            display: "uniq",
            func_name: "uniq",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<u64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<u64>::from_data(
                u64::to_data_type(),
                Vec::from([0u64]),
            )),
        },
        Test {
            name: "covar-sample-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "covar_samp",
            func_name: "covar_samp",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<f64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<f64>::from_data(
                f64::to_data_type(),
                Vec::from([f64::INFINITY]),
            )),
        },
        Test {
            name: "covar-pop-passed",
            eval_nums: 1,
            params: vec![],
            args: vec![args[0].clone(), args[1].clone()],
            display: "covar_pop",
            func_name: "covar_pop",
            arrays: vec![arrays[0].clone(), arrays[1].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<f64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<f64>::from_data(
                f64::to_data_type(),
                Vec::from([f64::INFINITY]),
            )),
        },
    ];

    for mut t in tests {
        let arena = Bump::new();
        let rows = t.arrays[0].len();

        let mut func = || -> Result<()> {
            let factory = AggregateFunctionFactory::instance();
            let func = factory.get(t.func_name, t.params.clone(), t.args.clone())?;
            let addr1 = arena.alloc_layout(func.state_layout());
            func.init_state(addr1.into());

            for _ in 0..t.eval_nums {
                func.accumulate(addr1.into(), &t.arrays, None, rows)?;
            }

            let addr2 = arena.alloc_layout(func.state_layout());
            func.init_state(addr2.into());
            for _ in 1..t.eval_nums {
                func.accumulate(addr2.into(), &t.arrays, None, rows)?;
            }

            func.merge(addr1.into(), addr2.into())?;
            let array: &mut dyn MutableColumn = t.input_array.borrow_mut();
            let _ = func.merge_result(addr1.into(), array)?;

            let datatype = t.input_array.data_type();
            with_match_primitive_type_id!(datatype.data_type_id(), |$T| {
                let array = t
                        .input_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveColumn<$T>>()
                        .unwrap();
                let expect = t
                        .expect_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveColumn<$T>>()
                        .unwrap();

                assert_eq!(array.data_type(), expect.data_type(), "{}", t.name);
                assert_eq!(array.values(), expect.values(), "{}", t.name);
            },
            {
                panic!("shoud never reach this way");
            });

            // assert_eq!(&t.expect, &result, "{}", t.name);
            assert_eq!(t.display, format!("{:}", func), "{}", t.name);
            Ok(())
        };

        if let Err(e) = func() {
            assert_eq!(t.error, e.to_string(), "{}", t.name);
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

    let arrays: Vec<ColumnRef> = vec![Series::from_data(v0), Series::from_data(v1)];

    let args = vec![
        DataField::new("a", i32::to_data_type()),
        DataField::new("b", i16::to_data_type()),
    ];

    let factory = AggregateFunctionFactory::instance();

    let run_test = |func_name: &'static str, array: &mut dyn MutableColumn| -> Result<f64> {
        let func = factory.get(func_name, vec![], args.clone())?;
        let addr = arena.alloc_layout(func.state_layout());
        func.init_state(addr.into());
        func.accumulate(addr.into(), &arrays, None, 2000)?;
        let _ = func.merge_result(addr.into(), array)?;
        let array = array
            .as_mut_any()
            .downcast_ref::<MutablePrimitiveColumn<f64>>()
            .unwrap();
        let val = array.values()[0];
        Ok(val)
    };

    let mut array = MutablePrimitiveColumn::<f64>::default();
    let r = run_test("covar_samp", &mut array)?;
    approx_eq!(f64, 0.0, r, epsilon = 0.000001);

    let mut array = MutablePrimitiveColumn::<f64>::default();
    let r = run_test("covar_pop", &mut array)?;
    approx_eq!(f64, 0.0, r, epsilon = 0.000001);

    Ok(())
}
