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
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::buffer::MutableBuffer;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::with_match_primitive_type;
use pretty_assertions::assert_eq;

#[test]
fn test_aggregate_combinator_function() -> Result<()> {
    struct Test {
        name: &'static str,
        params: Vec<DataValue>,
        args: Vec<DataField>,
        display: &'static str,
        arrays: Vec<Series>,
        error: &'static str,
        func_name: &'static str,
        input_array: Box<dyn MutableArrayBuilder>,
        expect_array: Box<dyn MutableArrayBuilder>,
    }

    let arrays: Vec<Series> = vec![
        Series::new(vec![4_i64, 3, 2, 1, 3, 4]),
        Series::new(vec![true, true, false, true, true, true]),
    ];

    let args = vec![
        DataField::new("a", DataType::Int64(false), false),
        DataField::new("b", DataType::Boolean(false), false),
    ];

    let tests = vec![
        Test {
            name: "count-distinct-passed",
            params: vec![],
            args: vec![args[0].clone()],
            display: "count",
            func_name: "countdistinct",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveArrayBuilder::<u64, true>::default()),
            expect_array: Box::new(MutablePrimitiveArrayBuilder::<u64, true>::from_data(
                DataType::UInt64(true),
                MutableBuffer::from([4u64]),
                Some(MutableBitmap::from([true])),
            )),
        },
        Test {
            name: "sum-distinct-passed",
            params: vec![],
            args: vec![args[0].clone()],
            display: "sum",
            func_name: "sumdistinct",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveArrayBuilder::<i64, true>::default()),
            expect_array: Box::new(MutablePrimitiveArrayBuilder::<i64, true>::from_data(
                DataType::Int64(true),
                MutableBuffer::from([10i64]),
                Some(MutableBitmap::from([true])),
            )),
        },
        Test {
            name: "count-if-passed",
            params: vec![],
            args: args.clone(),
            display: "count",
            func_name: "countif",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveArrayBuilder::<u64, true>::default()),
            expect_array: Box::new(MutablePrimitiveArrayBuilder::<u64, true>::from_data(
                DataType::UInt64(true),
                MutableBuffer::from([10u64]),
                Some(MutableBitmap::from([true])),
            )),
        },
        Test {
            name: "min-if-passed",
            params: vec![],
            args: args.clone(),
            display: "min",
            func_name: "minif",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveArrayBuilder::<i64, true>::default()),
            expect_array: Box::new(MutablePrimitiveArrayBuilder::<i64, true>::from_data(
                DataType::Int64(true),
                MutableBuffer::from([1i64]),
                Some(MutableBitmap::from([true])),
            )),
        },
        Test {
            name: "max-if-passed",
            params: vec![],
            args: args.clone(),
            display: "max",
            func_name: "maxif",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveArrayBuilder::<i64, true>::default()),
            expect_array: Box::new(MutablePrimitiveArrayBuilder::<i64, true>::from_data(
                DataType::Int64(true),
                MutableBuffer::from([4i64]),
                Some(MutableBitmap::from([true])),
            )),
        },
        Test {
            name: "sum-if-passed",
            params: vec![],
            args: args.clone(),
            display: "sum",
            func_name: "sumif",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveArrayBuilder::<i64, true>::default()),
            expect_array: Box::new(MutablePrimitiveArrayBuilder::<i64, true>::from_data(
                DataType::Int64(true),
                MutableBuffer::from([30i64]),
                Some(MutableBitmap::from([true])),
            )),
        },
        Test {
            name: "avg-if-passed",
            params: vec![],
            args: args.clone(),
            display: "avg",
            func_name: "avgif",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveArrayBuilder::<f64, true>::default()),
            expect_array: Box::new(MutablePrimitiveArrayBuilder::<f64, true>::from_data(
                DataType::Float64(true),
                MutableBuffer::from([3f64]),
                Some(MutableBitmap::from([true])),
            )),
        },
    ];

    for mut t in tests {
        let rows = t.arrays[0].len();

        let arena = Bump::new();
        let mut func = || -> Result<()> {
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
            let array: &mut dyn MutableArrayBuilder = t.input_array.borrow_mut();
            let _ = func.merge_result(addr.into(), array)?;

            let datatype = t.input_array.data_type();
            with_match_primitive_type!(datatype, |$T| {
                let array = t
                        .input_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveArrayBuilder<$T, true>>()
                        .unwrap();
                let expect = t
                        .expect_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveArrayBuilder<$T, true>>()
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
fn test_aggregate_combinator_function_on_empty_data() -> Result<()> {
    struct Test {
        name: &'static str,
        params: Vec<DataValue>,
        args: Vec<DataField>,
        display: &'static str,
        arrays: Vec<Series>,
        error: &'static str,
        func_name: &'static str,
        input_array: Box<dyn MutableArrayBuilder>,
        expect_array: Box<dyn MutableArrayBuilder>,
    }

    let arrays: Vec<Series> = vec![
        DFInt64Array::new_from_slice(&[]).into_series(),
        DFBooleanArray::new_from_slice(&[]).into_series(),
    ];

    let args = vec![
        DataField::new("a", DataType::Int64(true), true),
        DataField::new("b", DataType::Boolean(true), true),
    ];

    let tests = vec![
        Test {
            name: "count-distinct-passed",
            params: vec![],
            args: vec![args[0].clone()],
            display: "count",
            func_name: "countdistinct",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveArrayBuilder::<u64, true>::default()),
            expect_array: Box::new(MutablePrimitiveArrayBuilder::<u64, true>::from_data(
                DataType::UInt64(true),
                MutableBuffer::from([0u64]),
                Some(MutableBitmap::from([true])),
            )),
        },
        Test {
            name: "sum-distinct-passed",
            params: vec![],
            args: vec![args[0].clone()],
            display: "sum",
            func_name: "sumdistinct",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveArrayBuilder::<i64, true>::default()),
            expect_array: Box::new(MutablePrimitiveArrayBuilder::<i64, true>::from_data(
                DataType::Int64(true),
                MutableBuffer::from([0i64]),
                Some(MutableBitmap::from([false])),
            )),
        },
        Test {
            name: "count-if-passed",
            params: vec![],
            args: args.clone(),
            display: "count",
            func_name: "countif",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveArrayBuilder::<u64, true>::default()),
            expect_array: Box::new(MutablePrimitiveArrayBuilder::<u64, true>::from_data(
                DataType::UInt64(true),
                MutableBuffer::from([0u64]),
                Some(MutableBitmap::from([true])),
            )),
        },
        Test {
            name: "min-if-passed",
            params: vec![],
            args: args.clone(),
            display: "min",
            func_name: "minif",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveArrayBuilder::<i64, true>::default()),
            expect_array: Box::new(MutablePrimitiveArrayBuilder::<i64, true>::from_data(
                DataType::Int64(true),
                MutableBuffer::from([0i64]),
                Some(MutableBitmap::from([false])),
            )),
        },
        Test {
            name: "max-if-passed",
            params: vec![],
            args: args.clone(),
            display: "max",
            func_name: "maxif",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveArrayBuilder::<i64, true>::default()),
            expect_array: Box::new(MutablePrimitiveArrayBuilder::<i64, true>::from_data(
                DataType::Int64(true),
                MutableBuffer::from([0i64]),
                Some(MutableBitmap::from([false])),
            )),
        },
        Test {
            name: "sum-if-passed",
            params: vec![],
            args: args.clone(),
            display: "sum",
            func_name: "sumif",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveArrayBuilder::<i64, true>::default()),
            expect_array: Box::new(MutablePrimitiveArrayBuilder::<i64, true>::from_data(
                DataType::Int64(true),
                MutableBuffer::from([0i64]),
                Some(MutableBitmap::from([false])),
            )),
        },
        Test {
            name: "avg-if-passed",
            params: vec![],
            args: args.clone(),
            display: "avg",
            func_name: "avgif",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveArrayBuilder::<f64, true>::default()),
            expect_array: Box::new(MutablePrimitiveArrayBuilder::<f64, true>::from_data(
                DataType::Float64(true),
                MutableBuffer::from([0f64]),
                Some(MutableBitmap::from([false])),
            )),
        },
    ];

    for mut t in tests {
        let rows = t.arrays[0].len();

        let arena = Bump::new();
        let mut func = || -> Result<()> {
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

            let array: &mut dyn MutableArrayBuilder = t.input_array.borrow_mut();
            let _ = func.merge_result(addr1.into(), array)?;

            let datatype = t.input_array.data_type();
            with_match_primitive_type!(datatype, |$T| {
                let array = t
                        .input_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveArrayBuilder<$T, true>>()
                        .unwrap();
                let expect = t
                        .expect_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveArrayBuilder<$T, true>>()
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
