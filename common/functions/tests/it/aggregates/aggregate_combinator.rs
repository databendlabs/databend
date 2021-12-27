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
use common_arrow::arrow::array::MutableArray;
use common_arrow::arrow::array::MutablePrimitiveArray;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::buffer::MutableBuffer;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
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
        error: &'static str,
        func_name: &'static str,
        input_array: Box<dyn MutableArray>,
        expect_array: Box<dyn MutableArray>,
    }

    let arrays: Vec<Series> = vec![
        Series::new(vec![4_u64, 3, 2, 1, 3, 4]),
        Series::new(vec![true, true, false, true, true, true]),
    ];

    let args = vec![
        DataField::new("a", DataType::UInt64, false),
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
            error: "",
            input_array: Box::new(MutablePrimitiveArray::<u64>::new()),
            expect_array: Box::new(MutablePrimitiveArray::from_data(
                ArrowDataType::UInt64,
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
            input_array: Box::new(MutablePrimitiveArray::<u64>::new()),
            expect_array: Box::new(MutablePrimitiveArray::from_data(
                ArrowDataType::UInt64,
                MutableBuffer::from([10u64]),
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
            input_array: Box::new(MutablePrimitiveArray::<u64>::new()),
            expect_array: Box::new(MutablePrimitiveArray::from_data(
                ArrowDataType::UInt64,
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
            input_array: Box::new(MutablePrimitiveArray::<u64>::new()),
            expect_array: Box::new(MutablePrimitiveArray::from_data(
                ArrowDataType::UInt64,
                MutableBuffer::from([1u64]),
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
            input_array: Box::new(MutablePrimitiveArray::<u64>::new()),
            expect_array: Box::new(MutablePrimitiveArray::from_data(
                ArrowDataType::UInt64,
                MutableBuffer::from([4u64]),
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
            input_array: Box::new(MutablePrimitiveArray::<u64>::new()),
            expect_array: Box::new(MutablePrimitiveArray::from_data(
                ArrowDataType::UInt64,
                MutableBuffer::from([30u64]),
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
            input_array: Box::new(MutablePrimitiveArray::<f64>::new()),
            expect_array: Box::new(MutablePrimitiveArray::from_data(
                ArrowDataType::Float64,
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
            let array: &mut dyn MutableArray = t.input_array.borrow_mut();
            let _ = func.merge_result(addr.into(), array)?;

            match t.input_array.data_type() {
                ArrowDataType::UInt64 => {
                    let array = t
                        .input_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveArray<u64>>()
                        .unwrap();
                    let expect = t
                        .expect_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveArray<u64>>()
                        .unwrap();

                    assert_eq!(array.data_type(), expect.data_type(), "{}", t.name);
                    assert_eq!(array.values(), expect.values(), "{}", t.name);
                }
                ArrowDataType::Float64 => {
                    let array = t
                        .input_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveArray<f64>>()
                        .unwrap();
                    let expect = t
                        .expect_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveArray<f64>>()
                        .unwrap();

                    assert_eq!(array.data_type(), expect.data_type(), "{}", t.name);
                    assert_eq!(array.values(), expect.values(), "{}", t.name);
                }
                _ => {
                    panic!("this way should never reach")
                }
            }

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
        input_array: Box<dyn MutableArray>,
        expect_array: Box<dyn MutableArray>,
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
            error: "",
            input_array: Box::new(MutablePrimitiveArray::<u64>::new()),
            expect_array: Box::new(MutablePrimitiveArray::from_data(
                ArrowDataType::UInt64,
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
            input_array: Box::new(MutablePrimitiveArray::<i64>::new()),
            expect_array: Box::new(MutablePrimitiveArray::from_data(
                ArrowDataType::Int64,
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
            input_array: Box::new(MutablePrimitiveArray::<u64>::new()),
            expect_array: Box::new(MutablePrimitiveArray::from_data(
                ArrowDataType::UInt64,
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
            input_array: Box::new(MutablePrimitiveArray::<i64>::new()),
            expect_array: Box::new(MutablePrimitiveArray::from_data(
                ArrowDataType::Int64,
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
            input_array: Box::new(MutablePrimitiveArray::<i64>::new()),
            expect_array: Box::new(MutablePrimitiveArray::from_data(
                ArrowDataType::Int64,
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
            input_array: Box::new(MutablePrimitiveArray::<i64>::new()),
            expect_array: Box::new(MutablePrimitiveArray::from_data(
                ArrowDataType::Int64,
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
            input_array: Box::new(MutablePrimitiveArray::<f64>::new()),
            expect_array: Box::new(MutablePrimitiveArray::from_data(
                ArrowDataType::Float64,
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

            let array: &mut dyn MutableArray = t.input_array.borrow_mut();
            let _ = func.merge_result(addr1.into(), array)?;

            match t.input_array.data_type() {
                ArrowDataType::UInt64 => {
                    let array = t
                        .input_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveArray<u64>>()
                        .unwrap();
                    let expect = t
                        .expect_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveArray<u64>>()
                        .unwrap();

                    assert_eq!(array.data_type(), expect.data_type(), "{}", t.name);
                    assert_eq!(array.values(), expect.values(), "{}", t.name);
                }
                ArrowDataType::Int64 => {
                    let array = t
                        .input_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveArray<i64>>()
                        .unwrap();
                    let expect = t
                        .expect_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveArray<i64>>()
                        .unwrap();

                    assert_eq!(array.data_type(), expect.data_type(), "{}", t.name);
                    assert_eq!(array.values(), expect.values(), "{}", t.name);
                }
                ArrowDataType::Float64 => {
                    let array = t
                        .input_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveArray<f64>>()
                        .unwrap();
                    let expect = t
                        .expect_array
                        .as_mut_any()
                        .downcast_ref::<MutablePrimitiveArray<f64>>()
                        .unwrap();

                    assert_eq!(array.data_type(), expect.data_type(), "{}", t.name);
                    assert_eq!(array.values(), expect.values(), "{}", t.name);
                }
                _ => {
                    panic!("this way should never reach")
                }
            }

            assert_eq!(t.display, format!("{:}", func), "{}", t.name);
            Ok(())
        };

        if let Err(e) = func() {
            assert_eq!(t.error, e.to_string());
        }
    }
    Ok(())
}
