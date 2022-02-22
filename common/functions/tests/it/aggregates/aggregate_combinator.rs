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
use common_functions::aggregates::AggregateFunctionFactory;
use pretty_assertions::assert_eq;

#[test]
fn test_aggregate_combinator_function() -> Result<()> {
    struct Test {
        name: &'static str,
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
        Series::from_data(vec![4_i64, 3, 2, 1, 3, 4]),
        Series::from_data(vec![true, true, false, true, true, true]),
    ];

    let args = vec![
        DataField::new("a", i64::to_data_type()),
        DataField::new("b", bool::to_data_type()),
    ];

    let tests = vec![
        Test {
            name: "count-distinct-passed",
            params: vec![],
            args: vec![args[0].clone()],
            display: "countdistinct",
            func_name: "countdistinct",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<u64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<u64>::from_data(
                u64::to_data_type(),
                Vec::from([4u64]),
            )),
        },
        Test {
            name: "sum-distinct-passed",
            params: vec![],
            args: vec![args[0].clone()],
            display: "sumdistinct",
            func_name: "sumdistinct",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([10i64]),
            )),
        },
        Test {
            name: "count-if-passed",
            params: vec![],
            args: args.clone(),
            display: "countif",
            func_name: "countif",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<u64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<u64>::from_data(
                u64::to_data_type(),
                Vec::from([10u64]),
            )),
        },
        Test {
            name: "min-if-passed",
            params: vec![],
            args: args.clone(),
            display: "minif",
            func_name: "minif",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([1i64]),
            )),
        },
        Test {
            name: "max-if-passed",
            params: vec![],
            args: args.clone(),
            display: "maxif",
            func_name: "maxif",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([4i64]),
            )),
        },
        Test {
            name: "sum-if-passed",
            params: vec![],
            args: args.clone(),
            display: "sumif",
            func_name: "sumif",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([30i64]),
            )),
        },
        Test {
            name: "avg-if-passed",
            params: vec![],
            args: args.clone(),
            display: "avgif",
            func_name: "avgif",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<f64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<f64>::from_data(
                f64::to_data_type(),
                Vec::from([3f64]),
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
            func.accumulate(addr.into(), &t.arrays, None, rows)?;

            // Second.
            let addr2 = arena.alloc_layout(func.state_layout());
            func.init_state(addr2.into());
            func.accumulate(addr2.into(), &t.arrays, None, rows)?;

            func.merge(addr.into(), addr2.into())?;
            let array: &mut dyn MutableColumn = t.input_array.borrow_mut();
            let _ = func.merge_result(addr.into(), array)?;

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
fn test_aggregate_combinator_function_on_empty_data() -> Result<()> {
    struct Test {
        name: &'static str,
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
        DataField::new("b", bool::to_data_type()),
    ];

    let tests = vec![
        Test {
            name: "count-distinct-passed",
            params: vec![],
            args: vec![args[0].clone()],
            display: "countdistinct",
            func_name: "countdistinct",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<u64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<u64>::from_data(
                u64::to_data_type(),
                Vec::from([0u64]),
            )),
        },
        Test {
            name: "sum-distinct-passed",
            params: vec![],
            args: vec![args[0].clone()],
            display: "sumdistinct",
            func_name: "sumdistinct",
            arrays: vec![arrays[0].clone()],
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([0i64]),
            )),
        },
        Test {
            name: "count-if-passed",
            params: vec![],
            args: args.clone(),
            display: "countif",
            func_name: "countif",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<u64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<u64>::from_data(
                u64::to_data_type(),
                Vec::from([0u64]),
            )),
        },
        Test {
            name: "min-if-passed",
            params: vec![],
            args: args.clone(),
            display: "minif",
            func_name: "minif",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([0i64]),
            )),
        },
        Test {
            name: "max-if-passed",
            params: vec![],
            args: args.clone(),
            display: "maxif",
            func_name: "maxif",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([0i64]),
            )),
        },
        Test {
            name: "sum-if-passed",
            params: vec![],
            args: args.clone(),
            display: "sumif",
            func_name: "sumif",
            arrays: arrays.clone(),
            error: "",
            input_array: Box::new(MutablePrimitiveColumn::<i64>::default()),
            expect_array: Box::new(MutablePrimitiveColumn::<i64>::from_data(
                i64::to_data_type(),
                Vec::from([0i64]),
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

            func.accumulate(addr1.into(), &t.arrays, None, rows)?;

            // Second.
            let addr2 = arena.alloc_layout(func.state_layout());
            func.init_state(addr2.into());
            func.accumulate(addr2.into(), &t.arrays, None, rows)?;
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

            assert_eq!(t.display, format!("{:}", func), "{}", t.name);
            Ok(())
        };

        if let Err(e) = func() {
            assert_eq!(t.error, e.to_string());
        }
    }
    Ok(())
}
