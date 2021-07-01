// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::prelude::*;
use common_exception::Result;
use pretty_assertions::assert_eq;

use crate::*;

#[test]
fn test_aggregate_batch() {
    struct ArrayTest {
        name: &'static str,
        args: Vec<Series>,
        expect: Vec<DataValue>,
        error: Vec<&'static str>,
        func: Box<dyn Fn(&DataColumn) -> Result<DataValue>>,
    }

    let tests = vec![
        ArrayTest {
            name: "min-passed",
            args: vec![
                Series::new(vec!["x1", "x2"]).into(),
                Series::new(vec![1_i8, 2, 3, 4]).into(),
                Series::new(vec![4_i16, 3, 2, 1]).into(),
                Series::new(vec![4_i32, 3, 2, 1]).into(),
                Series::new(vec![4_i64, 3, 2, 1]).into(),
                Series::new(vec![4_u8, 3, 2, 1]).into(),
                Series::new(vec![4_u16, 3, 2, 1]).into(),
                Series::new(vec![4_u32, 3, 2, 1]).into(),
                Series::new(vec![4_u64, 3, 2, 1]).into(),
                Series::new(vec![4.0_f32, 3.0, 2.0, 1.0]).into(),
                Series::new(vec![4.0_f64, 3.0, 2.0, 1.0]).into(),
            ],
            func: Box::new(AggregateMinFunction::min_batch),
            expect: vec![
                DataValue::Utf8(Some("x1".to_string())),
                DataValue::Int8(Some(1)),
                DataValue::Int16(Some(1)),
                DataValue::Int32(Some(1)),
                DataValue::Int64(Some(1)),
                DataValue::UInt8(Some(1)),
                DataValue::UInt16(Some(1)),
                DataValue::UInt32(Some(1)),
                DataValue::UInt64(Some(1)),
                DataValue::Float32(Some(1.0)),
                DataValue::Float64(Some(1.0)),
            ],
            error: vec![""],
        },
        ArrayTest {
            name: "max-passed",
            args: vec![
                Series::new(vec!["x1", "x2"]),
                Series::new(vec![1_i8, 2, 3, 4]),
                Series::new(vec![4_i16, 3, 2, 1]),
                Series::new(vec![4_i32, 3, 2, 1]),
                Series::new(vec![4_i64, 3, 2, 1]),
                Series::new(vec![4_u8, 3, 2, 1]),
                Series::new(vec![4_u16, 3, 2, 1]),
                Series::new(vec![4_u32, 3, 2, 1]),
                Series::new(vec![4_u64, 3, 2, 1]),
                Series::new(vec![4.0_f32, 3.0, 2.0, 1.0]),
                Series::new(vec![4.0_f64, 3.0, 2.0, 1.0]),
            ],
            func: Box::new(AggregateMaxFunction::max_batch),
            expect: vec![
                DataValue::Utf8(Some("x2".to_string())),
                DataValue::Int8(Some(4)),
                DataValue::Int16(Some(4)),
                DataValue::Int32(Some(4)),
                DataValue::Int64(Some(4)),
                DataValue::UInt8(Some(4)),
                DataValue::UInt16(Some(4)),
                DataValue::UInt32(Some(4)),
                DataValue::UInt64(Some(4)),
                DataValue::Float32(Some(4.0)),
                DataValue::Float64(Some(4.0)),
            ],
            error: vec![""],
        },
        ArrayTest {
            name: "sum-passed",
            args: vec![
                Series::new(vec!["xx"]),
                Series::new(vec![1_i8, 2, 3, 4]),
                Series::new(vec![4_i16, 3, 2, 1]),
                Series::new(vec![4_i32, 3, 2, 1]),
                Series::new(vec![4_i64, 3, 2, 1]),
                Series::new(vec![4_u8, 3, 2, 1]),
                Series::new(vec![4_u16, 3, 2, 1]),
                Series::new(vec![4_u32, 3, 2, 1]),
                Series::new(vec![4_u64, 3, 2, 1]),
                Series::new(vec![4.0_f32, 3.0, 2.0, 1.0]),
                Series::new(vec![4.0_f64, 3.0, 2.0, 1.0]),
            ],
            func: Box::new(AggregateSumFunction::sum_batch),
            expect: vec![
                DataValue::Utf8(Some("xx".to_string())),
                DataValue::Int8(Some(10)),
                DataValue::Int16(Some(10)),
                DataValue::Int32(Some(10)),
                DataValue::Int64(Some(10)),
                DataValue::UInt8(Some(10)),
                DataValue::UInt16(Some(10)),
                DataValue::UInt32(Some(10)),
                DataValue::UInt64(Some(10)),
                DataValue::Float32(Some(10.0)),
                DataValue::Float64(Some(10.0)),
            ],
            error: vec![
                "Code: 10, displayText = Unsupported aggregate operation: sum for DataArray<Utf8>.",
            ],
        },
    ];

    for t in tests.iter() {
        for (i, arg) in t.args.iter().enumerate() {
            let column: DataColumn = arg.into();

            let result = (*(t.func))(&column);
            match result {
                Ok(v) => assert_eq!(v, t.expect[i], "{}", t.name),
                Err(e) => assert_eq!(t.error[i], e.to_string()),
            }
        }
    }
}
