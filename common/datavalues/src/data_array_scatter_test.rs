// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use std::sync::Arc;
use crate::{UInt32Array, Float64Array, DataArrayScatter, UInt64Array, Int8Array, DataArrayRef, Int16Array, Int32Array, Int64Array, UInt8Array, UInt16Array, Float32Array, Date32Array, Date64Array, BinaryArray, StringArray};
use common_arrow::arrow::datatypes::{ArrowPrimitiveType, UInt8Type, Int8Type};
use common_arrow::arrow::array::{PrimitiveArray, ArrayRef, TimestampSecondArray, TimestampMillisecondArray, TimestampMicrosecondArray, TimestampNanosecondArray, Time32SecondArray, Time32MillisecondArray, Time64MicrosecondArray, Time64NanosecondArray, IntervalYearMonthArray, IntervalDayTimeArray, DurationSecondArray, DurationMillisecondArray, DurationMicrosecondArray, DurationNanosecondArray, LargeBinaryArray, LargeStringArray};
use common_arrow::arrow::alloc::NativeType;
use common_arrow::arrow::datatypes::DataType::LargeBinary;

#[test]
fn test_scatter_array_data() -> Result<()> {
    #[allow(dead_code)]
    struct ArrayTest {
        name: &'static str,
        array: DataArrayRef,
        indices: DataArrayRef,
        expect: Vec<DataArrayRef>,
        error: &'static str,
    }

    let tests = vec![
        ArrayTest {
            name: "non-null-Int8",
            array: Arc::new(Int8Array::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(Int8Array::from(vec![1, 3])),
                Arc::new(Int8Array::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-Int16",
            array: Arc::new(Int16Array::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(Int16Array::from(vec![1, 3])),
                Arc::new(Int16Array::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-Int32",
            array: Arc::new(Int32Array::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(Int32Array::from(vec![1, 3])),
                Arc::new(Int32Array::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-Int64",
            array: Arc::new(Int64Array::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(Int64Array::from(vec![1, 3])),
                Arc::new(Int64Array::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-UInt8",
            array: Arc::new(UInt8Array::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(UInt8Array::from(vec![1, 3])),
                Arc::new(UInt8Array::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-UInt16",
            array: Arc::new(UInt16Array::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(UInt16Array::from(vec![1, 3])),
                Arc::new(UInt16Array::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-UInt32",
            array: Arc::new(UInt32Array::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(UInt32Array::from(vec![1, 3])),
                Arc::new(UInt32Array::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-UInt64",
            array: Arc::new(UInt64Array::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(UInt64Array::from(vec![1, 3])),
                Arc::new(UInt64Array::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-Float32",
            array: Arc::new(Float32Array::from(vec![1., 2., 3.])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(Float32Array::from(vec![1., 3.])),
                Arc::new(Float32Array::from(vec![2.]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-Float64",
            array: Arc::new(Float64Array::from(vec![1., 2., 3.])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(Float64Array::from(vec![1., 3.])),
                Arc::new(Float64Array::from(vec![2.]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-Date32",
            array: Arc::new(Date32Array::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(Date32Array::from(vec![1, 3])),
                Arc::new(Date32Array::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-Date64",
            array: Arc::new(Date64Array::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(Date64Array::from(vec![1, 3])),
                Arc::new(Date64Array::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-TimestampSecond",
            array: Arc::new(TimestampSecondArray::from_vec(vec![1, 2, 3], None)),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(TimestampSecondArray::from_vec(vec![1, 3], None)),
                Arc::new(TimestampSecondArray::from_vec(vec![2], None))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-TimestampMillisecond",
            array: Arc::new(TimestampMillisecondArray::from_vec(vec![1, 2, 3], None)),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(TimestampMillisecondArray::from_vec(vec![1, 3], None)),
                Arc::new(TimestampMillisecondArray::from_vec(vec![2], None))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-TimestampMicrosecond",
            array: Arc::new(TimestampMicrosecondArray::from_vec(vec![1, 2, 3], None)),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(TimestampMicrosecondArray::from_vec(vec![1, 3], None)),
                Arc::new(TimestampMicrosecondArray::from_vec(vec![2], None))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-TimestampNanosecond",
            array: Arc::new(TimestampNanosecondArray::from_vec(vec![1, 2, 3], None)),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(TimestampNanosecondArray::from_vec(vec![1, 3], None)),
                Arc::new(TimestampNanosecondArray::from_vec(vec![2], None))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-Time32Second",
            array: Arc::new(Time32SecondArray::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(Time32SecondArray::from(vec![1, 3])),
                Arc::new(Time32SecondArray::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-Time32Millisecond",
            array: Arc::new(Time32MillisecondArray::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(Time32MillisecondArray::from(vec![1, 3])),
                Arc::new(Time32MillisecondArray::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-Time64Microsecond",
            array: Arc::new(Time64MicrosecondArray::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(Time64MicrosecondArray::from(vec![1, 3])),
                Arc::new(Time64MicrosecondArray::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-Time64Nanosecond",
            array: Arc::new(Time64NanosecondArray::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(Time64NanosecondArray::from(vec![1, 3])),
                Arc::new(Time64NanosecondArray::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-IntervalYearMonth",
            array: Arc::new(IntervalYearMonthArray::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(IntervalYearMonthArray::from(vec![1, 3])),
                Arc::new(IntervalYearMonthArray::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-IntervalDayTime",
            array: Arc::new(IntervalDayTimeArray::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(IntervalDayTimeArray::from(vec![1, 3])),
                Arc::new(IntervalDayTimeArray::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-DurationSecond",
            array: Arc::new(DurationSecondArray::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(DurationSecondArray::from(vec![1, 3])),
                Arc::new(DurationSecondArray::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-DurationMillisecond",
            array: Arc::new(DurationMillisecondArray::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(DurationMillisecondArray::from(vec![1, 3])),
                Arc::new(DurationMillisecondArray::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-DurationMicrosecond",
            array: Arc::new(DurationMicrosecondArray::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(DurationMicrosecondArray::from(vec![1, 3])),
                Arc::new(DurationMicrosecondArray::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-DurationNanosecond",
            array: Arc::new(DurationNanosecondArray::from(vec![1, 2, 3])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(DurationNanosecondArray::from(vec![1, 3])),
                Arc::new(DurationNanosecondArray::from(vec![2]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-Binary",
            array: Arc::new(BinaryArray::from(vec![&vec![1_u8][..], &vec![2_u8][..], &vec![3_u8][..]])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(BinaryArray::from(vec![&vec![1_u8][..], &vec![3_u8][..]])),
                Arc::new(BinaryArray::from(vec![&vec![2_u8][..]]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-LargeBinary",
            array: Arc::new(LargeBinaryArray::from(vec![&vec![1_u8][..], &vec![2_u8][..], &vec![3_u8][..]])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(LargeBinaryArray::from(vec![&vec![1_u8][..], &vec![3_u8][..]])),
                Arc::new(LargeBinaryArray::from(vec![&vec![2_u8][..]]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-String",
            array: Arc::new(StringArray::from(vec!["1", "2", "3"])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(StringArray::from(vec!["1", "3"])),
                Arc::new(StringArray::from(vec!["2"]))
            ],
            error: "",
        },
        ArrayTest {
            name: "non-null-LargeString",
            array: Arc::new(LargeStringArray::from(vec!["1", "2", "3"])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 0])),
            expect: vec![
                Arc::new(LargeStringArray::from(vec!["1", "3"])),
                Arc::new(LargeStringArray::from(vec!["2"]))
            ],
            error: "",
        },


        ArrayTest {
            name: "null-Int8",
            array: Arc::new(Int8Array::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(Int8Array::from(vec![None, Some(2), None])),
                Arc::new(Int8Array::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-Int16",
            array: Arc::new(Int16Array::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(Int16Array::from(vec![None, Some(2), None])),
                Arc::new(Int16Array::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-Int32",
            array: Arc::new(Int32Array::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(Int32Array::from(vec![None, Some(2), None])),
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-Int64",
            array: Arc::new(Int64Array::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(Int64Array::from(vec![None, Some(2), None])),
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-UInt8",
            array: Arc::new(UInt8Array::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(UInt8Array::from(vec![None, Some(2), None])),
                Arc::new(UInt8Array::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-UInt16",
            array: Arc::new(UInt16Array::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(UInt16Array::from(vec![None, Some(2), None])),
                Arc::new(UInt16Array::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-UInt32",
            array: Arc::new(UInt32Array::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(UInt32Array::from(vec![None, Some(2), None])),
                Arc::new(UInt32Array::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-UInt64",
            array: Arc::new(UInt64Array::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(UInt64Array::from(vec![None, Some(2), None])),
                Arc::new(UInt64Array::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-Float32",
            array: Arc::new(Float32Array::from(vec![None, Some(1.), None, Some(2.), None, Some(3.)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(Float32Array::from(vec![None, Some(2.), None])),
                Arc::new(Float32Array::from(vec![Some(1.), None, Some(3.)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-Float64",
            array: Arc::new(Float64Array::from(vec![None, Some(1.), None, Some(2.), None, Some(3.)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(Float64Array::from(vec![None, Some(2.), None])),
                Arc::new(Float64Array::from(vec![Some(1.), None, Some(3.)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-Date32",
            array: Arc::new(Date32Array::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(Date32Array::from(vec![None, Some(2), None])),
                Arc::new(Date32Array::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-Date64",
            array: Arc::new(Date64Array::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(Date64Array::from(vec![None, Some(2), None])),
                Arc::new(Date64Array::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-TimestampSecond",
            array: Arc::new(TimestampSecondArray::from_opt_vec(vec![None, Some(1), None, Some(2), None, Some(3)], None)),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(TimestampSecondArray::from_opt_vec(vec![None, Some(2), None], None)),
                Arc::new(TimestampSecondArray::from_opt_vec(vec![Some(1), None, Some(3)], None))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-TimestampMillisecond",
            array: Arc::new(TimestampMillisecondArray::from_opt_vec(vec![None, Some(1), None, Some(2), None, Some(3)], None)),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(TimestampMillisecondArray::from_opt_vec(vec![None, Some(2), None], None)),
                Arc::new(TimestampMillisecondArray::from_opt_vec(vec![Some(1), None, Some(3)], None))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-TimestampMicrosecond",
            array: Arc::new(TimestampMicrosecondArray::from_opt_vec(vec![None, Some(1), None, Some(2), None, Some(3)], None)),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(TimestampMicrosecondArray::from_opt_vec(vec![None, Some(2), None], None)),
                Arc::new(TimestampMicrosecondArray::from_opt_vec(vec![Some(1), None, Some(3)], None))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-TimestampNanosecond",
            array: Arc::new(TimestampNanosecondArray::from_opt_vec(vec![None, Some(1), None, Some(2), None, Some(3)], None)),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(TimestampNanosecondArray::from_opt_vec(vec![None, Some(2), None], None)),
                Arc::new(TimestampNanosecondArray::from_opt_vec(vec![Some(1), None, Some(3)], None))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-Time32Second",
            array: Arc::new(Time32SecondArray::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(Time32SecondArray::from(vec![None, Some(2), None])),
                Arc::new(Time32SecondArray::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-Time32Millisecond",
            array: Arc::new(Time32MillisecondArray::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(Time32MillisecondArray::from(vec![None, Some(2), None])),
                Arc::new(Time32MillisecondArray::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-Time64Microsecond",
            array: Arc::new(Time64MicrosecondArray::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(Time64MicrosecondArray::from(vec![None, Some(2), None])),
                Arc::new(Time64MicrosecondArray::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-Time64Nanosecond",
            array: Arc::new(Time64NanosecondArray::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(Time64NanosecondArray::from(vec![None, Some(2), None])),
                Arc::new(Time64NanosecondArray::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-IntervalYearMonth",
            array: Arc::new(IntervalYearMonthArray::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(IntervalYearMonthArray::from(vec![None, Some(2), None])),
                Arc::new(IntervalYearMonthArray::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-IntervalDayTime",
            array: Arc::new(IntervalDayTimeArray::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(IntervalDayTimeArray::from(vec![None, Some(2), None])),
                Arc::new(IntervalDayTimeArray::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-DurationSecond",
            array: Arc::new(DurationSecondArray::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(DurationSecondArray::from(vec![None, Some(2), None])),
                Arc::new(DurationSecondArray::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-DurationMillisecond",
            array: Arc::new(DurationMillisecondArray::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(DurationMillisecondArray::from(vec![None, Some(2), None])),
                Arc::new(DurationMillisecondArray::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-DurationMicrosecond",
            array: Arc::new(DurationMicrosecondArray::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(DurationMicrosecondArray::from(vec![None, Some(2), None])),
                Arc::new(DurationMicrosecondArray::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-DurationNanosecond",
            array: Arc::new(DurationNanosecondArray::from(vec![None, Some(1), None, Some(2), None, Some(3)])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(DurationNanosecondArray::from(vec![None, Some(2), None])),
                Arc::new(DurationNanosecondArray::from(vec![Some(1), None, Some(3)]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-Binary",
            array: Arc::new(BinaryArray::from(vec![None, Some(&vec![1_u8][..]), None, Some(&vec![2_u8][..]), None, Some(&vec![3_u8][..])])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(BinaryArray::from(vec![None, Some(&vec![2_u8][..]), None])),
                Arc::new(BinaryArray::from(vec![Some(&vec![1_u8][..]), None, Some(&vec![3_u8][..])]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-LargeBinary",
            array: Arc::new(LargeBinaryArray::from(vec![None, Some(&vec![1_u8][..]), None, Some(&vec![2_u8][..]), None, Some(&vec![3_u8][..])])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(LargeBinaryArray::from(vec![None, Some(&vec![2_u8][..]), None])),
                Arc::new(LargeBinaryArray::from(vec![Some(&vec![1_u8][..]), None, Some(&vec![3_u8][..])]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-String",
            array: Arc::new(StringArray::from(vec![None, Some("1"), None, Some("2"), None, Some("3")])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(StringArray::from(vec![None, Some("2"), None])),
                Arc::new(StringArray::from(vec![Some("1"), None, Some("3")]))
            ],
            error: "",
        },
        ArrayTest {
            name: "null-LargeString",
            array: Arc::new(LargeStringArray::from(vec![None, Some("1"), None, Some("2"), None, Some("3")])),
            indices: Arc::new(UInt64Array::from(vec![0, 1, 1, 0, 0, 1])),
            expect: vec![
                Arc::new(LargeStringArray::from(vec![None, Some("2"), None])),
                Arc::new(LargeStringArray::from(vec![Some("1"), None, Some("3")]))
            ],
            error: "",
        },
    ];

    for test in tests {
        let result = DataArrayScatter::scatter(&test.array, &test.indices, 2);
        match result {
            Ok(scattered_array) => assert_eq!(
                scattered_array,
                test.expect,
                "failed in the test: {}",
                test.name
            ),
            Err(e) => assert_eq!(test.error, e.to_string(), "failed in the test: {}", test.name)
        }
    }

    Ok(())
}
