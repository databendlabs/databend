// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_arrow::arrow::array::*;
use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::*;
use common_exception::Result;

use crate::hashes::siphash::SipHashFunction;

#[test]
fn test_siphash_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        input_column: DataColumn,
        expect_output_column: DataColumn,
        error: &'static str,
    }

    let tests = vec![
        Test {
            name: "Int8Array siphash",
            input_column: DataColumn::Array(Arc::new(Int8Array::from(vec![1, 2, 1]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                4952851536318644461,
                7220060526038107403,
                4952851536318644461,
            ]))),
            error: "",
        },
        Test {
            name: "Int16Array siphash",
            input_column: DataColumn::Array(Arc::new(Int16Array::from(vec![1, 2, 1]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                10500823559348167161,
                4091451155859037844,
                10500823559348167161,
            ]))),
            error: "",
        },
        Test {
            name: "Int32Array siphash",
            input_column: DataColumn::Array(Arc::new(Int32Array::from(vec![1, 2, 1]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                1742378985846435984,
                16336925911988107921,
                1742378985846435984,
            ]))),
            error: "",
        },
        Test {
            name: "Int64Array siphash",
            input_column: DataColumn::Array(Arc::new(Int64Array::from(vec![1, 2, 1]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                2206609067086327257,
                11876854719037224982,
                2206609067086327257,
            ]))),
            error: "",
        },
        Test {
            name: "UInt8Array siphash",
            input_column: DataColumn::Array(Arc::new(UInt8Array::from(vec![1, 2, 1]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                4952851536318644461,
                7220060526038107403,
                4952851536318644461,
            ]))),
            error: "",
        },
        Test {
            name: "UInt16Array siphash",
            input_column: DataColumn::Array(Arc::new(UInt16Array::from(vec![1, 2, 1]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                10500823559348167161,
                4091451155859037844,
                10500823559348167161,
            ]))),
            error: "",
        },
        Test {
            name: "UInt32Array siphash",
            input_column: DataColumn::Array(Arc::new(UInt32Array::from(vec![1, 2, 1]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                1742378985846435984,
                16336925911988107921,
                1742378985846435984,
            ]))),
            error: "",
        },
        Test {
            name: "UInt64Array siphash",
            input_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![1, 2, 1]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                2206609067086327257,
                11876854719037224982,
                2206609067086327257,
            ]))),
            error: "",
        },
        Test {
            name: "Float32Array siphash",
            input_column: DataColumn::Array(Arc::new(Float32Array::from(vec![1., 2., 1.]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                729488449357906283,
                9872512741335963328,
                729488449357906283,
            ]))),
            error: "",
        },
        Test {
            name: "Float64Array siphash",
            input_column: DataColumn::Array(Arc::new(Float64Array::from(vec![1., 2., 1.]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                13833534234735907638,
                12773237290464453619,
                13833534234735907638,
            ]))),
            error: "",
        },
        Test {
            name: "Date32Array siphash",
            input_column: DataColumn::Array(Arc::new(Date32Array::from(vec![1, 2, 1]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                1742378985846435984,
                16336925911988107921,
                1742378985846435984,
            ]))),
            error: "",
        },
        Test {
            name: "Date64Array siphash",
            input_column: DataColumn::Array(Arc::new(Date64Array::from(vec![1, 2, 1]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                2206609067086327257,
                11876854719037224982,
                2206609067086327257,
            ]))),
            error: "",
        },
        Test {
            name: "TimestampSecondArray siphash",
            input_column: DataColumn::Array(Arc::new(TimestampSecondArray::from_vec(
                vec![1, 2, 1],
                None,
            ))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                2206609067086327257,
                11876854719037224982,
                2206609067086327257,
            ]))),
            error: "",
        },
        Test {
            name: "TimestampMillisecondArray siphash",
            input_column: DataColumn::Array(Arc::new(TimestampMillisecondArray::from_vec(
                vec![1, 2, 1],
                None,
            ))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                2206609067086327257,
                11876854719037224982,
                2206609067086327257,
            ]))),
            error: "",
        },
        Test {
            name: "TimestampMicrosecondArray siphash",
            input_column: DataColumn::Array(Arc::new(TimestampMicrosecondArray::from_vec(
                vec![1, 2, 1],
                None,
            ))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                2206609067086327257,
                11876854719037224982,
                2206609067086327257,
            ]))),
            error: "",
        },
        Test {
            name: "TimestampNanosecondArray siphash",
            input_column: DataColumn::Array(Arc::new(TimestampNanosecondArray::from_vec(
                vec![1, 2, 1],
                None,
            ))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                2206609067086327257,
                11876854719037224982,
                2206609067086327257,
            ]))),
            error: "",
        },
        Test {
            name: "Time32SecondArray siphash",
            input_column: DataColumn::Array(Arc::new(Time32SecondArray::from(vec![1, 2, 1]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                1742378985846435984,
                16336925911988107921,
                1742378985846435984,
            ]))),
            error: "",
        },
        Test {
            name: "Time32MillisecondArray siphash",
            input_column: DataColumn::Array(Arc::new(Time32MillisecondArray::from(vec![1, 2, 1]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                1742378985846435984,
                16336925911988107921,
                1742378985846435984,
            ]))),
            error: "",
        },
        Test {
            name: "Time64MicrosecondArray siphash",
            input_column: DataColumn::Array(Arc::new(Time64MicrosecondArray::from(vec![1, 2, 1]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                2206609067086327257,
                11876854719037224982,
                2206609067086327257,
            ]))),
            error: "",
        },
        Test {
            name: "Time64NanosecondArray siphash",
            input_column: DataColumn::Array(Arc::new(Time64NanosecondArray::from(vec![1, 2, 1]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                2206609067086327257,
                11876854719037224982,
                2206609067086327257,
            ]))),
            error: "",
        },
        Test {
            name: "IntervalYearMonthArray siphash",
            input_column: DataColumn::Array(Arc::new(IntervalYearMonthArray::from(vec![1, 2, 1]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                1742378985846435984,
                16336925911988107921,
                1742378985846435984,
            ]))),
            error: "",
        },
        Test {
            name: "IntervalDayTimeArray siphash",
            input_column: DataColumn::Array(Arc::new(IntervalDayTimeArray::from(vec![1, 2, 1]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                2206609067086327257,
                11876854719037224982,
                2206609067086327257,
            ]))),
            error: "",
        },
        Test {
            name: "DurationSecondArray siphash",
            input_column: DataColumn::Array(Arc::new(DurationSecondArray::from(vec![1, 2, 1]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                2206609067086327257,
                11876854719037224982,
                2206609067086327257,
            ]))),
            error: "",
        },
        Test {
            name: "DurationMillisecondArray siphash",
            input_column: DataColumn::Array(Arc::new(DurationMillisecondArray::from(vec![
                1, 2, 1,
            ]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                2206609067086327257,
                11876854719037224982,
                2206609067086327257,
            ]))),
            error: "",
        },
        Test {
            name: "DurationMicrosecondArray siphash",
            input_column: DataColumn::Array(Arc::new(DurationMicrosecondArray::from(vec![
                1, 2, 1,
            ]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                2206609067086327257,
                11876854719037224982,
                2206609067086327257,
            ]))),
            error: "",
        },
        Test {
            name: "DurationNanosecondArray siphash",
            input_column: DataColumn::Array(Arc::new(DurationNanosecondArray::from(vec![1, 2, 1]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                2206609067086327257,
                11876854719037224982,
                2206609067086327257,
            ]))),
            error: "",
        },
        Test {
            name: "BinaryArray siphash",
            input_column: DataColumn::Array(Arc::new(BinaryArray::from(vec![
                &vec![1_u8][..],
                &vec![2_u8][..],
                &vec![1_u8][..],
            ]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                4952851536318644461,
                7220060526038107403,
                4952851536318644461,
            ]))),
            error: "",
        },
        Test {
            name: "LargeBinaryArray siphash",
            input_column: DataColumn::Array(Arc::new(LargeBinaryArray::from(vec![
                &vec![1_u8][..],
                &vec![2_u8][..],
                &vec![1_u8][..],
            ]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                4952851536318644461,
                7220060526038107403,
                4952851536318644461,
            ]))),
            error: "",
        },
        Test {
            name: "StringArray siphash",
            input_column: DataColumn::Array(Arc::new(StringArray::from(vec!["1", "2", "1"]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                11582886058036617813,
                1450935647650615885,
                11582886058036617813,
            ]))),
            error: "",
        },
        Test {
            name: "LargeStringArray siphash",
            input_column: DataColumn::Array(Arc::new(LargeStringArray::from(vec!["1", "2", "1"]))),
            expect_output_column: DataColumn::Array(Arc::new(UInt64Array::from(vec![
                11582886058036617813,
                1450935647650615885,
                11582886058036617813,
            ]))),
            error: "",
        },
    ];

    for test in tests {
        let function = SipHashFunction::try_create("siphash")?;

        let rows = test.input_column.len();
        match function.eval(&[test.input_column], rows) {
            Ok(result_column) => assert_eq!(
                &result_column.to_array()?,
                &test.expect_output_column.to_array()?,
                "failed in the test: {}",
                test.name
            ),
            Err(error) => assert_eq!(
                test.error,
                error.to_string(),
                "failed in the test: {}",
                test.name
            ),
        };
    }

    Ok(())
}
