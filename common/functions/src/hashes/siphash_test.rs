// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use crate::hashes::siphash::SipHashFunction;
use common_datavalues::{DataColumnarValue, UInt8Array, UInt64Array, Int8Array, Int16Array, Int32Array, Int64Array, UInt16Array, UInt32Array};
use std::sync::Arc;
use common_arrow::arrow::datatypes::DataType::UInt64;

#[test]
fn test_siphash_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        input_column: DataColumnarValue,
        expect_output_column: DataColumnarValue,
        error: &'static str,
    }

    let tests = vec![
        Test {
            name: "Int8Array siphash",
            input_column: DataColumnarValue::Array(Arc::new(Int8Array::from(vec![1, 2, 1]))),
            expect_output_column: DataColumnarValue::Array(Arc::new(UInt64Array::from(vec![4952851536318644461, 7220060526038107403, 4952851536318644461]))),
            error: "",
        },
        Test {
            name: "Int16Array siphash",
            input_column: DataColumnarValue::Array(Arc::new(Int16Array::from(vec![1, 2, 1]))),
            expect_output_column: DataColumnarValue::Array(Arc::new(UInt64Array::from(vec![10500823559348167161, 4091451155859037844, 10500823559348167161]))),
            error: "",
        },
        Test {
            name: "Int32Array siphash",
            input_column: DataColumnarValue::Array(Arc::new(Int32Array::from(vec![1, 2, 1]))),
            expect_output_column: DataColumnarValue::Array(Arc::new(UInt64Array::from(vec![1742378985846435984, 16336925911988107921, 1742378985846435984]))),
            error: "",
        },
        Test {
            name: "Int64Array siphash",
            input_column: DataColumnarValue::Array(Arc::new(Int64Array::from(vec![1, 2, 1]))),
            expect_output_column: DataColumnarValue::Array(Arc::new(UInt64Array::from(vec![2206609067086327257, 11876854719037224982, 2206609067086327257]))),
            error: "",
        },
        Test {
            name: "UInt8Array siphash",
            input_column: DataColumnarValue::Array(Arc::new(UInt8Array::from(vec![1, 2, 1]))),
            expect_output_column: DataColumnarValue::Array(Arc::new(UInt64Array::from(vec![4952851536318644461, 7220060526038107403, 4952851536318644461]))),
            error: "",
        },
        Test {
            name: "UInt16Array siphash",
            input_column: DataColumnarValue::Array(Arc::new(UInt16Array::from(vec![1, 2, 1]))),
            expect_output_column: DataColumnarValue::Array(Arc::new(UInt64Array::from(vec![10500823559348167161, 4091451155859037844, 10500823559348167161]))),
            error: "",
        },
        Test {
            name: "UInt32Array siphash",
            input_column: DataColumnarValue::Array(Arc::new(UInt32Array::from(vec![1, 2, 1]))),
            expect_output_column: DataColumnarValue::Array(Arc::new(UInt64Array::from(vec![1742378985846435984, 16336925911988107921, 1742378985846435984]))),
            error: "",
        },
        Test {
            name: "UInt64Array siphash",
            input_column: DataColumnarValue::Array(Arc::new(UInt64Array::from(vec![1, 2, 1]))),
            expect_output_column: DataColumnarValue::Array(Arc::new(UInt64Array::from(vec![2206609067086327257, 11876854719037224982, 2206609067086327257]))),
            error: "",
        }
    ];

    for test in tests {
        let function = SipHashFunction::try_create("siphash")?;

        let rows = test.input_column.len();
        match function.eval(&[test.input_column], rows) {
            Ok(result_column) => assert_eq!(&result_column.to_array()?, &test.expect_output_column.to_array()?, "failed in the test: {}", test.name),
            Err(error) => assert_eq!(test.error, error.to_string(), "failed in the test: {}", test.name)
        };
    }

    Ok(())
}