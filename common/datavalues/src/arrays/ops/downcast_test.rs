// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::array::BooleanArray;
use crate::prelude::*;
use crate::DFUInt16Array;
use crate::DFBooleanArray;
use crate::UInt16Type;
use crate::UInt8Type;
use common_arrow::arrow::buffer::Buffer;

#[test]
fn test_array_as_ref() -> Result<()> {
    // Test DFUint16Array
    let df_uint16_array = DFUInt16Array::new_from_iter(1u16..4u16);
    let arrow_uint16_array : &PrimitiveArray<UInt16Type> = df_uint16_array.as_ref();
    assert_eq!(&[1u16, 2, 3], &arrow_uint16_array.values());

    // Test DFBooleanArray
    let mut boolean_builder = BooleanArrayBuilder::new(3);
    boolean_builder.append_value(true);
    boolean_builder.append_value(false);
    boolean_builder.append_value(true);
    let df_boolean_array = boolean_builder.finish();
    let arrow_boolean_array : &BooleanArray = df_boolean_array.as_ref();
    // 5 means 0b_101
    assert_eq!(&[5], &arrow_boolean_array.values().as_slice());

    // Test DFUtf8Array
    let mut utf8_builder = Utf8ArrayBuilder::new(3, 2);
    utf8_builder.append_value("1a");
    utf8_builder.append_value("2b");
    utf8_builder.append_value("3c");
    let df_utf8_array = utf8_builder.finish();
    let arrow_string_array = df_utf8_array.as_ref();
    assert_eq!(&"1a2b3c".as_bytes(), &arrow_string_array.value_data().as_slice());

    Ok(())
}
