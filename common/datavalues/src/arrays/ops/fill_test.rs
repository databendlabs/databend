// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::BooleanArray;
use common_arrow::arrow::array::PrimitiveArray;
use common_exception::Result;

use crate::arrays::ops::fill::ArrayFull;
use crate::arrays::ops::fill::ArrayFullNull;
use crate::DFBooleanArray;
use crate::DFUInt16Array;
use crate::DFUtf8Array;
use crate::UInt16Type;

#[test]
fn test_array_fill() -> Result<()> {
    // Test full for PrimitiveArray
    let mut df_uint16_array = DFUInt16Array::full(5u16, 3);
    let arrow_uint16_array: &PrimitiveArray<UInt16Type> = df_uint16_array.as_ref();
    assert_eq!(&[5u16, 5u16, 5u16], &arrow_uint16_array.values());
    // Test full_null for PrimitiveArray
    df_uint16_array = DFUInt16Array::full_null(3);
    assert_eq!(3, df_uint16_array.null_count());
    assert_eq!(true, df_uint16_array.is_null(0));
    assert_eq!(true, df_uint16_array.is_null(1));
    assert_eq!(true, df_uint16_array.is_null(2));

    // Test full for BooleanArray
    let mut df_boolean_array = DFBooleanArray::full(true, 3);
    let arrow_boolean_array: &BooleanArray = df_boolean_array.as_ref();
    // 7 means 0b_111
    assert_eq!(&[7], &arrow_boolean_array.values().as_slice());
    // Test full_null for BooleanArray
    df_boolean_array = DFBooleanArray::full_null(3);
    assert_eq!(3, df_boolean_array.null_count());
    assert_eq!(true, df_boolean_array.is_null(0));
    assert_eq!(true, df_boolean_array.is_null(1));
    assert_eq!(true, df_boolean_array.is_null(2));

    // Test full for Utf8Array
    let mut df_utf8_array = DFUtf8Array::full("ab", 3);
    assert_eq!(0, df_utf8_array.null_count());
    assert_eq!(false, df_utf8_array.is_null(0));
    assert_eq!(false, df_utf8_array.is_null(1));
    assert_eq!(false, df_utf8_array.is_null(2));
    assert_eq!("ab", df_utf8_array.as_ref().value(0));
    assert_eq!("ab", df_utf8_array.as_ref().value(1));
    assert_eq!("ab", df_utf8_array.as_ref().value(2));

    // Test full_null for Utf8Array
    df_utf8_array = DFUtf8Array::full_null(3);
    assert_eq!(3, df_utf8_array.null_count());
    assert_eq!(true, df_utf8_array.is_null(0));
    assert_eq!(true, df_utf8_array.is_null(1));
    assert_eq!(true, df_utf8_array.is_null(2));

    Ok(())
}
