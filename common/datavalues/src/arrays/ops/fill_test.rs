// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::arrays::ops::fill::ArrayFull;
use crate::arrays::ops::fill::ArrayFullNull;
use crate::prelude::*;

#[test]
fn test_array_fill() -> Result<()> {
    // Test full for PrimitiveArray
    let mut df_uint16_array = DFUInt16Array::full(5u16, 3);
    let arrow_uint16_array = df_uint16_array.as_ref();
    assert_eq!(&[5u16, 5u16, 5u16], &arrow_uint16_array.values().as_slice());
    // Test full_null for PrimitiveArray
    df_uint16_array = DFUInt16Array::full_null(3);
    assert_eq!(3, df_uint16_array.null_count());
    assert_eq!(true, df_uint16_array.is_null(0));
    assert_eq!(true, df_uint16_array.is_null(1));
    assert_eq!(true, df_uint16_array.is_null(2));

    // Test full for BooleanArray
    let mut df_boolean_array = DFBooleanArray::full(true, 3);
    assert_eq!(0, df_boolean_array.null_count());

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
