// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::arrays::builders::*;
use crate::prelude::*;
use crate::DFBinaryArray;
use crate::DFBooleanArray;
use crate::DFListArray;
use crate::DFStructArray;
use crate::DFUInt16Array;
use crate::DFUtf8Array;
use crate::Int32Type;
use crate::UInt16Type;

#[test]
fn test_array_fill() -> Result<()> {
    // Test fill for PrimitiveArray
    let mut df_uint16_array = DFUInt16Array::fill(5u16, 3);
    let mut arrow_uint16_array: &PrimitiveArray<UInt16Type> = df_uint16_array.as_ref();
    assert_eq!(&[5u16, 5u16, 5u16], &arrow_uint16_array.values());
    // Test fill_null for PrimitiveArray
    df_uint16_array = DFUInt16Array::fill_null(3);
    assert_eq!(3, df_uint16_array.null_count());
    assert_eq!(true, df_uint16_array.is_null(0));
    assert_eq!(true, df_uint16_array.is_null(1));
    assert_eq!(true, df_uint16_array.is_null(2));

    // Test fill for BooleanArray
    let mut df_boolean_array = DFBooleanArray::fill(true, 3);
    let mut arrow_boolean_array: &BooleanArray = df_boolean_array.as_ref();
    // 7 means 0b_111
    assert_eq!(&[7], &arrow_boolean_array.values().as_slice());
    // Test fill_null for BooleanArray
    df_boolean_array = DFBooleanArray::fill_null(3);
    assert_eq!(3, df_uint16_array.null_count());
    assert_eq!(true, df_uint16_array.is_null(0));
    assert_eq!(true, df_uint16_array.is_null(1));
    assert_eq!(true, df_uint16_array.is_null(2));

    // Test fill for Utf8Array
    let mut df_utf8_array = DFBooleanArray::fill("ab", 3);
    assert_eq!(0, df_utf8_array.null_count());
    assert_eq!(false, df_utf8_array.is_null(0));
    assert_eq!(false, df_utf8_array.is_null(1));
    assert_eq!(false, df_utf8_array.is_null(2));
    assert_eq!("ab", df_utf8_array.value(0));
    assert_eq!("ab", df_utf8_array.value(1));
    assert_eq!("ab", df_utf8_array.value(2));
    // Test fill_null for Utf8Array
    df_utf8_array = DFUInt8Array::fill_null(3);
    assert_eq!(3, df_utf8_array.null_count());
    assert_eq!(true, df_utf8_array.is_null(0));
    assert_eq!(true, df_utf8_array.is_null(1));
    assert_eq!(true, df_utf8_array.is_null(2));

    let s = Series::new(vec![1_u8, 2, 3]);
    let mut df_list_array = DFListArray::fill(&s, 2);
    assert_eq!(0, df_list_array.null_count());
    assert_eq!(false, df_list_array.is_null(0));
    assert_eq!(false, df_list_array.is_null(1));
    assert_eq!(&[1, 2, 3], &df_list_array.value(0));
    assert_eq!(&[1, 2, 3], &df_list_array.value(1));

    df_list_array = DFListArray::fill_null(2);
    assert_eq!(2, df_utf8_array.null_count());
    assert_eq!(true, df_utf8_array.is_null(0));
    assert_eq!(true, df_utf8_array.is_null(1));
    Ok(())
}