// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::*;
use common_exception::Result;

use crate::arrays::builders::*;
use crate::prelude::*;
use crate::DFBooleanArray;
use crate::DFUInt16Array;
use crate::DFUtf8Array;
use crate::UInt16Type;

#[test]
fn test_array_as_ref() -> Result<()> {
    // Test DFUint16Array
    let df_uint16_array = DFUInt16Array::new_from_iter(1u16..4u16);
    let arrow_uint16_array = df_uint16_array.as_ref();
    assert_eq!(&[1u16, 2, 3], &arrow_uint16_array.values().as_slice());

    // Test DFBooleanArray
    let mut boolean_builder = BooleanArrayBuilder::with_capacity(3);
    boolean_builder.append_value(true);
    boolean_builder.append_value(false);
    boolean_builder.append_value(true);
    let df_boolean_array = boolean_builder.finish();
    let arrow_boolean_array: &BooleanArray = df_boolean_array.as_ref();
    // 5 means 0b_101
    assert_eq!(1, arrow_boolean_array.values().null_count());
    assert_eq!(false, arrow_boolean_array.values().get_bit(1));

    // Test DFUtf8Array
    let mut utf8_builder = Utf8ArrayBuilder::with_capacity(3);
    utf8_builder.append_value("1a");
    utf8_builder.append_value("2b");
    utf8_builder.append_value("3c");
    let df_utf8_array = utf8_builder.finish();
    let arrow_string_array = df_utf8_array.as_ref();
    assert_eq!(b"1a2b3c", arrow_string_array.values().as_slice());

    // Test DFListArray
    let mut list_builder = ListPrimitiveArrayBuilder::<UInt16Type>::with_capacity(3, 0);
    list_builder.append_slice(Some(&[1u16, 2u16, 3u16]));
    let df_list = list_builder.finish();
    let arrow_list = df_list.as_ref();
    let first_array: ArrayRef = Arc::from(arrow_list.value(0));
    let first_array = DFUInt16Array::from(first_array);
    let vs: Vec<_> = first_array.into_no_null_iter().collect();
    assert_eq!(vs, vec![1u16, 2u16, 3u16]);

    // Test DFBinaryArray
    let mut binary_builder = BinaryArrayBuilder::with_capacity(8);
    binary_builder.append_value(&"123");
    let df_binary_array = binary_builder.finish();
    let array_ref = df_binary_array.downcast_ref();
    assert_eq!(b"123", array_ref.value(0));

    Ok(())
}

#[test]
// Test from_arrow_array() and collect_values() which calls downcast_iter()
fn test_array_downcast() -> Result<()> {
    // Test PrimitiveArray
    let vec_uint16 = vec![1u16, 2u16, 3u16];
    let arrow_array = PrimitiveArray::<u16>::from_trusted_len_values_iter(vec_uint16.into_iter());
    let df_array = DFUInt16Array::from_arrow_array(arrow_array);
    let values = df_array.collect_values();
    assert_eq!(&[Some(1u16), Some(2u16), Some(3u16)], values.as_slice());

    // Test BooleanArray
    let vec_bool = vec![true, false, true];
    let arrow_bool_array = BooleanArray::from_slice(&vec_bool);
    let df_array = DFBooleanArray::from_arrow_array(arrow_bool_array);
    let values = df_array.collect_values();
    assert_eq!(&[Some(true), Some(false), Some(true)], values.as_slice());

    // Test Utf8Array
    let vec_str = vec![Some("foo"), None, Some("bar")];
    let arrow_str_array = LargeUtf8Array::from(vec_str);
    let df_array = DFUtf8Array::from_arrow_array(arrow_str_array);
    let values = df_array.collect_values();
    assert_eq!(&[Some("foo"), None, Some("bar")], values.as_slice());

    Ok(())
}
