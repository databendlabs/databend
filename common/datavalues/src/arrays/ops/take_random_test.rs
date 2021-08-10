// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

//use common_arrow::arrow::array::ListArray;
use common_exception::Result;

use crate::arrays::builders::*;
use crate::arrays::get_list_builder;
use crate::prelude::*;

#[test]
fn test_take_random() -> Result<()> {
    // Test DFUint16Array
    let df_uint16_array = &DFUInt16Array::new_from_iter(1u16..4u16);
    // Create TakeRandBranch for the array
    let taker = df_uint16_array.take_rand();
    // Call APIs defined in trait TakeRandom
    assert_eq!(Some(1u16), taker.get(0));
    let unsafe_val = unsafe { taker.get_unchecked(0) };
    assert_eq!(1u16, unsafe_val);

    // Test BooleanArray
    let df_bool_array = &DFBooleanArray::new_from_slice(&[true, false, true, false]);
    // Create TakeRandBranch for the array
    let taker = df_bool_array.take_rand();
    assert_eq!(Some(true), taker.get(2));
    let unsafe_val = unsafe { taker.get_unchecked(3) };
    assert_eq!(false, unsafe_val);

    // Test ListArray
    let mut builder = get_list_builder(&DataType::UInt16, 12, 3);
    builder.append_series(&Series::new(vec![1_u16, 2, 3]));
    builder.append_series(&Series::new(vec![7_u16, 8, 9]));
    let df_list = &builder.finish();
    // Create TakeRandBranch for the array
    let taker = df_list.take_rand();
    let result = taker.get(1).unwrap();
    let expected = Series::new(vec![7_u16, 8, 9]);
    assert!(result.series_equal(&expected));
    // Test get_unchecked
    let result = unsafe { taker.get_unchecked(0) };
    let expected = Series::new(vec![1_u16, 2, 3]);
    assert!(result.series_equal(&expected));

    // Test DFUtf8Array
    let mut utf8_builder = Utf8ArrayBuilder::with_capacity(3);
    utf8_builder.append_value("1a");
    utf8_builder.append_value("2b");
    utf8_builder.append_value("3c");
    let df_utf8_array = &utf8_builder.finish();
    // Create TakeRandBranch for the array
    let taker = df_utf8_array.take_rand();
    assert_eq!(Some("1a"), taker.get(0));
    // Test get_unchecked
    let result = unsafe { taker.get_unchecked(1) };
    assert_eq!("2b", result);

    Ok(())
}
