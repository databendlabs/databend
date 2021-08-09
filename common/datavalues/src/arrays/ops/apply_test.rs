// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::borrow::Cow;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::compute::arithmetics::basic::add;
use common_arrow::arrow::compute::boolean::and;
use common_arrow::arrow::compute::boolean::or;
use common_exception::Result;

use crate::arrays::ops::apply::ArrayApply;
use crate::arrays::ops::apply::ArrayApplyKernel;
use crate::arrays::DataArray;
use crate::prelude::*;
use crate::BooleanType;
use crate::UInt16Type;

fn new_test_uint16_array(cap: usize, begin: i32, end: i32) -> DataArray<UInt16Type> {
    let mut builder = PrimitiveArrayBuilder::<UInt16Type>::with_capacity(cap);

    (begin..end).for_each(|index| {
        if index % 3 == 0 {
            builder.append_null();
        } else {
            builder.append_value(index as u16);
        }
    });
    builder.finish()
}

fn new_test_boolean_array(cap: usize, begin: i32, end: i32) -> DataArray<BooleanType> {
    let mut builder = BooleanArrayBuilder::with_capacity(cap);

    (begin..end).for_each(|index| {
        if index % 3 == 0 {
            builder.append_null();
        } else {
            builder.append_value(index % 3 == 1);
        }
    });
    builder.finish()
}

fn new_test_utf8_array(cap: usize, begin: i32, end: i32) -> DFUtf8Array {
    let mut builder = Utf8ArrayBuilder::with_capacity(cap);
    let s = vec!["ax", "by", "cz", "dm", "13"];

    (begin..end).for_each(|index| {
        if index % 3 == 0 {
            builder.append_null();
        } else {
            builder.append_value(s[index as usize]);
        }
    });
    builder.finish()
}

#[test]
fn test_array_apply() -> Result<()> {
    // array=[null, 1, 2, null, 4]
    let array = new_test_uint16_array(5, 0, 5);
    let arrays = vec![
        array.apply(|arr| arr + 10),
        array.apply_with_idx(|(_, arr)| arr + 10),
        array.apply_with_idx_on_opt(|(_, arr)| match arr {
            Some(v) => Some(v + 20),
            None => Some(0),
        }),
        array.apply_cast_numeric(|arr| arr + 30),
        array.branch_apply_cast_numeric_no_null(|arr| match arr {
            Some(v) => (v + 40) as u16,
            None => 0 as u16,
        }),
    ];

    let values = vec![
        arrays[0].downcast_ref(),
        arrays[1].downcast_ref(),
        arrays[2].downcast_ref(),
        arrays[3].downcast_ref(),
        arrays[4].downcast_ref(),
    ];

    assert_eq!(2, values[0].null_count());
    assert_eq!(true, values[0].is_null(0));
    assert_eq!(11, values[0].value(1));
    assert_eq!(12, values[0].value(2));
    assert_eq!(true, values[0].is_null(3));
    assert_eq!(14, values[0].value(4));

    assert_eq!(2, values[1].null_count());
    assert_eq!(true, values[1].is_null(0));
    assert_eq!(11, values[1].value(1));
    assert_eq!(12, values[1].value(2));
    assert_eq!(true, values[1].is_null(3));
    assert_eq!(14, values[1].value(4));

    assert_eq!(0, values[2].null_count());
    assert_eq!(0, values[2].value(0));
    assert_eq!(21, values[2].value(1));
    assert_eq!(22, values[2].value(2));
    assert_eq!(0, values[2].value(3));
    assert_eq!(24, values[2].value(4));
    assert_eq!(2, values[3].null_count());
    assert_eq!(true, values[3].is_null(0));
    assert_eq!(31, values[3].value(1));
    assert_eq!(32, values[3].value(2));
    assert_eq!(true, values[3].is_null(3));
    assert_eq!(34, values[3].value(4));

    assert_eq!(2, values[4].null_count());
    assert_eq!(true, values[4].is_null(0));
    assert_eq!(41, values[4].value(1));
    assert_eq!(42, values[4].value(2));
    assert_eq!(true, values[4].is_null(3));
    assert_eq!(44, values[4].value(4));

    Ok(())
}

#[test]
fn test_array_apply_kernel() -> Result<()> {
    // array=[null, 1, 2, null, 4]
    let array1 = new_test_uint16_array(5, 0, 5);
    // array=[5, null, 7, 8, null]
    let array2 = new_test_uint16_array(5, 5, 10);

    let arrays = vec![
        array1.apply_kernel(|arr| Arc::new(add::add(arr, array2.as_ref()).unwrap())),
        array1.apply_kernel_cast(|arr| Arc::new(add::add(arr, array2.as_ref()).unwrap())),
    ];

    let values = vec![arrays[0].downcast_ref(), arrays[1].downcast_ref()];

    assert_eq!(4, values[0].null_count());
    assert_eq!(true, values[0].is_null(0));
    assert_eq!(true, values[0].is_null(1));
    assert_eq!(9, values[0].value(2));
    assert_eq!(true, values[0].is_null(3));
    assert_eq!(true, values[0].is_null(4));

    assert_eq!(4, values[1].null_count());
    assert_eq!(true, values[1].is_null(0));
    assert_eq!(true, values[1].is_null(1));
    assert_eq!(9, values[1].value(2));
    assert_eq!(true, values[1].is_null(3));
    assert_eq!(true, values[1].is_null(4));

    Ok(())
}

#[test]
fn test_boolean_array_apply() -> Result<()> {
    // array= [null, true, false, null, true]
    let array = new_test_boolean_array(5, 0, 5);

    let arrays = vec![
        array.apply(|arr| arr && true),
        array.apply_with_idx(|(_, arr)| arr && true),
        array.apply_with_idx_on_opt(|(_, arr)| match arr {
            Some(v) => Some(!v),
            None => Some(true),
        }),
    ];

    let values = vec![
        arrays[0].downcast_ref(),
        arrays[1].downcast_ref(),
        arrays[2].downcast_ref(),
    ];

    assert_eq!(2, values[0].null_count());
    assert_eq!(true, values[0].is_null(0));
    assert_eq!(true, values[0].value(1));
    assert_eq!(false, values[0].value(2));
    assert_eq!(true, values[0].is_null(3));
    assert_eq!(true, values[0].value(4));

    assert_eq!(2, values[1].null_count());
    assert_eq!(true, values[1].is_null(0));
    assert_eq!(true, values[1].value(1));
    assert_eq!(false, values[1].value(2));
    assert_eq!(true, values[1].is_null(3));
    assert_eq!(true, values[1].value(4));

    assert_eq!(0, values[2].null_count());
    assert_eq!(true, values[2].value(0));
    assert_eq!(false, values[2].value(1));
    assert_eq!(true, values[2].value(2));
    assert_eq!(true, values[2].value(3));
    assert_eq!(false, values[2].value(4));

    Ok(())
}

#[test]
fn test_boolean_array_apply_kernel() -> Result<()> {
    // array1= [null, true, false, null, true]
    let array1 = new_test_boolean_array(5, 0, 5);
    // array2= [false, null, true, false, null]
    let array2 = new_test_boolean_array(5, 5, 10);

    let arrays = vec![
        array1.apply_kernel(|arr| Arc::new(and(arr, array2.as_ref()).unwrap())),
        array1.apply_kernel_cast(|arr| Arc::new(or(arr, array2.as_ref()).unwrap())),
    ];

    let values = vec![arrays[0].downcast_ref(), arrays[1].downcast_ref()];

    assert_eq!(4, values[0].null_count());
    assert_eq!(true, values[0].is_null(0));
    assert_eq!(true, values[0].is_null(1));
    assert_eq!(false, values[0].value(2));
    assert_eq!(true, values[0].is_null(3));
    assert_eq!(true, values[0].is_null(4));

    assert_eq!(4, values[0].null_count());
    assert_eq!(true, values[1].is_null(0));
    assert_eq!(true, values[1].is_null(1));
    assert_eq!(true, values[1].value(2));
    assert_eq!(true, values[1].is_null(3));
    assert_eq!(true, values[1].is_null(4));

    Ok(())
}

#[test]
fn test_utf8_array_apply() -> Result<()> {
    // array=[null, "by", "cz", null, "13"]
    let array = new_test_utf8_array(5, 0, 5);
    let arrays = vec![
        array.apply(|arr| Cow::from(&arr[1..])),
        array.apply_with_idx(|(_, arr)| Cow::from(&arr[..1])),
        array.apply_with_idx_on_opt(|(_, arr)| match arr {
            Some(v) => Some(Cow::from(&v[0..])),
            None => Some(Cow::from("ff")),
        }),
    ];

    let values = vec![
        arrays[0].downcast_ref(),
        arrays[1].downcast_ref(),
        arrays[2].downcast_ref(),
    ];

    let cast_arrays = vec![
        array.apply_cast_numeric::<_, UInt16Type>(|arr| arr.len() as u16),
        array.branch_apply_cast_numeric_no_null::<_, UInt16Type>(|arr| match arr {
            Some(v) => (v.len() + 1) as u16,
            None => 0 as u16,
        }),
    ];

    let cast_values = vec![cast_arrays[0].downcast_ref(), cast_arrays[1].downcast_ref()];

    assert_eq!(2, values[0].null_count());
    assert_eq!(true, values[0].is_null(0));
    assert_eq!("y", values[0].value(1));
    assert_eq!("z", values[0].value(2));
    assert_eq!(true, values[0].is_null(3));
    assert_eq!("3", values[0].value(4));
    assert_eq!(true, values[0].is_null(3));

    assert_eq!(2, values[1].null_count());
    assert_eq!(true, values[1].is_null(0));
    assert_eq!("b", values[1].value(1));
    assert_eq!("c", values[1].value(2));
    assert_eq!(true, values[1].is_null(3));
    assert_eq!("1", values[1].value(4));
    assert_eq!(true, values[1].is_null(3));

    assert_eq!(0, values[2].null_count());
    assert_eq!("ff", values[2].value(0));
    assert_eq!("by", values[2].value(1));
    assert_eq!("cz", values[2].value(2));
    assert_eq!("ff", values[2].value(3));
    assert_eq!("13", values[2].value(4));

    assert_eq!(2, cast_values[0].null_count());
    assert_eq!(true, cast_values[0].is_null(0));
    assert_eq!(2, cast_values[0].value(1));
    assert_eq!(2, cast_values[0].value(2));
    assert_eq!(true, cast_values[0].is_null(3));
    assert_eq!(2, cast_values[0].value(4));

    assert_eq!(2, cast_values[1].null_count());
    assert_eq!(true, cast_values[1].is_null(0));
    assert_eq!(3, cast_values[1].value(1));
    assert_eq!(3, cast_values[1].value(2));
    assert_eq!(true, cast_values[1].is_null(3));
    assert_eq!(3, cast_values[1].value(4));

    Ok(())
}
