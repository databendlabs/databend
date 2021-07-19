// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::compute;
use common_exception::Result;

use crate::arrays::ops::apply::ArrayApply;
use crate::arrays::ops::apply::ArrayApplyKernel;
use crate::arrays::DataArray;
use crate::prelude::*;
use crate::DFBooleanArray;
use crate::DFUInt16Array;
use crate::DFUtf8Array;
use crate::UInt16Type;

fn new_test_uint16_array(cap: usize, begin: i32, end: i32) -> DataArray<UInt16Type> {
    let mut builder = PrimitiveArrayBuilder::<UInt16Type>::new(cap);

    (begin..end).for_each(|index| {
        if index % 2 == 0 {
            builder.append_null();
        } else {
            builder.append_value(index as u16);
        }
    });
    builder.finish()
}

#[test]
fn test_array_apply() -> Result<()> {
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

    println!("branch_apply_cast_numeric_no_null={:?}", values[4]);

    for index in 0..5 {
        if index % 2 == 0 {
            assert_eq!(10 as u16, values[0].value(index));
            assert_eq!(0 as u16, values[1].value(index));
            assert_eq!(0 as u16, values[2].value(index));
            assert_eq!(30 as u16, values[3].value(index));
            assert_eq!(40 as u16, values[4].value(index));
        } else {
            assert_eq!((10 + index) as u16, values[0].value(index));
            assert_eq!((10 + index) as u16, values[1].value(index));
            assert_eq!((20 + index) as u16, values[2].value(index));
            assert_eq!((30 + index) as u16, values[3].value(index));
            assert_eq!((40 + index) as u16, values[4].value(index));
        }
    }
    Ok(())
}

#[test]
fn test_array_apply_kernel() -> Result<()> {
    let array1 = new_test_uint16_array(5, 0, 5);
    let array2 = new_test_uint16_array(5, 5, 10);

    let arrays = vec![
        array1.apply_kernel(|arr| Arc::new(compute::add(arr, array2.as_ref()).unwrap())),
        array1.apply_kernel_cast(|arr| Arc::new(compute::add(arr, array2.as_ref()).unwrap())),
    ];

    let values = vec![arrays[0].downcast_ref(), arrays[1].downcast_ref()];

    assert_eq!(5 as u16, values[0].value(0));
    assert_eq!(1 as u16, values[0].value(1));
    assert_eq!(7 as u16, values[0].value(2));
    assert_eq!(3 as u16, values[0].value(3));
    assert_eq!(9 as u16, values[0].value(4));

    assert_eq!(5 as u16, values[1].value(0));
    assert_eq!(1 as u16, values[1].value(1));
    assert_eq!(7 as u16, values[1].value(2));
    assert_eq!(3 as u16, values[1].value(3));
    assert_eq!(9 as u16, values[1].value(4));

    Ok(())
}
