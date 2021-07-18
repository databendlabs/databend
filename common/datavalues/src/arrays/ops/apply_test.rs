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

#[test]
fn test_array_apply() -> Result<()> {
    let mut builder = PrimitiveArrayBuilder::<UInt16Type>::new(5);

    (0..5).for_each(|index| {
        if index % 2 == 0 {
            builder.append_null();
        } else {
            builder.append_value(index as u16);
        }
    });
    let array = builder.finish();

    let arrays = vec![
        array.apply(|arr| arr + 10),
        array.apply_with_idx(|(_, arr)| arr + 10),
        array.apply_with_idx_on_opt(|(_, arr)| match arr {
            Some(v) => Some(v + 20),
            None => Some(5),
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
            assert_eq!(5 as u16, values[2].value(index));
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
