// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::arrays::ops::apply::ArrayApply;
use crate::arrays::ops::apply::ArrayApplyKernel;
use crate::prelude::*;
use crate::DFBooleanArray;
use crate::DFUInt16Array;
use crate::DFUtf8Array;
use crate::UInt16Type;
use common_arrow::arrow::compute;
use crate::arrays::DataArray;
use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::PrimitiveArray;


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

    let array  = builder.finish();
    let res = array.apply(|arr| arr + 10);

    let values = res.downcast_ref();

    for index in 0..res.len() {
        println!("res={:?}", values.value(index));
    }

    Ok(())
}
