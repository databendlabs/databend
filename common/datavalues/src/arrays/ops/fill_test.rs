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
fn test_primitive_array_fill() -> Result<()> {
    let mut df_uint16_array = DFUInt16Array::fill(5u16, 3);
    let mut arrow_uint16_array: &PrimitiveArray<UInt16Type> = df_uint16_array.as_ref();
    assert_eq!(&[5u16, 5u16, 5u16], &arrow_uint16_array.values());

    df_uint16_array = DFUInt16Array::fill_null(3);
    arrow_uint16_array: &PrimitiveArray<UInt16Type> = df_uint16_array.as_ref();
    assert_eq!(&[5u16, 5u16, 5u16], &arrow_uint16_array.values());

    Ok(())
}