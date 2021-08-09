// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::arrays::builders::*;
use crate::arrays::ops::group_hash::GroupHash;
use crate::DFUInt16Array;

#[test]
fn test_group_hash() -> Result<()> {
    // Create DFUint16Array
    let df_uint16_array = DFUInt16Array::new_from_iter(1u16..4u16);
    // Create a buffer
    let mut buffer = Box::new([0u16, 0, 0]);
    let ptr = buffer.as_mut_ptr() as *mut u8;
    let _ = df_uint16_array.fixed_hash(ptr, 2);

    assert_eq!(buffer[0], 1);
    assert_eq!(buffer[1], 2);
    assert_eq!(buffer[2], 3);

    return Ok(());
}
