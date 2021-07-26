// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::DFUInt16Array;
use common_exception::Result;
use crate::arrays::builders::*;
use crate::arrays::ops::group_hash::GroupHash;

#[test]
fn test_group_hash() -> Result<()> {
    // Create DFUint16Array
    let df_uint16_array = DFUInt16Array::new_from_iter(1u16..4u16);
    // Create a buffer 
    let buffer = Box::new([0u16, 0, 0]);
    let ptr: *const [u16;3] = &*buffer;

    let _ = df_uint16_array.group_hash(ptr as usize, 2);

    assert_eq!(buffer[0], 1);
    assert_eq!(buffer[1], 2);
    assert_eq!(buffer[2], 3);

    return Ok(())
}
