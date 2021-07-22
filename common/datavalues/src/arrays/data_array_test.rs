// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::prelude::*;

#[test]
fn test_data_array() -> Result<()> {
    let array = DFUInt16Array::new_from_iter(1..10);
    assert_eq!(array.null_count(), 0);

    let mask: Vec<_> = (1..10).map(|c| (c % 3 > 0) as u8).collect();
    let array = array.apply_null_mask(&mask).unwrap();
    assert_eq!(array.null_count(), 3);
    Ok(())
}
