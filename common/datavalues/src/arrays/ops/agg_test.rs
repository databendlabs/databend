// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::prelude::*;
use crate::DFUInt16Array;

#[test]
fn test_array_agg() -> Result<()> {
    let array = DFUInt16Array::new_from_iter(1_u16..4u16);
    let sum = array.sum(&DataType::UInt16)?.to_series_with_size(1);
    let expected = DataValue::UInt16(Some(10)).to_arrow_array_with_size(1);
    assert!(sum.series_equal(&expected));
    Ok(())
}