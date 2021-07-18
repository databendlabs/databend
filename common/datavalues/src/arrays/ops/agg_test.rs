// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::prelude::*;
use crate::DFUInt16Array;
use crate::arrays::ops::agg::ArrayAgg;

#[test]
fn test_array_agg() -> Result<()> {
    let array = DFUInt16Array::new_from_iter(1u16..4u16);
    let value = [
        array.sum()?.to_series_with_size(1)?,
        array.max()?.to_series_with_size(1)?,
        array.min()?.to_series_with_size(1)?,
        array.arg_min()?.to_series_with_size(1)?,
        array.arg_max()?.to_series_with_size(1)?,
    ];

    let expected = [
        DataValue::UInt16(Some(6)).to_series_with_size(1)?,
        DataValue::UInt16(Some(3)).to_series_with_size(1)?;
        DataValue::UInt16(Some(1)).to_series_with_size(1)?;
        DataValue::UInt16(Some(0)).to_series_with_size(1)?;
        DataValue::UInt16(Some(2)).to_series_with_size(1)?;
    ];

    let len = value.len();
    for i in 0..len {
        v = value[i];
        e = expected[i];
        assert!(v.series_equal(&e));   
    }
    Ok(())
}
