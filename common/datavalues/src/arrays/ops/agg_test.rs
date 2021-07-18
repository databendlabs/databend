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
    let sum = array.sum()?.to_series_with_size(1)?;
    let expected = DataValue::UInt16(Some(6)).to_series_with_size(1)?;
    println!("sum {:?}", array.sum()?);
    assert!(sum.series_equal(&expected));
    Ok(())
}
