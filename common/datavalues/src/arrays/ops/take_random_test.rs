// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::array::ListArray;
use common_exception::Result;

use crate::arrays::builders::*;
use crate::arrays::get_list_builder;
use crate::arrays::ops::scatter::ArrayScatter;
use crate::prelude::*;
use crate::DFBooleanArray;
use crate::DFUInt16Array;
use crate::DFUtf8Array;
//use super::take_random::TakeRandom;
//use super::take_random::ListTakeRandom;
//use super::take_random::ListTakeRandom;
//use super::take_random::IntoTakeRandom;
//use crate::arrays::ops::take_random::IntoTakeRandom;

#[test]
fn test_take_random() -> Result<()> {
    // Test DFUint16Array
    let df_uint16_array = &DFUInt16Array::new_from_iter(1u16..4u16);
    // Create TakeRandBranch for the array 
    let taker = df_uint16_array.take_rand();

    // Call APIs defined in trait TakeRandom 
    assert_eq!(Some(1u16), taker.get(0));
    let unsafe_val = unsafe { taker.get_unchecked(0) };
    assert_eq!(1u16, unsafe_val);

    Ok(())
}
