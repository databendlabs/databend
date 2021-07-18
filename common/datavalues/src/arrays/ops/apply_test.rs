// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::arrays::ops::agg::ArrayAgg;
use crate::arrays::ArrayBuilder;
use crate::prelude::*;
use crate::DFBooleanArray;
use crate::DFUInt16Array;
use crate::DFUtf8Array;


#[test]
fn test_array_apply() -> Result<()> {
    let array = DFUInt16Array::new_from_iter(1u16..4u16);
    array.append_null();

    OK(())
}