// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::borrow::Cow;

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::Array;
use common_arrow::arrow::compute::arithmetics::basic::add;
use common_arrow::arrow::compute::boolean::and;
use common_arrow::arrow::compute::boolean::or;
use common_exception::Result;

use crate::arrays::ops::contain::ArrayContain;
use crate::arrays::DataArray;
use crate::prelude::*;
use crate::series::IntoSeries;
use crate::series::Series;

#[test]
fn test_contain() -> Result<()> {
    // Create DFUint16Array
    let df_uint16_array = &DFUInt16Array::new_from_iter(1u16..4u16);
    // Create ListArray
    let mut builder = get_list_builder(&DataType::UInt16, 3, 1);
    builder.append_series(&Series::new(vec![1_u16, 2, 5]));
    let df_list = builder.finish();

    let boolean = df_uint16_array.contain(&df_list);
    let values = boolean?.collect_values();
    assert_eq!(&[Some(true), Some(true), Some(false)], values.as_slice());

    Ok(())
}
