// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::compute::contains::contains;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::arrays::get_list_builder;
use crate::arrays::DataArray;
use crate::prelude::*;
use crate::series::IntoSeries;

pub trait ArrayContain: Debug {
    /// # Safety
    ///
    fn contain(&self, _list: &DFListArray) -> Result<DFBooleanArray>
    where Self: std::marker::Sized {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported apply contain operation for {:?}",
            self,
        )))
    }
}

macro_rules! contain_internal {
    ($self:expr, $list:expr) => {{
        let arrow_array = $self.downcast_ref();

        // List's length must be 1. Need to create
        assert_eq!($list.len(), 1);

        // a new ListArray whose length is equal to self.len()
        // Create a new ListArray to Arrow API
        let mut builder = get_list_builder(&$list.sub_data_type(), 3, 3);
        let arr_ref: ArrayRef = Arc::from($list.downcast_ref().value(0));
        let series = arr_ref.into_series();
        for _i in 0..$self.len() {
            builder.append_series(&series);
        }
        let df_list = builder.finish();
        let arrow_list = df_list.downcast_ref();

        // Call arrow2 API
        let arrow_res = contains(arrow_list, arrow_array)?;
        Ok(DFBooleanArray::from_arrow_array(arrow_res))
    }};
}

impl<T> ArrayContain for DataArray<T>
where T: DFNumericType
{
    fn contain(&self, list: &DFListArray) -> Result<DFBooleanArray>
    where Self: std::marker::Sized {
        contain_internal!(self, list)
    }
}

impl ArrayContain for DFUtf8Array {
    fn contain(&self, list: &DFListArray) -> Result<DFBooleanArray>
    where Self: std::marker::Sized {
        contain_internal!(self, list)
    }
}
