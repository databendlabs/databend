// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;

use common_arrow::arrow::compute::contains::contains;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::arrays::get_list_builder;
use crate::arrays::DataArray;
use crate::prelude::*;

pub trait ArrayContain: Debug {
    /// Ex: array = [1,2,3,4,5] and list = [2,3,5],
    /// then the result should be [false, true, true, false, true].
    fn contain_inlist(&self, _list: &Series) -> Result<DFBooleanArray>
    where Self: std::marker::Sized {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported apply contain operation for {:?}",
            self,
        )))
    }

    /// Ex: array = [1,2,3] and list = [[1,3,5], [2,6], [1,2]],
    /// then the result should be [true, true, false].
    fn contain(&self, _list: &DFListArray) -> Result<DFBooleanArray>
    where Self: std::marker::Sized {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported apply contain operation for {:?}",
            self,
        )))
    }
}

macro_rules! contain_inlist_internal {
    ($self:expr, $list:expr) => {{
        let arrow_array = $self.downcast_ref();

        // Create a new ListArray whose length is equal to self.len()
        // by duplicating the input list self.len() times
        let mut builder = get_list_builder(&$list.data_type(), $list.len(), $self.len());
        for _i in 0..$self.len() {
            builder.append_series($list);
        }
        let df_list = builder.finish();
        // Transform into arrow ListArray
        let arrow_list = df_list.downcast_ref();

        // Call arrow2 API
        let arrow_res = contains(arrow_list, arrow_array)?;
        Ok(DFBooleanArray::from_arrow_array(arrow_res))
    }};
}

macro_rules! contain_internal {
    ($self:expr, $list_arr:expr) => {{
        assert_eq!($self.len(), $list_arr.len());
        let arrow_array = $self.downcast_ref();
        let arrow_list = $list_arr.downcast_ref();
        let arrow_res = contains(arrow_list, arrow_array)?;
        Ok(DFBooleanArray::from_arrow_array(arrow_res))
    }};
}

impl<T> ArrayContain for DataArray<T>
where T: DFNumericType
{
    fn contain_inlist(&self, list: &Series) -> Result<DFBooleanArray>
    where Self: std::marker::Sized {
        contain_inlist_internal!(self, list)
    }

    fn contain(&self, list: &DFListArray) -> Result<DFBooleanArray>
    where Self: std::marker::Sized {
        contain_internal!(self, list)
    }
}

impl ArrayContain for DFUtf8Array {
    fn contain_inlist(&self, list: &Series) -> Result<DFBooleanArray>
    where Self: std::marker::Sized {
        contain_inlist_internal!(self, list)
    }

    fn contain(&self, list: &DFListArray) -> Result<DFBooleanArray>
    where Self: std::marker::Sized {
        contain_internal!(self, list)
    }
}
