// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;
use std::collections::HashSet;
use std::hash::Hash;
use std::hash::Hasher;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::arrays::get_list_builder;
use crate::arrays::BinaryArrayBuilder;
use crate::arrays::BooleanArrayBuilder;
use crate::arrays::DataArray;
use crate::arrays::PrimitiveArrayBuilder;
use crate::arrays::Utf8ArrayBuilder;
use crate::prelude::*;
use crate::utils::get_iter_capacity;
use common_arrow::arrow::compute::arity::unary;
use common_arrow::arrow::compute::contains::contains;

pub trait ArrayContain: Debug {
    /// # Safety
    ///
    fn contain(
        &self,
        _list: &DFListArray,
    ) -> Result<DFBooleanArray>
    where
        Self: std::marker::Sized,
    {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported apply contain operation for {:?}",
            self,
        )))
    }
}

impl<T> ArrayContain for DataArray<T>
where T: DFNumericType
{
    fn contain(&self, list: &DFListArray) -> Result<DFBooleanArray>
    where Self: std::marker::Sized,
    {
        let arrow_array = self.downcast_ref();
        let arrow_list = list.downcast_ref();
        let arrow_res = contains(arrow_list, arrow_array)?;
        Ok(DFBooleanArray::from_arrow_array(arrow_res)) 
    }
}
