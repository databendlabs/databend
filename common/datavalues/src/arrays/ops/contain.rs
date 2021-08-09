// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;

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

pub trait ArrayContain: Debug {
    /// # Safety
    /// Note this doesn't do any bound checking, for performance reason.
    ///
    unsafe fn contain_unchecked(
        &self,
        _list: &Self,
    ) -> Result<Self>
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
    unsafe fn contain_unchecked(&self, list: &Self) -> Result<Self>
    where Self: std::marker::Sized,
    {
        let array = self.downcast_ref();
        


    }
}
