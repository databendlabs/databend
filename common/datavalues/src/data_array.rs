// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use core::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::data_df_type::*;
use crate::DataColumn;
use crate::DataType;
use crate::DataValue;

pub type DataArrayRef = Arc<dyn DataArray>;

pub trait IntoDataArray {
    fn into_array(self) -> DataArrayRef
    where Self: Sized;
}

pub trait DataArray: Send + Sync + fmt::Debug {
    fn data_type(&self) -> DataType;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn get_array_memory_size(&self) -> usize;
    fn slice(&self, offset: usize, length: usize) -> DataArrayRef;

    fn cast_with_type(&self, data_type: &DataType) -> Result<DataArrayRef>;
    fn try_get(&self, index: usize) -> Result<DataValue>;
}
