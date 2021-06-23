// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use core::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use common_arrow::arrow::array::ArrayRef;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::data_df_type::*;
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

// TODO: add types
// impl std::convert::TryFrom<(&str, Vec<ArrayRef>)> for DataArrayRef {
//     type Error = ErrorCode;

//     fn try_from(name_arr: (&str, Vec<ArrayRef>)) -> Result<Self> {
//         let (name, chunks) = name_arr;

//         let mut chunks_iter = chunks.iter();
//         let data_type: &ArrowDataType = chunks_iter
//             .next()
//             .ok_or_else(|| PolarsError::NoData("Expected at least on ArrayRef".into()))?
//             .data_type();

//         for chunk in chunks_iter {
//             if chunk.data_type() != data_type {
//                 return Err(PolarsError::InvalidOperation(
//                     "Cannot create series from multiple arrays with different types".into(),
//                 ));
//             }
//         }
//     }
// }
