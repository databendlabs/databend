// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Deref;
use std::sync::Arc;

use common_exception::Result;

use crate::arrays::ops::ArrayCast;
use crate::arrays::*;
use crate::*;

pub struct DataArrayWrap<T>(pub T);

impl<T> From<DataArrayBase<T>> for DataArrayWrap<DataArrayBase<T>> {
    fn from(da: DataArrayBase<T>) -> Self {
        DataArrayWrap(da)
    }
}

impl<T> Deref for DataArrayWrap<DataArrayBase<T>> {
    type Target = DataArrayBase<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

macro_rules! impl_dyn_array {
    ($da: ident) => {
        impl IntoDataArray for $da {
            fn into_array(self) -> DataArrayRef {
                Arc::new(DataArrayWrap(self))
            }
        }

        impl Debug for DataArrayWrap<$da> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
                write!(
                    f,
                    "Column: data_type: {:?}, size: {:?}",
                    self.data_type(),
                    self.len()
                )
            }
        }

        impl DataArray for DataArrayWrap<$da> {
            fn data_type(&self) -> DataType {
                self.0.data_type()
            }
            fn len(&self) -> usize {
                self.0.len()
            }

            fn is_empty(&self) -> bool {
                self.0.is_empty()
            }

            fn get_array_memory_size(&self) -> usize {
                self.0.get_array_memory_size()
            }

            fn slice(&self, offset: usize, length: usize) -> DataArrayRef {
                self.0.slice(offset, length).into_array()
            }

            fn cast_with_type(&self, data_type: &DataType) -> Result<DataArrayRef> {
                ArrayCast::cast_with_type(&self.0, data_type)
            }

            fn try_get(&self, index: usize) -> Result<DataValue> {
                unsafe { self.0.try_get(index) }
            }
        }
    };
}

impl_dyn_array!(DFFloat32Array);
impl_dyn_array!(DFFloat64Array);
impl_dyn_array!(DFUInt8Array);
impl_dyn_array!(DFUInt16Array);
impl_dyn_array!(DFUInt32Array);
impl_dyn_array!(DFUInt64Array);
impl_dyn_array!(DFInt8Array);
impl_dyn_array!(DFInt16Array);
impl_dyn_array!(DFInt32Array);
impl_dyn_array!(DFInt64Array);
impl_dyn_array!(DFStringArray);
impl_dyn_array!(DFListArray);
impl_dyn_array!(DFBooleanArray);
