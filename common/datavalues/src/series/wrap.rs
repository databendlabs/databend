// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Deref;
use std::sync::Arc;

use ahash::RandomState;
use common_arrow::arrow::array::ArrayRef;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::arrays::*;
use crate::series::*;
use crate::*;

pub struct SeriesWrap<T>(pub T);

impl<T> From<DataArray<T>> for SeriesWrap<DataArray<T>> {
    fn from(da: DataArray<T>) -> Self {
        SeriesWrap(da)
    }
}

impl<T> Deref for SeriesWrap<DataArray<T>> {
    type Target = DataArray<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

macro_rules! impl_dyn_array {
    ($da: ident) => {
        impl IntoSeries for $da {
            fn into_series(self) -> Series {
                Series(Arc::new(SeriesWrap(self)))
            }
        }

        impl Debug for SeriesWrap<$da> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
                write!(
                    f,
                    "Column: data_type: {:?}, size: {:?}",
                    self.data_type(),
                    self.len()
                )
            }
        }

        impl SeriesTrait for SeriesWrap<$da> {
            fn data_type(&self) -> DataType {
                self.0.data_type()
            }
            fn len(&self) -> usize {
                self.0.len()
            }

            fn is_empty(&self) -> bool {
                self.0.is_empty()
            }

            fn is_null(&self, row: usize) -> bool {
                self.0.is_null(row)
            }

            fn null_count(&self) -> usize {
                self.0.null_count()
            }

            fn get_array_memory_size(&self) -> usize {
                self.0.get_array_memory_size()
            }

            fn get_array_ref(&self) -> ArrayRef {
                self.0.get_array_ref()
            }

            fn slice(&self, offset: usize, length: usize) -> Series {
                self.0.slice(offset, length).into_series()
            }

            unsafe fn equal_element(
                &self,
                idx_self: usize,
                idx_other: usize,
                other: &Series,
            ) -> bool {
                self.0.equal_element(idx_self, idx_other, other)
            }

            fn cast_with_type(&self, data_type: &DataType) -> Result<Series> {
                ArrayCast::cast_with_type(&self.0, data_type)
            }

            fn try_get(&self, index: usize) -> Result<DataValue> {
                unsafe { self.0.try_get(index) }
            }

            fn vec_hash(&self, random_state: RandomState) -> DFUInt64Array {
                self.0.vec_hash(random_state)
            }

            fn subtract(&self, rhs: &Series) -> Result<Series> {
                NumOpsDispatch::subtract(&self.0, rhs)
            }
            fn add_to(&self, rhs: &Series) -> Result<Series> {
                NumOpsDispatch::add_to(&self.0, rhs)
            }
            fn multiply(&self, rhs: &Series) -> Result<Series> {
                NumOpsDispatch::multiply(&self.0, rhs)
            }
            fn divide(&self, rhs: &Series) -> Result<Series> {
                NumOpsDispatch::divide(&self.0, rhs)
            }
            fn remainder(&self, rhs: &Series) -> Result<Series> {
                NumOpsDispatch::remainder(&self.0, rhs)
            }

            fn i8(&self) -> Result<&DFInt8Array> {
                if matches!(self.0.data_type(), DataType::Int8) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFInt8Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series: {:?} of type {:?} into i8",
                        self.name(),
                        self.data_type(),
                    )))
                }
            }

            // For each column create a series
            fn i16(&self) -> Result<&DFInt16Array> {
                if matches!(self.0.data_type(), DataType::Int16) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFInt16Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series: {:?} of type {:?} into i16",
                        self.name(),
                        self.data_type(),
                    )))
                }
            }

            fn i32(&self) -> Result<&DFInt32Array> {
                if matches!(self.0.data_type(), DataType::Int32) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFInt32Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series: {:?} of type {:?} into i32",
                        self.name(),
                        self.data_type(),
                    )))
                }
            }

            fn i64(&self) -> Result<&DFInt64Array> {
                if matches!(self.0.data_type(), DataType::Int64) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFInt64Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series: {:?} of type {:?} into i64",
                        self.name(),
                        self.data_type(),
                    )))
                }
            }

            fn f32(&self) -> Result<&DFFloat32Array> {
                if matches!(self.0.data_type(), DataType::Float32) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFFloat32Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series: {:?} of type {:?} into f32",
                        self.name(),
                        self.data_type(),
                    )))
                }
            }

            fn f64(&self) -> Result<&DFFloat64Array> {
                if matches!(self.0.data_type(), DataType::Float64) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFFloat64Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series: {:?} of type {:?} into f64",
                        self.name(),
                        self.data_type(),
                    )))
                }
            }

            fn u8(&self) -> Result<&DFUInt8Array> {
                if matches!(self.0.data_type(), DataType::UInt8) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFUInt8Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series: {:?} of type {:?} into u8",
                        self.name(),
                        self.data_type(),
                    )))
                }
            }

            fn u16(&self) -> Result<&DFUInt16Array> {
                if matches!(self.0.data_type(), DataType::UInt16) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFUInt16Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series: {:?} of type {:?} into u16",
                        self.name(),
                        self.data_type(),
                    )))
                }
            }

            fn u32(&self) -> Result<&DFUInt32Array> {
                if matches!(self.0.data_type(), DataType::UInt32) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFUInt32Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series: {:?} of type {:?} into u32",
                        self.name(),
                        self.data_type(),
                    )))
                }
            }

            fn u64(&self) -> Result<&DFUInt64Array> {
                if matches!(self.0.data_type(), DataType::UInt64) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFUInt64Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series: {:?} of type {:?} into u64",
                        self.name(),
                        self.data_type(),
                    )))
                }
            }

            fn bool(&self) -> Result<&DFBooleanArray> {
                if matches!(self.0.data_type(), DataType::Boolean) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFBooleanArray)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series: {:?} of type {:?} into bool",
                        self.name(),
                        self.data_type(),
                    )))
                }
            }

            fn utf8(&self) -> Result<&DFStringArray> {
                if matches!(self.0.data_type(), DataType::Utf8) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFStringArray)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series: {:?} of type {:?} into utf8",
                        self.name(),
                        self.data_type(),
                    )))
                }
            }

            fn date32(&self) -> Result<&DFDate32Array> {
                if matches!(self.0.data_type(), DataType::Date32) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFDate32Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series: {:?} of type {:?} into date32",
                        self.name(),
                        self.data_type(),
                    )))
                }
            }

            fn date64(&self) -> Result<&DFDate64Array> {
                if matches!(self.0.data_type(), DataType::Date64) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFDate64Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series: {:?} of type {:?} into date64",
                        self.name(),
                        self.data_type(),
                    )))
                }
            }
        }
    };
}

impl_dyn_array!(DFNullArray);
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
impl_dyn_array!(DFBinaryArray);
impl_dyn_array!(DFStructArray);
