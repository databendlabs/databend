// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::compute::aggregate;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;
use crate::series::*;

pub struct SeriesWrap<T>(pub T);

impl<T> From<T> for SeriesWrap<T> {
    fn from(da: T) -> Self {
        SeriesWrap(da)
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
            fn data_type(&self) -> &DataType {
                self.0.data_type()
            }

            fn len(&self) -> usize {
                self.0.len()
            }

            fn is_empty(&self) -> bool {
                self.len() == 0
            }

            fn is_null(&self, row: usize) -> bool {
                self.0.array.is_null(row)
            }

            fn null_count(&self) -> usize {
                self.0.null_count()
            }

            fn get_array_memory_size(&self) -> usize {
                aggregate::estimated_bytes_size(&self.0.array)
            }

            fn get_array_ref(&self) -> ArrayRef {
                Arc::new(self.0.array.clone()) as ArrayRef
            }

            fn to_values(&self) -> Result<Vec<DataValue>> {
                self.0.to_values()
            }

            fn slice(&self, offset: usize, length: usize) -> Series {
                self.0.slice(offset, length).into_series()
            }

            fn cast_with_type(&self, data_type: &DataType) -> Result<Series> {
                ArrayCast::cast_with_type(&self.0, data_type)
            }

            fn if_then_else(&self, rhs: &Series, predicate: &Series) -> Result<Series> {
                if predicate.data_type() != &DataType::Boolean {
                    return Err(ErrorCode::BadDataValueType(
                        "If function requires the first argument type must be Boolean",
                    ));
                }

                if self.data_type() != rhs.data_type() {
                    return Err(ErrorCode::BadArguments(format!(
                        "If then else requires the arguments to have the same datatypes ({} != {})",
                        self.data_type(),
                        rhs.data_type()
                    )));
                }

                let rhs = unsafe { self.0.unpack(rhs)? };
                Ok(self.0.if_then_else(rhs, predicate.bool()?)?.into_series())
            }

            fn try_get(&self, index: usize) -> Result<DataValue> {
                unsafe { self.0.try_get(index) }
            }

            fn vec_hash(&self, hasher: DFHasher) -> Result<DFUInt64Array> {
                self.0.vec_hash(hasher)
            }

            fn fixed_hash(&self, ptr: *mut u8, step: usize) -> Result<()> {
                self.0.fixed_hash(ptr, step)
            }

            fn serialize(&self, vec: &mut Vec<Vec<u8>>) -> Result<()> {
                self.0.serialize(vec)
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

            fn remainder(&self, rhs: &Series, dtype: &DataType) -> Result<Series> {
                NumOpsDispatch::remainder(&self.0, rhs, dtype)
            }

            fn negative(&self) -> Result<Series> {
                NumOpsDispatch::negative(&self.0)
            }

            fn sum(&self) -> Result<DataValue> {
                self.0.sum()
            }

            fn max(&self) -> Result<DataValue> {
                self.0.max()
            }
            fn min(&self) -> Result<DataValue> {
                self.0.min()
            }
            fn arg_max(&self) -> Result<DataValue> {
                self.0.arg_max()
            }
            fn arg_min(&self) -> Result<DataValue> {
                self.0.arg_min()
            }

            fn i8(&self) -> Result<&DFInt8Array> {
                if matches!(self.0.data_type(), &DataType::Int8) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFInt8Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series of type {:?} into i8",
                        self.data_type(),
                    )))
                }
            }

            // For each column create a series
            fn i16(&self) -> Result<&DFInt16Array> {
                if matches!(self.0.data_type(), &DataType::Int16) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFInt16Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series of type {:?} into i16",
                        self.data_type(),
                    )))
                }
            }

            fn i32(&self) -> Result<&DFInt32Array> {
                if matches!(self.0.data_type(), &DataType::Int32) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFInt32Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series of type {:?} into i32",
                        self.data_type(),
                    )))
                }
            }

            fn i64(&self) -> Result<&DFInt64Array> {
                if matches!(self.0.data_type(), &DataType::Int64) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFInt64Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series of type {:?} into i64",
                        self.data_type(),
                    )))
                }
            }

            fn f32(&self) -> Result<&DFFloat32Array> {
                if matches!(self.0.data_type(), &DataType::Float32) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFFloat32Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series of type {:?} into f32",
                        self.data_type(),
                    )))
                }
            }

            fn f64(&self) -> Result<&DFFloat64Array> {
                if matches!(self.0.data_type(), &DataType::Float64) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFFloat64Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series of type {:?} into f64",
                        self.data_type(),
                    )))
                }
            }

            fn u8(&self) -> Result<&DFUInt8Array> {
                if matches!(self.0.data_type(), &DataType::UInt8) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFUInt8Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series of type {:?} into u8",
                        self.data_type(),
                    )))
                }
            }

            fn u16(&self) -> Result<&DFUInt16Array> {
                if matches!(self.0.data_type(), &DataType::UInt16) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFUInt16Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series of type {:?} into u16",
                        self.data_type(),
                    )))
                }
            }

            fn u32(&self) -> Result<&DFUInt32Array> {
                if matches!(self.0.data_type(), &DataType::UInt32) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFUInt32Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series of type {:?} into u32",
                        self.data_type(),
                    )))
                }
            }

            fn u64(&self) -> Result<&DFUInt64Array> {
                if matches!(self.0.data_type(), &DataType::UInt64) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFUInt64Array)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series of type {:?} into u64",
                        self.data_type(),
                    )))
                }
            }

            fn bool(&self) -> Result<&DFBooleanArray> {
                if matches!(self.0.data_type(), &DataType::Boolean) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFBooleanArray)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series of type {:?} into bool",
                        self.data_type(),
                    )))
                }
            }

            /// Unpack to DFArray of data_type string
            fn string(&self) -> Result<&DFStringArray> {
                if matches!(self.0.data_type(), &DataType::String) {
                    unsafe { Ok(&*(self as *const dyn SeriesTrait as *const DFStringArray)) }
                } else {
                    Err(ErrorCode::IllegalDataType(format!(
                        "cannot unpack Series of type {:?} into string",
                        self.data_type(),
                    )))
                }
            }

            fn take_iter(&self, iter: &mut dyn Iterator<Item = usize>) -> Result<Series> {
                Ok(ArrayTake::take(&self.0, iter.into())?.into_series())
            }

            unsafe fn take_iter_unchecked(
                &self,
                iter: &mut dyn Iterator<Item = usize>,
            ) -> Result<Series> {
                Ok(ArrayTake::take_unchecked(&self.0, iter.into())?.into_series())
            }

            /// scatter the arrays by indices, the size of indices must be equal to the size of array
            unsafe fn scatter_unchecked(
                &self,
                indices: &mut dyn Iterator<Item = u64>,
                scattered_size: usize,
            ) -> Result<Vec<Series>> {
                let results = ArrayScatter::scatter_unchecked(&self.0, indices, scattered_size)?;
                Ok(results
                    .iter()
                    .map(|array| array.clone().into_series())
                    .collect())
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
impl_dyn_array!(DFListArray);
impl_dyn_array!(DFBooleanArray);
impl_dyn_array!(DFStringArray);
impl_dyn_array!(DFStructArray);
