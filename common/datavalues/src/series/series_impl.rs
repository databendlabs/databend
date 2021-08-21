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

use core::fmt;
use std::convert::TryFrom;
use std::ops::Deref;
use std::sync::Arc;

use common_arrow::arrow::array::ArrayRef;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;
use crate::DataValue;

#[derive(Debug, Clone)]
pub struct Series(pub Arc<dyn SeriesTrait>);

impl<'a> AsRef<(dyn SeriesTrait + 'a)> for Series {
    fn as_ref(&self) -> &(dyn SeriesTrait + 'a) {
        &*self.0
    }
}

impl Deref for Series {
    type Target = dyn SeriesTrait;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

pub trait IntoSeries {
    fn into_series(self) -> Series
    where Self: Sized;
}

pub trait SeriesTrait: Send + Sync + fmt::Debug {
    fn data_type(&self) -> DataType;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn is_null(&self, row: usize) -> bool;
    fn null_count(&self) -> usize;

    fn get_array_memory_size(&self) -> usize;
    fn get_array_ref(&self) -> ArrayRef;
    fn to_values(&self) -> Result<Vec<DataValue>>;
    fn slice(&self, offset: usize, length: usize) -> Series;

    /// # Safety
    /// Note this doesn't do any bound checking, for performance reason.
    unsafe fn equal_element(&self, idx_self: usize, idx_other: usize, other: &Series) -> bool;

    fn cast_with_type(&self, data_type: &DataType) -> Result<Series>;

    fn if_then_else(&self, rhs: &Series, predicate: &Series) -> Result<Series>;

    fn try_get(&self, index: usize) -> Result<DataValue>;

    fn vec_hash(&self, hasher: DFHasher) -> Result<DFUInt64Array>;
    fn fixed_hash(&self, ptr: *mut u8, step: usize) -> Result<()>;
    fn serialize(&self, vec: &mut Vec<Vec<u8>>) -> Result<()>;

    fn subtract(&self, rhs: &Series) -> Result<Series>;
    fn add_to(&self, rhs: &Series) -> Result<Series>;
    fn multiply(&self, rhs: &Series) -> Result<Series>;
    fn divide(&self, rhs: &Series) -> Result<Series>;
    fn remainder(&self, rhs: &Series, dtype: &DataType) -> Result<Series>;
    fn negative(&self) -> Result<Series>;

    fn sum(&self) -> Result<DataValue>;
    fn max(&self) -> Result<DataValue>;
    fn min(&self) -> Result<DataValue>;
    fn arg_min(&self) -> Result<DataValue>;
    fn arg_max(&self) -> Result<DataValue>;

    /// Unpack to DFArray of data_type i8
    fn i8(&self) -> Result<&DFInt8Array> {
        Err(ErrorCode::IllegalDataType(format!(
            "{:?} != i8",
            self.data_type()
        )))
    }

    /// Unpack to DFArray i16
    fn i16(&self) -> Result<&DFInt16Array> {
        Err(ErrorCode::IllegalDataType(format!(
            "{:?} != i16",
            self.data_type()
        )))
    }

    fn i32(&self) -> Result<&DFInt32Array> {
        Err(ErrorCode::IllegalDataType(format!(
            "{:?} != i32",
            self.data_type()
        )))
    }

    /// Unpack to DFArray of data_type i64
    fn i64(&self) -> Result<&DFInt64Array> {
        Err(ErrorCode::IllegalDataType(format!(
            "{:?} != i64",
            self.data_type()
        )))
    }

    /// Unpack to DFArray of data_type f32
    fn f32(&self) -> Result<&DFFloat32Array> {
        Err(ErrorCode::IllegalDataType(format!(
            "{:?} != f32",
            self.data_type()
        )))
    }

    /// Unpack to DFArray of data_type f64
    fn f64(&self) -> Result<&DFFloat64Array> {
        Err(ErrorCode::IllegalDataType(format!(
            "{:?} != f64",
            self.data_type()
        )))
    }

    /// Unpack to DFArray of data_type u8
    fn u8(&self) -> Result<&DFUInt8Array> {
        Err(ErrorCode::IllegalDataType(format!(
            "{:?} != u8",
            self.data_type()
        )))
    }

    /// Unpack to DFArray of data_type u16
    fn u16(&self) -> Result<&DFUInt16Array> {
        Err(ErrorCode::IllegalDataType(format!(
            "{:?} != u16",
            self.data_type()
        )))
    }

    /// Unpack to DFArray of data_type u32
    fn u32(&self) -> Result<&DFUInt32Array> {
        Err(ErrorCode::IllegalDataType(format!(
            "{:?} != u32",
            self.data_type()
        )))
    }

    /// Unpack to DFArray of data_type u64
    fn u64(&self) -> Result<&DFUInt64Array> {
        Err(ErrorCode::IllegalDataType(format!(
            "{:?} != u32",
            self.data_type()
        )))
    }

    /// Unpack to DFArray of data_type bool
    fn bool(&self) -> Result<&DFBooleanArray> {
        Err(ErrorCode::IllegalDataType(format!(
            "{:?} != bool",
            self.data_type()
        )))
    }

    /// Unpack to DFArray of data_type utf8
    fn utf8(&self) -> Result<&DFUtf8Array> {
        Err(ErrorCode::IllegalDataType(format!(
            "{:?} != utf8",
            self.data_type()
        )))
    }

    /// Unpack to DFArray of data_type date32
    fn date32(&self) -> Result<&DFDate32Array> {
        Err(ErrorCode::IllegalDataType(format!(
            "{:?} != date32",
            self.data_type()
        )))
    }

    /// Unpack to DFArray of data_type date64
    fn date64(&self) -> Result<&DFDate64Array> {
        Err(ErrorCode::IllegalDataType(format!(
            "{:?} != date64",
            self.data_type()
        )))
    }

    /// Unpack to DFArray of data_type binary
    fn binary(&self) -> Result<&DFBinaryArray> {
        Err(ErrorCode::IllegalDataType(format!(
            "{:?} != binary",
            self.data_type()
        )))
    }

    /// Take by index from an iterator. This operation clones the data.
    ///
    /// # Safety
    ///
    /// Out of bounds access doesn't Error but will return a Null value
    fn take_iter(&self, _iter: &mut dyn Iterator<Item = usize>) -> Result<Series>;

    /// Take by index from an iterator. This operation clones the data.
    ///
    /// # Safety
    ///
    /// This doesn't check any bounds or null validity.
    unsafe fn take_iter_unchecked(&self, _iter: &mut dyn Iterator<Item = usize>) -> Result<Series>;

    /// scatter the arrays by indices, the size of indices must be equal to the size of array
    /// # Safety
    /// Note this doesn't do any bound checking, for performance reason.
    unsafe fn scatter_unchecked(
        &self,
        indices: &mut dyn Iterator<Item = u64>,
        scattered_size: usize,
    ) -> Result<Vec<Series>>;
}

impl<'a, T> AsRef<DataArray<T>> for dyn SeriesTrait + 'a
where T: 'static + DFDataType
{
    fn as_ref(&self) -> &DataArray<T> {
        if T::data_type() == self.data_type() ||
            // needed because we want to get ref of List no matter what the inner type is.
            (matches!(T::data_type(), DataType::List(_)) && matches!(self.data_type(), DataType::List(_)) )
        {
            unsafe { &*(self as *const dyn SeriesTrait as *const DataArray<T>) }
        } else {
            panic!(
                "implementation error, cannot get ref {:?} from {:?}",
                T::data_type(),
                self.data_type()
            )
        }
    }
}

pub trait SeriesFrom<T, Phantom: ?Sized> {
    /// Initialize by name and values.
    fn new(_: T) -> Self;
}

//
macro_rules! impl_from {
    ($type:ty, $series_var:ident, $method:ident) => {
        impl<T: AsRef<$type>> SeriesFrom<T, $type> for Series {
            fn new(v: T) -> Self {
                DataArray::<$series_var>::$method(v.as_ref()).into_series()
            }
        }
    };
}

impl<'a, T: AsRef<[&'a str]>> SeriesFrom<T, [&'a str]> for Series {
    fn new(v: T) -> Self {
        DFUtf8Array::new_from_slice(v.as_ref()).into_series()
    }
}

impl<'a, T: AsRef<[Option<&'a str>]>> SeriesFrom<T, [Option<&'a str>]> for Series {
    fn new(v: T) -> Self {
        DFUtf8Array::new_from_opt_slice(v.as_ref()).into_series()
    }
}

impl_from!([String], Utf8Type, new_from_slice);
impl_from!([bool], BooleanType, new_from_slice);
impl_from!([u8], UInt8Type, new_from_slice);
impl_from!([u16], UInt16Type, new_from_slice);
impl_from!([u32], UInt32Type, new_from_slice);
impl_from!([u64], UInt64Type, new_from_slice);
impl_from!([i8], Int8Type, new_from_slice);
impl_from!([i16], Int16Type, new_from_slice);
impl_from!([i32], Int32Type, new_from_slice);
impl_from!([i64], Int64Type, new_from_slice);
impl_from!([f32], Float32Type, new_from_slice);
impl_from!([f64], Float64Type, new_from_slice);
impl_from!([Option<String>], Utf8Type, new_from_opt_slice);
impl_from!([Option<bool>], BooleanType, new_from_opt_slice);
impl_from!([Option<u8>], UInt8Type, new_from_opt_slice);
impl_from!([Option<u16>], UInt16Type, new_from_opt_slice);
impl_from!([Option<u32>], UInt32Type, new_from_opt_slice);
impl_from!([Option<u64>], UInt64Type, new_from_opt_slice);
impl_from!([Option<i8>], Int8Type, new_from_opt_slice);
impl_from!([Option<i16>], Int16Type, new_from_opt_slice);
impl_from!([Option<i32>], Int32Type, new_from_opt_slice);
impl_from!([Option<i64>], Int64Type, new_from_opt_slice);
impl_from!([Option<f32>], Float32Type, new_from_opt_slice);
impl_from!([Option<f64>], Float64Type, new_from_opt_slice);

impl IntoSeries for ArrayRef {
    fn into_series(self) -> Series
    where Self: Sized {
        let data_type = DataType::try_from(self.data_type()).unwrap();
        match data_type {
            DataType::Null => DFNullArray::new(self).into_series(),
            DataType::Boolean => DFBooleanArray::new(self).into_series(),
            DataType::UInt8 => DFUInt8Array::new(self).into_series(),
            DataType::UInt16 => DFUInt16Array::new(self).into_series(),
            DataType::UInt32 => DFUInt32Array::new(self).into_series(),
            DataType::UInt64 => DFUInt64Array::new(self).into_series(),

            DataType::Int8 => DFInt8Array::new(self).into_series(),
            DataType::Int16 => DFInt16Array::new(self).into_series(),
            DataType::Int32 => DFInt32Array::new(self).into_series(),
            DataType::Int64 => DFInt64Array::new(self).into_series(),

            DataType::Float32 => DFFloat32Array::new(self).into_series(),
            DataType::Float64 => DFFloat64Array::new(self).into_series(),
            DataType::Utf8 => DFUtf8Array::new(self).into_series(),
            DataType::Date32 => DFDate32Array::new(self).into_series(),
            DataType::Date64 => DFDate64Array::new(self).into_series(),

            DataType::List(_) => DFListArray::new(self).into_series(),
            DataType::Struct(_) => DFStructArray::new(self).into_series(),
            DataType::Binary => DFBinaryArray::new(self).into_series(),

            _ => unreachable!(),
        }
    }
}

impl Series {
    /// Check if series are equal. Note that `None == None` evaluates to `false`
    pub fn series_equal(&self, other: &Series) -> bool {
        if self.get_data_ptr() == other.get_data_ptr() {
            return true;
        }
        if self.len() != other.len() {
            return false;
        }
        if self.null_count() != other.null_count() {
            return false;
        }

        match self.eq(other) {
            Ok(arr) => arr.all_true(),
            Err(_) => false,
        }
    }

    /// Get a pointer to the underlying data of this Series.
    /// Can be useful for fast comparisons.
    pub fn get_data_ptr(&self) -> usize {
        let object = self.0.deref();

        // Safety:
        // A fat pointer consists of a data ptr and a ptr to the vtable.
        // we specifically check that we only transmute &dyn SeriesTrait e.g.
        // a trait object, therefore this is sound.
        let (data_ptr, _vtable_ptr) =
            unsafe { std::mem::transmute::<&dyn SeriesTrait, (usize, usize)>(object) };
        data_ptr
    }

    pub fn static_cast<T>(&self) -> &DataArray<T> {
        let object = self.0.deref();
        unsafe { &*(object as *const dyn SeriesTrait as *const DataArray<T>) }
    }
}
