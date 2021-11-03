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
    fn into_series(self) -> Series;
}

pub trait SeriesTrait: Send + Sync + fmt::Debug {
    fn data_type(&self) -> &DataType;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn is_null(&self, row: usize) -> bool;
    fn null_count(&self) -> usize;

    fn get_array_memory_size(&self) -> usize;
    fn get_array_ref(&self) -> ArrayRef;
    fn to_values(&self) -> Result<Vec<DataValue>>;
    fn slice(&self, offset: usize, length: usize) -> Series;

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
            "{:?} != u64",
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

    /// Unpack to DFArray of data_type string
    fn string(&self) -> Result<&DFStringArray> {
        Err(ErrorCode::IllegalDataType(format!(
            "{:?} != string",
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

pub trait SeriesFrom<T, Phantom: ?Sized> {
    /// Initialize by name and values.
    fn new(_: T) -> Self;
}

macro_rules! impl_from {
    ($type:ty, $array:ident, $method:ident) => {
        impl<T: AsRef<$type>> SeriesFrom<T, $type> for Series {
            fn new(v: T) -> Self {
                $array::$method(v.as_ref()).into_series()
            }
        }
    };
}

impl<'a, T: AsRef<[&'a str]>> SeriesFrom<T, [&'a str]> for Series {
    fn new(v: T) -> Self {
        DFStringArray::new_from_slice(v.as_ref()).into_series()
    }
}

impl<'a, T: AsRef<[Option<&'a str>]>> SeriesFrom<T, [Option<&'a str>]> for Series {
    fn new(v: T) -> Self {
        DFStringArray::new_from_opt_slice(v.as_ref()).into_series()
    }
}

impl<'a, T: AsRef<[&'a [u8]]>> SeriesFrom<T, [&'a [u8]]> for Series {
    fn new(v: T) -> Self {
        DFStringArray::new_from_slice(v.as_ref()).into_series()
    }
}

impl<'a, T: AsRef<[Option<&'a [u8]>]>> SeriesFrom<T, [Option<&'a [u8]>]> for Series {
    fn new(v: T) -> Self {
        DFStringArray::new_from_opt_slice(v.as_ref()).into_series()
    }
}

impl_from!([bool], DFBooleanArray, new_from_slice);
impl_from!([u8], DFUInt8Array, new_from_slice);
impl_from!([u16], DFUInt16Array, new_from_slice);
impl_from!([u32], DFUInt32Array, new_from_slice);
impl_from!([u64], DFUInt64Array, new_from_slice);
impl_from!([i8], DFInt8Array, new_from_slice);
impl_from!([i16], DFInt16Array, new_from_slice);
impl_from!([i32], DFInt32Array, new_from_slice);
impl_from!([i64], DFInt64Array, new_from_slice);
impl_from!([f32], DFFloat32Array, new_from_slice);
impl_from!([f64], DFFloat64Array, new_from_slice);
impl_from!([Vec<u8>], DFStringArray, new_from_slice);

impl_from!([Option<bool>], DFBooleanArray, new_from_opt_slice);
impl_from!([Option<u8>], DFUInt8Array, new_from_opt_slice);
impl_from!([Option<u16>], DFUInt16Array, new_from_opt_slice);
impl_from!([Option<u32>], DFUInt32Array, new_from_opt_slice);
impl_from!([Option<u64>], DFUInt64Array, new_from_opt_slice);
impl_from!([Option<i8>], DFInt8Array, new_from_opt_slice);
impl_from!([Option<i16>], DFInt16Array, new_from_opt_slice);
impl_from!([Option<i32>], DFInt32Array, new_from_opt_slice);
impl_from!([Option<i64>], DFInt64Array, new_from_opt_slice);
impl_from!([Option<f32>], DFFloat32Array, new_from_opt_slice);
impl_from!([Option<f64>], DFFloat64Array, new_from_opt_slice);
impl_from!([Option<Vec<u8>>], DFStringArray, new_from_opt_slice);

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

    /// Get a pointer to the underlying data of this Series.
    /// Can be useful for fast comparisons.
    pub fn static_cast<T>(&self) -> &T {
        let object = self.0.deref();
        unsafe { &*(object as *const dyn SeriesTrait as *const T) }
    }
}

impl IntoSeries for ArrayRef {
    fn into_series(self) -> Series {
        let data_type = DataType::try_from(self.data_type()).unwrap();
        let physical_type: PhysicalDataType = data_type.into();

        use PhysicalDataType::*;
        match physical_type {
            Null => DFNullArray::from_arrow_array(self.as_ref()).into_series(),
            Boolean => DFBooleanArray::from_arrow_array(self.as_ref()).into_series(),
            UInt8 => DFUInt8Array::from_arrow_array(self.as_ref()).into_series(),
            UInt16 => DFUInt16Array::from_arrow_array(self.as_ref()).into_series(),
            UInt32 => DFUInt32Array::from_arrow_array(self.as_ref()).into_series(),
            UInt64 => DFUInt64Array::from_arrow_array(self.as_ref()).into_series(),

            Int8 => DFInt8Array::from_arrow_array(self.as_ref()).into_series(),
            Int16 => DFInt16Array::from_arrow_array(self.as_ref()).into_series(),
            Int32 => DFInt32Array::from_arrow_array(self.as_ref()).into_series(),
            Int64 => DFInt64Array::from_arrow_array(self.as_ref()).into_series(),

            Float32 => DFFloat32Array::from_arrow_array(self.as_ref()).into_series(),
            Float64 => DFFloat64Array::from_arrow_array(self.as_ref()).into_series(),

            List(_) => DFListArray::from_arrow_array(self.as_ref()).into_series(),
            Struct(_) => DFStructArray::from_arrow_array(self.as_ref()).into_series(),
            String => DFStringArray::from_arrow_array(self.as_ref()).into_series(),
        }
    }
}
