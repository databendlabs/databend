// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;
use crate::Column;
use crate::ColumnRef;
use crate::NewColumn;
use crate::NullableColumn;
use crate::StringColumn;

// Series is a util struct to work with Column
// Maybe rename to ColumnHelper later
pub struct Series;

impl Series {
    /// Get a pointer to the underlying data of this Series.
    /// Can be useful for fast comparisons.
    /// # Safety
    /// Assumes that the `column` is  T.
    pub unsafe fn static_cast<T>(column: &ColumnRef) -> &T {
        let object = column.as_ref();
        &*(object as *const dyn Column as *const T)
    }

    pub fn check_get<T: 'static + Column>(column: &ColumnRef) -> Result<&T> {
        let arr = column.as_any().downcast_ref::<T>().ok_or_else(|| {
            ErrorCode::UnknownColumn(format!(
                "downcast column error, column type: {:?}",
                column.data_type_id(),
            ))
        });
        arr
    }
}

pub trait SeriesFrom<T, Phantom: ?Sized> {
    /// Initialize by name and values.
    fn from_data(_: T) -> ColumnRef;
}

macro_rules! impl_from {
    ($type:ty, $array:ident) => {
        impl<T: AsRef<$type>> SeriesFrom<T, $type> for Series {
            fn from_data(v: T) -> ColumnRef {
                Arc::new($array::new_from_slice(v.as_ref()))
            }
        }
    };
}

macro_rules! impl_from_option {
    ($type:ty, $array:ident, $default:expr) => {
        impl<T: AsRef<$type>> SeriesFrom<T, $type> for Series {
            fn from_data(v: T) -> ColumnRef {
                let iter = v.as_ref().iter().map(|v| v.is_some());
                let bitmap = MutableBitmap::from_iter(iter);

                let iter = v.as_ref().iter().map(|v| v.unwrap_or($default));
                let column = $array::new_from_iter(iter);

                Arc::new(NullableColumn::new(Arc::new(column), bitmap.into()))
            }
        }
    };
}

impl<'a, T: AsRef<[&'a str]>> SeriesFrom<T, [&'a str]> for Series {
    fn from_data(v: T) -> ColumnRef {
        Arc::new(StringColumn::new_from_slice(v))
    }
}

impl<'a, T: AsRef<[Option<&'a str>]>> SeriesFrom<T, [Option<&'a str>]> for Series {
    fn from_data(v: T) -> ColumnRef {
        let iter = v.as_ref().iter().map(|v| v.is_some());
        let bitmap = MutableBitmap::from_iter(iter);

        let iter = v.as_ref().iter().map(|v| v.unwrap_or(""));
        let column = StringColumn::new_from_iter(iter);
        Arc::new(NullableColumn::new(Arc::new(column), bitmap.into()))
    }
}

impl<'a, T: AsRef<[&'a [u8]]>> SeriesFrom<T, [&'a [u8]]> for Series {
    fn from_data(v: T) -> ColumnRef {
        let column = StringColumn::new_from_iter(v.as_ref().iter());
        Arc::new(column)
    }
}

impl<'a, T: AsRef<[Option<&'a [u8]>]>> SeriesFrom<T, [Option<&'a [u8]>]> for Series {
    fn from_data(v: T) -> ColumnRef {
        let iter = v.as_ref().iter().map(|v| v.is_some());
        let bitmap = MutableBitmap::from_iter(iter);

        let iter = v.as_ref().iter().map(|v| v.unwrap_or(&[]));
        let column = StringColumn::new_from_iter(iter);
        Arc::new(NullableColumn::new(Arc::new(column), bitmap.into()))
    }
}

impl_from!([bool], BooleanColumn);
impl_from!([u8], UInt8Column);
impl_from!([u16], UInt16Column);
impl_from!([u32], UInt32Column);
impl_from!([u64], UInt64Column);
impl_from!([i8], Int8Column);
impl_from!([i16], Int16Column);
impl_from!([i32], Int32Column);
impl_from!([i64], Int64Column);
impl_from!([f32], Float32Column);
impl_from!([f64], Float64Column);
impl_from!([Vec<u8>], StringColumn);
impl_from!([String], StringColumn);

impl_from_option!([Option<bool>], BooleanColumn, false);
impl_from_option!([Option<u8>], UInt8Column, 0);
impl_from_option!([Option<u16>], UInt16Column, 0);
impl_from_option!([Option<u32>], UInt32Column, 0);
impl_from_option!([Option<u64>], UInt64Column, 0);
impl_from_option!([Option<i8>], Int8Column, 0);
impl_from_option!([Option<i16>], Int16Column, 0);
impl_from_option!([Option<i32>], Int32Column, 0);
impl_from_option!([Option<i64>], Int64Column, 0);
impl_from_option!([Option<f32>], Float32Column, 0f32);
impl_from_option!([Option<f64>], Float64Column, 0f64);
