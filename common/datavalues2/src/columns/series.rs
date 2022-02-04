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

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::compute::concatenate;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;
use crate::Column;
use crate::ColumnRef;
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
                "downcast column error, column type: {:?}, expected column: {:?}",
                column.data_type(),
                std::any::type_name::<T>(),
            ))
        });
        arr
    }

    pub fn check_get_mutable_column<T: 'static + MutableColumn>(
        column: &mut dyn MutableColumn,
    ) -> Result<&mut T> {
        let ty = column.data_type();
        let arr = column.as_mut_any().downcast_mut().ok_or_else(|| {
            ErrorCode::UnknownColumn(format!(
                "downcast column error, column type: {:?}, expected column: {:?}",
                ty,
                std::any::type_name::<T>(),
            ))
        });
        arr
    }

    pub fn check_get_scalar_column<T: Scalar>(
        column: &ColumnRef,
    ) -> Result<&<T as Scalar>::ColumnType> {
        let arr = column
            .as_any()
            .downcast_ref::<<T as Scalar>::ColumnType>()
            .ok_or_else(|| {
                ErrorCode::UnknownColumn(format!(
                    "downcast column error, column type: {:?}, expected type: {:?}",
                    column.data_type_id(),
                    std::any::type_name::<<T as Scalar>::ColumnType>(),
                ))
            });
        arr
    }

    pub fn remove_nullable(column: &ColumnRef) -> ColumnRef {
        if let Ok(column) = Self::check_get::<NullableColumn>(column) {
            column.inner().clone()
        } else {
            column.clone()
        }
    }

    pub fn concat(columns: &[ColumnRef]) -> Result<ColumnRef> {
        debug_assert!(!columns.is_empty());
        let is_nullable = columns[0].is_nullable();
        let arrays = columns
            .iter()
            .map(|c| c.as_arrow_array())
            .collect::<Vec<_>>();

        let arrays = arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
        let array: ArrayRef = Arc::from(concatenate::concatenate(&arrays)?);
        Ok(match is_nullable {
            true => array.into_nullable_column(),
            false => array.into_column(),
        })
    }
}
pub trait SeriesFrom<T, Phantom: ?Sized> {
    /// Initialize by name and values.
    fn from_data(_: T) -> ColumnRef;
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
        let column = StringColumn::new_from_slice(v.as_ref().iter());
        Arc::new(column)
    }
}

impl<'a, T: AsRef<[Option<&'a [u8]>]>> SeriesFrom<T, [Option<&'a [u8]>]> for Series {
    fn from_data(v: T) -> ColumnRef {
        let iter = v.as_ref().iter();
        let capacity = get_iter_capacity(&iter);
        let mut builder = MutableStringColumn::with_capacity(capacity);
        let mut bitmap = MutableBitmap::with_capacity(capacity);

        for item in iter {
            match item {
                Some(v) => {
                    bitmap.push(true);
                    builder.append_value(v);
                }
                None => {
                    bitmap.push(false);
                    builder.push("".as_bytes());
                }
            }
        }
        let column = builder.finish();
        Arc::new(NullableColumn::new(Arc::new(column), bitmap.into()))
    }
}

macro_rules! impl_from {
    ([], $( { $S: ident} ),*) => {
        $(
                impl<T: AsRef<[$S]>> SeriesFrom<T, [$S]> for Series {
                    fn from_data(v: T) -> ColumnRef {
                        Arc::new( <$S as Scalar>::ColumnType::from_slice(v.as_ref()))
                    }
                }
         )*
     }
}

for_all_primitive_boolean_types! { impl_from }

impl<T: AsRef<[Vec<u8>]>> SeriesFrom<T, [Vec<u8>]> for Series {
    fn from_data(v: T) -> ColumnRef {
        Arc::new(StringColumn::new_from_slice(v.as_ref()))
    }
}

impl<T: AsRef<[String]>> SeriesFrom<T, [String]> for Series {
    fn from_data(v: T) -> ColumnRef {
        Arc::new(StringColumn::new_from_slice(v.as_ref()))
    }
}

macro_rules! impl_from_option {
    ([], $( { $S: ident} ),*) => {
        $(
                impl<T: AsRef<[Option<$S>]>> SeriesFrom<T, [Option<$S>]> for Series {
                    fn from_data(v: T) -> ColumnRef {
                        let iter = v.as_ref().iter();
                        let capacity = get_iter_capacity(&iter);
                        type Builder = <<$S as Scalar>::ColumnType as ScalarColumn>::Builder;
                        let mut builder = Builder::with_capacity(capacity);
                        let mut bitmap = MutableBitmap::with_capacity(capacity);

                        for item in iter {
                            match item {
                                Some(v) => {
                                    bitmap.push(true);
                                    builder.append_value(v.as_scalar_ref());
                                }
                                None => {
                                    bitmap.push(false);
                                    builder.append_value($S::default());
                                }
                            }
                        }
                        let column = builder.finish();
                        Arc::new(NullableColumn::new(Arc::new(column), bitmap.into()))
                    }
                }
         )*
     }
}

for_all_scalar_types! { impl_from_option }
