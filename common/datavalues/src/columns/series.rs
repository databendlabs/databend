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

    pub fn check_get_scalar<T: Scalar>(column: &ColumnRef) -> Result<&<T as Scalar>::ColumnType> {
        let arr = column
            .as_any()
            .downcast_ref::<<T as Scalar>::ColumnType>()
            .ok_or_else(|| {
                ErrorCode::UnknownColumn(format!(
                    "downcast column error, column type: {:?}, expected column: {:?}",
                    column.data_type(),
                    std::any::type_name::<T>(),
                ))
            });
        arr
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
        if column.is_nullable() {
            //constant nullable ?
            if column.is_const() {
                let col: &ConstColumn = unsafe { Self::static_cast(column) };
                let inner = Self::remove_nullable(col.inner());
                ConstColumn::new(inner, col.len()).arc()
            } else {
                let col = unsafe { Self::static_cast::<NullableColumn>(column) };
                col.inner().clone()
            }
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

macro_rules! impl_from_iterator {
    ([], $( { $S: ident} ),*) => {
        $(
                impl<'a, T: Iterator<Item = <$S as Scalar>::RefType<'a>> > SeriesFrom<T, [$S; 0]> for Series {
                    fn from_data(iter: T) -> ColumnRef {
                        Arc::new( <$S as Scalar>::ColumnType::from_iterator(iter))
                    }
                }
         )*
     }
}

macro_rules! impl_from_slices{
    ([], $( { $S: ident} ),*) => {
        $(
                impl<'a, T: AsRef<[<$S as Scalar>::RefType<'a>]> > SeriesFrom<T, [$S]> for Series {
                    fn from_data(v: T) -> ColumnRef {
                        Arc::new( <$S as Scalar>::ColumnType::from_slice(v.as_ref()))
                    }
                }
         )*
     }
}

for_all_scalar_types! { impl_from_iterator }
for_all_scalar_types! { impl_from_slices }

impl SeriesFrom<Vec<Vu8>, Vec<Vu8>> for Series {
    fn from_data(v: Vec<Vu8>) -> ColumnRef {
        let iter = v.as_slice().iter().map(|v| v.as_slice());
        StringColumn::from_iterator(iter).arc()
    }
}

impl SeriesFrom<Vec<String>, Vec<String>> for Series {
    fn from_data(v: Vec<String>) -> ColumnRef {
        let iter = v.as_slice().iter().map(|v| v.as_bytes());
        StringColumn::from_iterator(iter).arc()
    }
}

macro_rules! impl_from_option_iterator {
    ([], $( { $S: ident} ),*) => {
        $(
                impl<'a, T: Iterator<Item = Option<<$S as Scalar>::RefType<'a> >>> SeriesFrom<T, [Option<$S>; 0]> for Series {
                    fn from_data(iter: T) -> ColumnRef {
                        let capacity = get_iter_capacity(&iter);
                        type Builder = <<$S as Scalar>::ColumnType as ScalarColumn>::Builder;
                        let mut builder = Builder::with_capacity(capacity);
                        let mut bitmap = MutableBitmap::with_capacity(capacity);

                        for item in iter {
                            match item {
                                Some(v) => {
                                    bitmap.push(true);
                                    builder.push(v);
                                }
                                None => {
                                    bitmap.push(false);
                                    builder.push($S::default().as_scalar_ref());
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

macro_rules! impl_from_option_slices {
    ([], $( { $S: ident} ),*) => {
        $(
                impl<'a, T: AsRef<[Option<<$S as Scalar>::RefType<'a>>]> > SeriesFrom<T, [Option<$S>]> for Series {
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
                                    builder.push(*v);
                                }
                                None => {
                                    bitmap.push(false);
                                    builder.push($S::default().as_scalar_ref());
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

for_all_scalar_types! { impl_from_option_iterator }
for_all_scalar_types! { impl_from_option_slices }

impl<'a, T: AsRef<[Option<Vu8>]>> SeriesFrom<T, [Option<Vu8>; 2]> for Series {
    fn from_data(v: T) -> ColumnRef {
        let iter = v.as_ref().iter();
        let capacity = get_iter_capacity(&iter);
        type Builder = <<Vu8 as Scalar>::ColumnType as ScalarColumn>::Builder;
        let mut builder = Builder::with_capacity(capacity);
        let mut bitmap = MutableBitmap::with_capacity(capacity);
        for item in iter {
            match item {
                Some(v) => {
                    bitmap.push(true);
                    builder.push(v.as_scalar_ref());
                }
                None => {
                    bitmap.push(false);
                    builder.push(Vu8::default().as_scalar_ref());
                }
            }
        }
        let column = builder.finish();
        Arc::new(NullableColumn::new(Arc::new(column), bitmap.into()))
    }
}
