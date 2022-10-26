// Copyright 2022 Datafuse Labs.
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

use std::iter::Iterator;

use crate::types::nullable::NullableColumn;
use crate::types::number::*;
use crate::types::*;
use crate::Column;

/// ColumnFrom is a helper trait to generate columns.
pub trait ColumnFrom<D, Phantom: ?Sized> {
    /// Initialize by name and values.
    fn from_data(_: D) -> Column;

    fn from_data_with_validity(d: D, valids: Vec<bool>) -> Column {
        let column = Self::from_data(d);
        Column::Nullable(Box::new(NullableColumn {
            column,
            validity: valids.into(),
        }))
    }
}

macro_rules! for_common_scalar_values {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            { Int8Type },
            { Int16Type },
            { Int32Type },
            { Int64Type },
            { UInt8Type },
            { UInt16Type },
            { UInt32Type },
            { UInt64Type },
            { Float32Type },
            { Float64Type },
            { BooleanType },
            { StringType }
        }
    };
}

macro_rules! impl_from_iterator {
    ([], $( { $T: ident} ),*) => {
        $(
        impl<'a, D: Iterator<Item = <$T as ValueType>::ScalarRef<'a>>>
            ColumnFrom<D, [<$T as ValueType>::Scalar; 0]> for Column
        {
            fn from_data(d: D) -> Column {
                $T::upcast_column($T::column_from_ref_iter(d.into_iter(), &[]))
            }
        }
        )*
    };
}

macro_rules! impl_from_opt_iterator {
    ([], $( { $T: ident} ),*) => {
        $(
        impl<'a, D: Iterator<Item = <NullableType<$T> as ValueType>::ScalarRef<'a>>>
            ColumnFrom<D, [<NullableType<$T> as ValueType>::Scalar; 0]> for Column
        {
            fn from_data(d: D) -> Column {
                NullableType::<$T>::upcast_column(NullableType::<$T>::column_from_ref_iter(
                    d.into_iter(),
                    &[],
                ))
            }
        }
        )*
    };
}

macro_rules! impl_from_vec {
    ([], $( { $T: ident} ),*) => {
        $(
        impl ColumnFrom<Vec<<$T as ValueType>::Scalar>, [<$T as ValueType>::Scalar; 1]> for Column {
            fn from_data(d: Vec<<$T as ValueType>::Scalar>) -> Column {
                $T::upcast_column($T::column_from_vec(d, &[]))
            }
        }
        )*
    };
}

macro_rules! impl_from_opt_vec {
    ([], $( { $T: ident} ),*) => {
        $(
        impl
            ColumnFrom<
                Vec<<NullableType<$T> as ValueType>::Scalar>,
                [<NullableType<$T> as ValueType>::Scalar; 1],
            > for Column
        {
            fn from_data(d: Vec<<NullableType<$T> as ValueType>::Scalar>) -> Column {
                NullableType::<$T>::upcast_column(NullableType::<$T>::column_from_vec(d, &[]))
            }
        }
        )*
    };
}

impl<'a, D: AsRef<[&'a str]>> ColumnFrom<D, [Vec<u8>; 2]> for Column {
    fn from_data(d: D) -> Column {
        StringType::upcast_column(StringType::column_from_ref_iter(
            d.as_ref().iter().map(|c| c.as_bytes()),
            &[],
        ))
    }
}

impl<D: AsRef<[f32]>> ColumnFrom<D, [Vec<f32>; 0]> for Column {
    fn from_data(d: D) -> Column {
        NumberType::<F32>::upcast_column(NumberType::<F32>::column_from_iter(
            d.as_ref().iter().map(|f| (*f).into()),
            &[],
        ))
    }
}

impl<D: AsRef<[f64]>> ColumnFrom<D, [Vec<f64>; 0]> for Column {
    fn from_data(d: D) -> Column {
        NumberType::<F64>::upcast_column(NumberType::<F64>::column_from_iter(
            d.as_ref().iter().map(|f| (*f).into()),
            &[],
        ))
    }
}

// Specialize for `TimestampType`, because from `Vec<i64>` will be conflict with `Int64Type`.
pub fn from_timestamp_data(d: Vec<i64>) -> Column {
    TimestampType::upcast_column(TimestampType::column_from_vec(d, &[]))
}

pub fn from_timestamp_data_with_validity(d: Vec<i64>, valids: Vec<bool>) -> Column {
    let column = from_timestamp_data(d);
    Column::Nullable(Box::new(NullableColumn {
        column,
        validity: valids.into(),
    }))
}

// Specialize for `DateType`, because from `Vec<i32>` will be conflict with `Int32Type`.
pub fn from_date_data(d: Vec<i32>) -> Column {
    DateType::upcast_column(DateType::column_from_vec(d, &[]))
}

pub fn from_date_data_with_validity(d: Vec<i32>, valids: Vec<bool>) -> Column {
    let column = from_date_data(d);
    Column::Nullable(Box::new(NullableColumn {
        column,
        validity: valids.into(),
    }))
}

for_common_scalar_values! { impl_from_iterator }
for_common_scalar_values! { impl_from_opt_iterator }
for_common_scalar_values! { impl_from_vec }
for_common_scalar_values! { impl_from_opt_vec }
