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

use ordered_float::OrderedFloat;

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

impl<'a, D: AsRef<[f32]>> ColumnFrom<D, [Vec<f32>; 0]> for Column {
    fn from_data(d: D) -> Column {
        NumberType::<OrderedFloat<f32>>::upcast_column(
            NumberType::<OrderedFloat<f32>>::column_from_iter(
                d.as_ref().iter().map(|f| OrderedFloat(*f)),
                &[],
            ),
        )
    }
}

impl<'a, D: AsRef<[f64]>> ColumnFrom<D, [Vec<f64>; 0]> for Column {
    fn from_data(d: D) -> Column {
        NumberType::<OrderedFloat<f64>>::upcast_column(
            NumberType::<OrderedFloat<f64>>::column_from_iter(
                d.as_ref().iter().map(|f| OrderedFloat(*f)),
                &[],
            ),
        )
    }
}

for_common_scalar_values! { impl_from_iterator }
for_common_scalar_values! { impl_from_opt_iterator }
for_common_scalar_values! { impl_from_vec }
for_common_scalar_values! { impl_from_opt_vec }
