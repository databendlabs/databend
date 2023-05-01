// Copyright 2021 Datafuse Labs
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

use ethnum::i256;

use crate::types::decimal::*;
use crate::types::nullable::NullableColumn;
use crate::types::number::*;
use crate::types::*;
use crate::Column;

pub trait FromData<D, Phantom: ?Sized> {
    fn from_data(_: D) -> Column;

    fn from_data_with_validity(d: D, valids: Vec<bool>) -> Column {
        let column = Self::from_data(d);
        Column::Nullable(Box::new(NullableColumn {
            column,
            validity: valids.into(),
        }))
    }
}

pub trait FromOptData<D, Phantom: ?Sized> {
    fn from_opt_data(_: D) -> Column;
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
            { StringType },
            { DateType },
            { TimestampType },
            { VariantType },
            { BitmapType }
        }
    };
}

macro_rules! impl_from_iterator {
    ([], $( { $T: ident} ),*) => {
        $(
            impl<D: Iterator<Item = <$T as ValueType>::Scalar>> FromData<D, i8> for $T
            {
                fn from_data(d: D) -> Column {
                    $T::upcast_column($T::column_from_iter(d, &[]))
                }
            }
        )*
    };
}

macro_rules! impl_from_data {
    ([], $( { $T: ident} ),*) => {
        $(
            impl FromData<Vec<<$T as ValueType>::Scalar>, i16> for $T
            {
                fn from_data(d: Vec<<$T as ValueType>::Scalar>) -> Column {
                    $T::upcast_column($T::column_from_vec(d, &[]))
                }
            }
        )*
    };
}

macro_rules! impl_from_opt_data {
    ([], $( { $T: ident} ),*) => {
        $(
            impl FromOptData<Vec<Option<<$T as ValueType>::Scalar>>, i8> for $T
            {
                fn from_opt_data(d: Vec<Option<<$T as ValueType>::Scalar>>) -> Column {
                    type NT = NullableType::<$T>;
                    NT::upcast_column(NT::column_from_vec(d, &[]))
                }
            }
        )*
    };
}

for_common_scalar_values! { impl_from_iterator }
for_common_scalar_values! { impl_from_data }
for_common_scalar_values! { impl_from_opt_data }

impl<'a, D: AsRef<[&'a str]>> FromData<D, [Vec<u8>; 2]> for StringType {
    fn from_data(d: D) -> Column {
        StringType::upcast_column(StringType::column_from_ref_iter(
            d.as_ref().iter().map(|c| c.as_bytes()),
            &[],
        ))
    }
}

impl<'a, D: AsRef<[&'a [u8]]>> FromData<D, [Vec<u8>; 2]> for BitmapType {
    fn from_data(d: D) -> Column {
        BitmapType::upcast_column(BitmapType::column_from_ref_iter(
            d.as_ref().iter().copied(),
            &[],
        ))
    }
}

impl<D: AsRef<[f32]>> FromData<D, [Vec<f32>; 0]> for Float32Type {
    fn from_data(d: D) -> Column {
        Float32Type::upcast_column(Float32Type::column_from_iter(
            d.as_ref().iter().map(|f| (*f).into()),
            &[],
        ))
    }
}

impl<D: AsRef<[f64]>> FromData<D, [Vec<f64>; 0]> for Float64Type {
    fn from_data(d: D) -> Column {
        Float64Type::upcast_column(Float64Type::column_from_iter(
            d.as_ref().iter().map(|f| (*f).into()),
            &[],
        ))
    }
}

impl<D: AsRef<[i128]>> FromData<D, [Vec<i128>; 0]> for Decimal128Type {
    fn from_data(d: D) -> Column {
        Decimal128Type::upcast_column(Decimal128Type::column_from_iter(
            d.as_ref().iter().copied(),
            &[],
        ))
    }
}

impl<D: AsRef<[i256]>> FromData<D, [Vec<i256>; 0]> for Decimal256Type {
    fn from_data(d: D) -> Column {
        Decimal256Type::upcast_column(Decimal256Type::column_from_iter(
            d.as_ref().iter().copied(),
            &[],
        ))
    }
}

#[cfg(test)]
mod test {

    use crate::types::number::Float32Type;
    use crate::types::number::Int8Type;
    use crate::types::TimestampType;
    use crate::FromData;
    use crate::FromOptData;

    #[test]
    fn test() {
        let a = Int8Type::from_data(vec![1, 2, 3]);
        let b = Int8Type::from_data(vec![1, 2, 3].into_iter());
        assert!(a == b);

        let a = TimestampType::from_data(vec![1, 2, 3]);
        let b = TimestampType::from_data(vec![1, 2, 3].into_iter());
        assert!(a == b);

        let a = Float32Type::from_data(vec![1.0f32, 2.0, 3.0]);
        let b = Float32Type::from_data(vec![1.0f32, 2.0, 3.0].into_iter());
        assert!(a == b);

        let a = TimestampType::from_opt_data(vec![Some(1), None, Some(3)]);
        let b = TimestampType::from_opt_data(vec![Some(1), None, Some(3)]);
        assert!(a == b);
    }
}
