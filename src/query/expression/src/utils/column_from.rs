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

impl<T, S> FromData<Vec<S>, [S; 0]> for T
where for<'a> T: ArgType<Scalar = S>
{
    fn from_data(d: Vec<S>) -> Column {
        T::upcast_column(T::column_from_vec(d, &[]))
    }
}

impl<T, S, D: Iterator<Item = S>> FromData<D, [S; 1]> for T
where T: ArgType<Scalar = S>
{
    fn from_data(d: D) -> Column {
        T::upcast_column(T::column_from_iter(d, &[]))
    }
}

impl<'a, D: AsRef<[&'a str]>> FromData<D, [Vec<u8>; 2]> for StringType {
    fn from_data(d: D) -> Column {
        StringType::upcast_column(StringType::column_from_ref_iter(
            d.as_ref().iter().map(|c| c.as_bytes()),
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

#[cfg(test)]
mod test {

    use crate::types::number::Float32Type;
    use crate::types::number::Int8Type;
    use crate::types::NullableType;
    use crate::types::TimestampType;
    use crate::FromData;

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

        let a = NullableType::<TimestampType>::from_data(vec![Some(1), None, Some(3)]);
        let b = NullableType::<TimestampType>::from_data(vec![Some(1), None, Some(3)].into_iter());
        assert!(a == b);
    }
}
