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

use std::marker::PhantomData;
use std::ops::Range;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::trusted_len::TrustedLen;

use crate::property::Domain;
use crate::property::NullableDomain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::util::bitmap_into_mut;
use crate::values::Column;
use crate::values::Scalar;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NullableType<T: ValueType>(PhantomData<T>);

impl<T: ValueType> ValueType for NullableType<T> {
    type Scalar = Option<T::Scalar>;
    type ScalarRef<'a> = Option<T::ScalarRef<'a>>;
    type Column = (T::Column, Bitmap);
    type Domain = NullableDomain<T>;
    type ColumnIterator<'a> = NullableIterator<'a, T>;
    type ColumnBuilder = (T::ColumnBuilder, MutableBitmap);

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        scalar.map(T::to_owned_scalar)
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        scalar.as_ref().map(T::to_scalar_ref)
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Null => Some(None),
            scalar => Some(Some(T::try_downcast_scalar(scalar)?)),
        }
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        match col {
            Column::Nullable { column, validity } => {
                Some((T::try_downcast_column(column)?, validity.clone()))
            }
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        match domain {
            Domain::Nullable(NullableDomain {
                has_null,
                value: Some(value),
            }) => Some(NullableDomain {
                has_null: *has_null,
                value: Some(Box::new(T::try_downcast_domain(value)?)),
            }),
            _ => None,
        }
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        match scalar {
            Some(scalar) => T::upcast_scalar(scalar),
            None => Scalar::Null,
        }
    }

    fn upcast_column((col, validity): Self::Column) -> Column {
        Column::Nullable {
            column: Box::new(T::upcast_column(col)),
            validity,
        }
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::Nullable(NullableDomain {
            has_null: domain.has_null,
            value: domain.value.map(|value| Box::new(T::upcast_domain(*value))),
        })
    }

    fn column_len<'a>((_, validity): &'a Self::Column) -> usize {
        validity.len()
    }

    fn index_column<'a>(
        (col, validity): &'a Self::Column,
        index: usize,
    ) -> Option<Self::ScalarRef<'a>> {
        let scalar = T::index_column(col, index)?;
        Some(if validity.get(index).unwrap() {
            Some(scalar)
        } else {
            None
        })
    }

    fn slice_column<'a>((col, validity): &'a Self::Column, range: Range<usize>) -> Self::Column {
        (T::slice_column(col, range), validity.clone())
    }

    fn iter_column<'a>((col, validity): &'a Self::Column) -> Self::ColumnIterator<'a> {
        NullableIterator {
            iter: T::iter_column(col),
            validity: validity.iter(),
        }
    }

    fn column_to_builder((col, validity): Self::Column) -> Self::ColumnBuilder {
        (T::column_to_builder(col), bitmap_into_mut(validity))
    }

    fn builder_len((_, validity): &Self::ColumnBuilder) -> usize {
        validity.len()
    }

    fn push_item((builder, validity): &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        match item {
            Some(scalar) => {
                T::push_item(builder, scalar);
                validity.push(true);
            }
            None => {
                T::push_default(builder);
                validity.push(false);
            }
        }
    }

    fn push_default((builder, validity): &mut Self::ColumnBuilder) {
        T::push_default(builder);
        validity.push(false);
    }

    fn append_builder(
        (builder, validity): &mut Self::ColumnBuilder,
        (other_builder, other_nulls): &Self::ColumnBuilder,
    ) {
        T::append_builder(builder, other_builder);
        validity.extend_from_slice(other_nulls.as_slice(), 0, other_nulls.len());
    }

    fn build_column((builder, validity): Self::ColumnBuilder) -> Self::Column {
        assert_eq!(T::builder_len(&builder), validity.len());
        (T::build_column(builder), validity.into())
    }

    fn build_scalar((builder, validity): Self::ColumnBuilder) -> Self::Scalar {
        assert_eq!(validity.len(), 1);
        if validity.get(0) {
            Some(T::build_scalar(builder))
        } else {
            None
        }
    }
}

impl<T: ArgType> ArgType for NullableType<T> {
    fn data_type() -> DataType {
        DataType::Nullable(Box::new(T::data_type()))
    }

    fn full_domain(generics: &GenericMap) -> Self::Domain {
        NullableDomain {
            has_null: true,
            value: Some(Box::new(T::full_domain(generics))),
        }
    }

    fn create_builder(capacity: usize, generics: &GenericMap) -> Self::ColumnBuilder {
        (
            T::create_builder(capacity, generics),
            MutableBitmap::with_capacity(capacity),
        )
    }
}

pub struct NullableIterator<'a, T: ValueType> {
    iter: T::ColumnIterator<'a>,
    validity: common_arrow::arrow::bitmap::utils::BitmapIter<'a>,
}

impl<'a, T: ValueType> Iterator for NullableIterator<'a, T> {
    type Item = Option<T::ScalarRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().zip(self.validity.next()).map(
            |(scalar, is_null)| {
                if is_null { None } else { Some(scalar) }
            },
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        assert_eq!(self.iter.size_hint(), self.validity.size_hint());
        self.validity.size_hint()
    }
}

unsafe impl<'a, T: ValueType> TrustedLen for NullableIterator<'a, T> {}
