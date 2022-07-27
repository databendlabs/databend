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

use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::trusted_len::TrustedLen;

use crate::property::Domain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::values::Column;
use crate::values::Scalar;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrayType<T: ArgType>(PhantomData<T>);

impl<T: ArgType> ValueType for ArrayType<T> {
    type Scalar = T::Column;
    type ScalarRef<'a> = T::Column;
    type Column = (T::Column, Buffer<u64>);
    type Domain = T::Domain;

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        scalar.clone()
    }
}

impl<T: ArgType> ArgType for ArrayType<T> {
    type ColumnIterator<'a> = ArrayIterator<'a, T>;
    type ColumnBuilder = (T::ColumnBuilder, Vec<u64>);

    fn data_type() -> DataType {
        DataType::Array(Box::new(T::data_type()))
    }

    fn try_downcast_scalar<'a>(scalar: &'a Scalar) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            Scalar::Array(array) => T::try_downcast_column(array),
            _ => None,
        }
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        match col {
            Column::Array { array, offsets } => {
                Some((T::try_downcast_column(array)?, offsets.clone()))
            }
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        match domain {
            Domain::Array(Some(domain)) => Some(T::try_downcast_domain(domain)?),
            _ => None,
        }
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Array(T::upcast_column(scalar))
    }

    fn upcast_column((col, offsets): Self::Column) -> Column {
        Column::Array {
            array: Box::new(T::upcast_column(col)),
            offsets,
        }
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::Array(Some(Box::new(T::upcast_domain(domain))))
    }

    fn full_domain(generics: &GenericMap) -> Self::Domain {
        T::full_domain(generics)
    }

    fn column_len<'a>((_, offsets): &'a Self::Column) -> usize {
        offsets.len()
    }

    fn index_column<'a>(
        (col, offsets): &'a Self::Column,
        index: usize,
    ) -> Option<Self::ScalarRef<'a>> {
        Some(T::slice_column(
            col,
            (offsets[index] as usize)..(offsets[index + 1] as usize),
        ))
    }

    fn slice_column<'a>((col, offsets): &'a Self::Column, range: Range<usize>) -> Self::Column {
        let offsets = offsets
            .clone()
            .slice(range.start, range.end - range.start + 1);
        (col.clone(), offsets)
    }

    fn iter_column<'a>((col, offsets): &'a Self::Column) -> Self::ColumnIterator<'a> {
        ArrayIterator {
            col,
            offsets: offsets.windows(2),
        }
    }

    fn create_builder(_capacity: usize, generics: &GenericMap) -> Self::ColumnBuilder {
        (T::create_builder(0, generics), vec![0])
    }

    fn column_to_builder((col, offsets): Self::Column) -> Self::ColumnBuilder {
        (T::column_to_builder(col), offsets.to_vec())
    }

    fn builder_len((_, offsets): &Self::ColumnBuilder) -> usize {
        offsets.len() - 1
    }

    fn push_item((builder, offsets): &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        let other_col = T::column_to_builder(item);
        T::append_builder(builder, &other_col);
        let len = T::builder_len(builder);
        offsets.push(len as u64);
    }

    fn push_default((builder, offsets): &mut Self::ColumnBuilder) {
        let len = T::builder_len(builder);
        offsets.push(len as u64);
    }

    fn append_builder(
        (builder, offsets): &mut Self::ColumnBuilder,
        (other_builder, other_offsets): &Self::ColumnBuilder,
    ) {
        let end = offsets.last().cloned().unwrap();
        offsets.extend(other_offsets.iter().skip(1).map(|offset| offset + end));
        T::append_builder(builder, other_builder);
    }

    fn build_column((builder, offsets): Self::ColumnBuilder) -> Self::Column {
        (T::build_column(builder), offsets.into())
    }

    fn build_scalar((builder, offsets): Self::ColumnBuilder) -> Self::Scalar {
        assert_eq!(offsets.len(), 2);
        T::slice_column(
            &T::build_column(builder),
            (offsets[0] as usize)..(offsets[1] as usize),
        )
    }
}

pub struct ArrayIterator<'a, T: ArgType> {
    col: &'a T::Column,
    offsets: std::slice::Windows<'a, u64>,
}

impl<'a, T: ArgType> Iterator for ArrayIterator<'a, T> {
    type Item = T::Column;

    fn next(&mut self) -> Option<Self::Item> {
        self.offsets
            .next()
            .map(|range| T::slice_column(self.col, (range[0] as usize)..(range[1] as usize)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.offsets.size_hint()
    }
}

unsafe impl<'a, T: ArgType> TrustedLen for ArrayIterator<'a, T> {}
