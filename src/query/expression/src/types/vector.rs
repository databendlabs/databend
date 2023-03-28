// Copyright 2023 Datafuse Labs.
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

use std::ops::Range;

use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::trusted_len::TrustedLen;

use crate::property::Domain;
use crate::types::array::ArrayColumn;
use crate::types::array::ArrayColumnBuilder;
use crate::types::number::F32;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::Float32Type;
use crate::types::GenericMap;
use crate::types::NumberDomain;
use crate::types::SimpleDomain;
use crate::types::ValueType;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorType;

impl ValueType for VectorType {
    type Scalar = Vec<F32>;
    type ScalarRef<'a> = Vec<F32>;
    type Column = ArrayColumn<Float32Type>;
    type Domain = SimpleDomain<F32>;
    type ColumnIterator<'a> = VectorIterator<'a>;
    type ColumnBuilder = ArrayColumnBuilder<Float32Type>;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: Vec<F32>) -> Vec<F32> {
        long
    }

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        scalar.to_vec()
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        scalar.to_vec()
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        scalar.as_vector().cloned()
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        Some(*col.as_vector()?.clone())
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        match domain {
            Domain::Array(Some(domain)) => Some(Float32Type::try_downcast_domain(domain)?),
            Domain::Array(None) => Some(Float32Type::full_domain()),
            _ => None,
        }
    }

    fn try_downcast_builder<'a>(
        _builder: &'a mut ColumnBuilder,
    ) -> Option<&'a mut Self::ColumnBuilder> {
        None
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Vector(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Vector(Box::new(col))
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::Array(Some(Box::new(Domain::Number(NumberDomain::Float32(
            domain,
        )))))
    }

    fn column_len<'a>(col: &'a Self::Column) -> usize {
        col.len()
    }

    fn index_column<'a>(col: &'a Self::Column, index: usize) -> Option<Self::ScalarRef<'a>> {
        Some(col.index(index).unwrap().as_slice().to_vec())
    }

    unsafe fn index_column_unchecked<'a>(
        col: &'a Self::Column,
        index: usize,
    ) -> Self::ScalarRef<'a> {
        col.index_unchecked(index).as_slice().to_vec()
    }

    fn slice_column<'a>(col: &'a Self::Column, range: Range<usize>) -> Self::Column {
        col.slice(range)
    }

    fn iter_column<'a>(col: &'a Self::Column) -> Self::ColumnIterator<'a> {
        VectorIterator {
            values: &col.values,
            offsets: col.offsets.windows(2),
        }
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        ArrayColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        let value = Buffer::<F32>::from(item);
        builder.push(value);
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push_default();
    }

    fn append_column(builder: &mut Self::ColumnBuilder, other_builder: &Self::Column) {
        builder.append_column(other_builder);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar().as_slice().to_vec()
    }

    fn scalar_memory_size<'a>(scalar: &Self::ScalarRef<'a>) -> usize {
        4 * scalar.len()
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        col.memory_size()
    }
}

impl ArgType for VectorType {
    fn data_type() -> DataType {
        DataType::Vector
    }

    fn full_domain() -> Self::Domain {
        Float32Type::full_domain()
    }

    fn create_builder(capacity: usize, generics: &GenericMap) -> Self::ColumnBuilder {
        ArrayColumnBuilder::<Float32Type>::with_capacity(capacity, 0, generics)
    }
}

pub struct VectorIterator<'a> {
    values: &'a Buffer<F32>,
    offsets: std::slice::Windows<'a, u64>,
}

impl<'a> Iterator for VectorIterator<'a> {
    type Item = Vec<F32>;

    fn next(&mut self) -> Option<Self::Item> {
        self.offsets.next().map(|range| {
            Float32Type::slice_column(self.values, (range[0] as usize)..(range[1] as usize))
                .as_slice()
                .to_vec()
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.offsets.size_hint()
    }
}

unsafe impl<'a> TrustedLen for VectorIterator<'a> {}
unsafe impl<'a> std::iter::TrustedLen for VectorIterator<'a> {}
