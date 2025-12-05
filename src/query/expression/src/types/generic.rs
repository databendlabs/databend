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

use std::cmp::Ordering;
use std::ops::Range;

use databend_common_exception::Result;

use super::AccessType;
use super::ArgType;
use super::BuilderMut;
use super::DataType;
use super::GenericMap;
use super::ReturnType;
use super::ValueType;
use crate::property::Domain;
use crate::values::Column;
use crate::values::ColumnBuilder;
use crate::values::ColumnIterator;
use crate::values::Scalar;
use crate::values::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenericType<const INDEX: usize>;

impl<const INDEX: usize> AccessType for GenericType<INDEX> {
    type Scalar = Scalar;
    type ScalarRef<'a> = ScalarRef<'a>;
    type Column = Column;
    type Domain = Domain;
    type ColumnIterator<'a> = ColumnIterator<'a>;

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar.to_owned()
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        scalar.as_ref()
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Result<Self::ScalarRef<'a>> {
        Ok(scalar.clone())
    }

    fn try_downcast_column(col: &Column) -> Result<Self::Column> {
        Ok(col.clone())
    }

    fn try_downcast_domain(domain: &Domain) -> Result<Self::Domain> {
        Ok(domain.clone())
    }

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.index(index)
    }

    #[inline(always)]
    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        col.index(index).unwrap()
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        col.slice(range)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        col.iter()
    }

    fn scalar_memory_size(scalar: &Self::ScalarRef<'_>) -> usize {
        scalar.memory_size()
    }

    fn column_memory_size(col: &Self::Column, gc: bool) -> usize {
        col.memory_size(gc)
    }

    #[inline(always)]
    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Ordering {
        lhs.cmp(&rhs)
    }
}

impl<const INDEX: usize> ValueType for GenericType<INDEX> {
    type ColumnBuilder = ColumnBuilder;
    type ColumnBuilderMut<'a> = BuilderMut<'a, Self>;

    fn upcast_scalar_with_type(scalar: Self::Scalar, _: &DataType) -> Scalar {
        scalar
    }

    fn upcast_domain_with_type(domain: Self::Domain, _: &DataType) -> Domain {
        domain
    }

    fn upcast_column_with_type(col: Self::Column, _: &DataType) -> Column {
        col
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Self::ColumnBuilderMut<'_> {
        builder.into()
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        _: &DataType,
    ) -> Option<ColumnBuilder> {
        Some(builder)
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        ColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn builder_len_mut(builder: &Self::ColumnBuilderMut<'_>) -> usize {
        builder.len()
    }

    fn push_item_mut(builder: &mut Self::ColumnBuilderMut<'_>, item: Self::ScalarRef<'_>) {
        builder.push(item);
    }

    fn push_item_repeat_mut(
        builder: &mut Self::ColumnBuilderMut<'_>,
        item: Self::ScalarRef<'_>,
        n: usize,
    ) {
        builder.push_repeat(&item, n)
    }

    fn push_default_mut(builder: &mut Self::ColumnBuilderMut<'_>) {
        builder.push_default();
    }

    fn append_column_mut(builder: &mut Self::ColumnBuilderMut<'_>, other: &Self::Column) {
        builder.append_column(other);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }
}

impl<const INDEX: usize> ArgType for GenericType<INDEX> {
    fn data_type() -> DataType {
        DataType::Generic(INDEX)
    }

    fn full_domain() -> Self::Domain {
        unreachable!()
    }
}

impl<const INDEX: usize> ReturnType for GenericType<INDEX> {
    fn create_builder(capacity: usize, generics: &GenericMap) -> Self::ColumnBuilder {
        ColumnBuilder::with_capacity(&generics[INDEX], capacity)
    }
}
