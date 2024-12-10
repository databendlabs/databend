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

use std::fmt::Display;
use std::ops::Range;

use databend_common_column::fixedsizebinary::FixedSizeBinaryColumn;
use databend_common_column::fixedsizebinary::FixedSizeBinaryColumnBuilder;
use databend_common_column::fixedsizebinary::FixedSizeBinaryColumnIter;
use databend_common_io::Interval;

use crate::property::Domain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::DecimalSize;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntervalType;

impl ValueType for IntervalType {
    type Scalar = Vec<u8>;
    type ScalarRef<'a> = &'a [u8];
    type Column = FixedSizeBinaryColumn;
    type Domain = ();
    type ColumnIterator<'a> = FixedSizeBinaryColumnIter<'a>;
    type ColumnBuilder = FixedSizeBinaryColumnBuilder;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: &'long [u8]) -> &'short [u8] {
        long
    }

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar.to_vec()
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        scalar.as_interval().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        col.as_interval().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        if domain.is_undefined() {
            Some(())
        } else {
            None
        }
    }

    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Interval(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Interval(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        _decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::Interval(builder))
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Interval(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Interval(col)
    }

    fn upcast_domain(_domain: Self::Domain) -> Domain {
        Domain::Undefined
    }

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.index(index)
    }

    #[inline(always)]
    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        col.index_unchecked(index)
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        col.slice(range)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        col.iter()
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        FixedSizeBinaryColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.put_slice(item);
        builder.commit_row();
    }

    fn push_item_repeat(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>, n: usize) {
        builder.push_repeat(item, n);
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push_default();
        builder.commit_row();
    }

    fn append_column(builder: &mut Self::ColumnBuilder, other_builder: &Self::Column) {
        builder.append_column(other_builder)
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }

    fn scalar_memory_size(scalar: &Self::ScalarRef<'_>) -> usize {
        scalar.len()
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        col.memory_size()
    }
}

impl ArgType for IntervalType {
    fn data_type() -> DataType {
        DataType::Interval
    }

    fn full_domain() -> Self::Domain {}

    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        FixedSizeBinaryColumnBuilder::with_capacity(capacity, 16)
    }
}

#[inline]
pub fn string_to_interval(interval_str: &str) -> databend_common_exception::Result<Interval> {
    Interval::from_string(interval_str)
}

#[inline]
pub fn interval_to_string(interval: Interval) -> impl Display {
    interval.to_string()
}
