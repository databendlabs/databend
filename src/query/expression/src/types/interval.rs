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
use std::fmt::Display;
use std::ops::Range;

use databend_common_column::buffer::Buffer;
use databend_common_column::types::months_days_micros;
use databend_common_io::Interval;

use super::number::SimpleDomain;
use crate::property::Domain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::DecimalSize;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::utils::arrow::buffer_into_mut;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntervalType;

impl ValueType for IntervalType {
    type Scalar = months_days_micros;
    type ScalarRef<'a> = months_days_micros;
    type Column = Buffer<months_days_micros>;
    type Domain = SimpleDomain<months_days_micros>;
    type ColumnIterator<'a> = std::iter::Cloned<std::slice::Iter<'a, months_days_micros>>;
    type ColumnBuilder = Vec<months_days_micros>;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: months_days_micros) -> months_days_micros {
        long
    }

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Interval(scalar) => Some(*scalar),
            _ => None,
        }
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        match col {
            Column::Interval(column) => Some(column.clone()),
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Option<SimpleDomain<months_days_micros>> {
        domain.as_interval().cloned()
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

    fn upcast_domain(domain: SimpleDomain<months_days_micros>) -> Domain {
        Domain::Interval(domain)
    }

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.get(index).cloned()
    }

    #[inline(always)]
    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        debug_assert!(index < col.len());

        *col.get_unchecked(index)
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        col.clone().sliced(range.start, range.end - range.start)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        col.iter().cloned()
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        buffer_into_mut(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::Scalar) {
        builder.push(item);
    }

    fn push_item_repeat(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>, n: usize) {
        builder.resize(builder.len() + n, item);
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push(Self::Scalar::default());
    }

    fn append_column(builder: &mut Self::ColumnBuilder, other: &Self::Column) {
        builder.extend_from_slice(other);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.into()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        assert_eq!(builder.len(), 1);
        builder[0]
    }

    #[inline(always)]
    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Ordering {
        lhs.cmp(&rhs)
    }

    #[inline(always)]
    fn equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left == right
    }

    #[inline(always)]
    fn not_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left != right
    }

    #[inline(always)]
    fn greater_than(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left > right
    }

    #[inline(always)]
    fn less_than(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left < right
    }

    #[inline(always)]
    fn greater_than_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left >= right
    }

    #[inline(always)]
    fn less_than_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left <= right
    }
}

impl ArgType for IntervalType {
    fn data_type() -> DataType {
        DataType::Interval
    }

    fn full_domain() -> Self::Domain {
        SimpleDomain {
            min: months_days_micros::new(-12 * 200, -365 * 200, -7200000000000000000),
            max: months_days_micros::new(12 * 200, 365 * 200, 7200000000000000000),
        }
    }

    fn create_builder(capacity: usize, _generics: &GenericMap) -> Self::ColumnBuilder {
        Vec::with_capacity(capacity)
    }

    fn column_from_vec(vec: Vec<Self::Scalar>, _generics: &GenericMap) -> Self::Column {
        vec.into()
    }

    fn column_from_iter(iter: impl Iterator<Item = Self::Scalar>, _: &GenericMap) -> Self::Column {
        iter.collect()
    }

    fn column_from_ref_iter<'a>(
        iter: impl Iterator<Item = Self::ScalarRef<'a>>,
        _: &GenericMap,
    ) -> Self::Column {
        iter.collect()
    }
}

#[inline]
pub fn string_to_interval(interval_str: &str) -> databend_common_exception::Result<Interval> {
    Interval::from_string(interval_str)
}

#[inline]
pub fn interval_to_string(i: &months_days_micros) -> impl Display {
    let interval = Interval {
        months: i.months(),
        days: i.days(),
        micros: i.microseconds(),
    };
    interval.to_string()
}
