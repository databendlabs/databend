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
use std::io::Cursor;
use std::ops::Range;

use chrono::NaiveDate;
use chrono_tz::Tz;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_io::cursor_ext::BufferReadDateTimeExt;
use databend_common_io::cursor_ext::ReadBytesExt;
use num_traits::AsPrimitive;

use super::number::SimpleDomain;
use crate::date_helper::DateConverter;
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

pub const DATE_FORMAT: &str = "%Y-%m-%d";
/// Minimum valid date, represented by the day offset from 1970-01-01.
pub const DATE_MIN: i32 = -354285;
/// Maximum valid date, represented by the day offset from 1970-01-01.
pub const DATE_MAX: i32 = 2932896;

/// Check if date is within range.
#[inline]
pub fn check_date(days: i64) -> Result<i32, String> {
    if (DATE_MIN as i64..=DATE_MAX as i64).contains(&days) {
        Ok(days as i32)
    } else {
        Err("date is out of range".to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DateType;

impl ValueType for DateType {
    type Scalar = i32;
    type ScalarRef<'a> = i32;
    type Column = Buffer<i32>;
    type Domain = SimpleDomain<i32>;
    type ColumnIterator<'a> = std::iter::Cloned<std::slice::Iter<'a, i32>>;
    type ColumnBuilder = Vec<i32>;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: i32) -> i32 {
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
            ScalarRef::Date(scalar) => Some(*scalar),
            _ => None,
        }
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        match col {
            Column::Date(column) => Some(column.clone()),
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Option<SimpleDomain<i32>> {
        domain.as_date().cloned()
    }

    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Date(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Date(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        _decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::Date(builder))
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Date(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Date(col)
    }

    fn upcast_domain(domain: SimpleDomain<i32>) -> Domain {
        Domain::Date(domain)
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
    fn greater_than_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left >= right
    }

    #[inline(always)]
    fn less_than(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left < right
    }

    #[inline(always)]
    fn less_than_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left <= right
    }
}

impl ArgType for DateType {
    fn data_type() -> DataType {
        DataType::Date
    }

    fn full_domain() -> Self::Domain {
        SimpleDomain {
            min: DATE_MIN,
            max: DATE_MAX,
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
pub fn string_to_date(
    date_str: impl AsRef<[u8]>,
    tz: Tz,
) -> databend_common_exception::Result<NaiveDate> {
    let mut reader = Cursor::new(std::str::from_utf8(date_str.as_ref()).unwrap().as_bytes());
    match reader.read_date_text(&tz) {
        Ok(d) => match reader.must_eof() {
            Ok(..) => Ok(d),
            Err(e) => Err(ErrorCode::BadArguments(format!("{}", e))),
        },
        Err(e) => Err(e),
    }
}

#[inline]
pub fn date_to_string(date: impl AsPrimitive<i64>, tz: Tz) -> impl Display {
    date.as_().to_date(tz).format(DATE_FORMAT)
}
