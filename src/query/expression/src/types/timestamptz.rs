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

use std::fmt::Debug;
use std::ops::Range;

use common_arrow::arrow::buffer::Buffer;
use itertools::Itertools;
use serde::Deserialize;
use serde::Serialize;

use super::number::SimpleDomain;
use crate::property::Domain;
use crate::types::ValueType;
use crate::utils::arrow::buffer_into_mut;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ScalarRef;

pub const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.6f";
/// Minimum valid timestamp `1000-01-01 00:00:00.000000`, represented by the microsecs offset from 1970-01-01.
pub const TIMESTAMP_MIN: i64 = -30610224000000000;
/// Maximum valid timestamp `9999-12-31 23:59:59.999999`, represented by the microsecs offset from 1970-01-01.
pub const TIMESTAMP_MAX: i64 = 253402300799999999;

pub const MICROS_IN_A_SEC: i64 = 1_000_000;
pub const MICROS_IN_A_MILLI: i64 = 1_000;

pub const PRECISION_MICRO: u8 = 6;
pub const PRECISION_MILLI: u8 = 3;
pub const PRECISION_SEC: u8 = 0;

/// Check if the timestamp value is valid.
#[inline]
pub fn check_timestamp(micros: i64) -> Result<i64, String> {
    if (TIMESTAMP_MIN..=TIMESTAMP_MAX).contains(&micros) {
        Ok(micros)
    } else {
        Err("timestamp is out of range".to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TimestampTzDataType {
    pub tz: Option<String>,
}

#[derive(Clone, PartialEq)]
pub struct TimestampTzColumn(pub Buffer<i64>, pub Option<String>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampTzType(pub(crate) Option<String>);

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimestampTzScalar(pub i64, pub Option<String>);

#[derive(Debug, Clone, PartialEq)]
pub struct TimestampTzDomain(pub SimpleDomain<i64>, pub Option<String>);

impl TimestampTzDomain {
    pub(crate) fn full_domain() -> SimpleDomain<i64> {
        SimpleDomain {
            min: TIMESTAMP_MIN,
            max: TIMESTAMP_MAX,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampTzColumnBuilder(pub Vec<i64>, pub Option<String>);

impl TimestampTzColumnBuilder {
    pub fn from_column(col: TimestampTzColumn) -> Self {
        TimestampTzColumnBuilder(buffer_into_mut(col.0), col.1)
    }

    pub fn repeat(scalar: TimestampTzScalar, n: usize) -> TimestampTzColumnBuilder {
        TimestampTzColumnBuilder(vec![scalar.0; n], scalar.1)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn with_capacity(ty: &TimestampTzDataType, capacity: usize) -> Self {
        TimestampTzColumnBuilder(Vec::with_capacity(capacity), ty.tz.clone())
    }

    pub fn push(&mut self, item: TimestampTzScalar) {
        if self.1 == item.1 {
            self.0.push(item.0)
        } else {
            unreachable!(
                "{}",
                format!(
                    "unable push scalar(data type: TimestampTz('{:?}')) into builder(data type: TimestampTz('{:?}'))",
                    item.1, self.1
                )
            )
        }
    }

    pub fn push_default(&mut self) {
        self.0.push(0.into())
    }

    pub fn append_column(&mut self, other: &TimestampTzColumn) {
        if self.1 == other.1 {
            self.0.extend_from_slice(other.0.as_slice())
        } else {
            unreachable!(
                "{}",
                format!(
                    "unable append column(data type: TimestampTz('{:?}')) into builder(data type: TimestampTz('{:?}'))",
                    other.1, self.1
                )
            )
        }
    }

    pub fn build(self) -> TimestampTzColumn {
        TimestampTzColumn(self.0.into(), self.1)
    }

    pub fn build_scalar(self) -> TimestampTzScalar {
        assert_eq!(self.len(), 1);

        TimestampTzScalar(self.0[0], self.1)
    }

    pub fn pop(&mut self) -> Option<TimestampTzScalar> {
        self.0
            .pop()
            .map(|num| TimestampTzScalar(num, self.1.clone()))
    }
}

impl TimestampTzScalar {
    pub fn domain(&self) -> TimestampTzDomain {
        TimestampTzDomain(
            SimpleDomain {
                min: self.0,
                max: self.0,
            },
            self.1.clone(),
        )
    }
}

impl TimestampTzColumn {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn index(&self, index: usize) -> Option<TimestampTzScalar> {
        Some(TimestampTzScalar(
            self.0.get(index).cloned()?,
            self.1.clone(),
        ))
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, index: usize) -> TimestampTzScalar {
        TimestampTzScalar(*self.0.get_unchecked(index), self.1.clone())
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        assert!(
            range.end <= self.len(),
            "range {:?} out of len {}",
            range,
            self.len()
        );

        TimestampTzColumn(
            self.0.clone().sliced(range.start, range.end - range.start),
            self.1.clone(),
        )
    }

    pub fn domain(&self) -> TimestampTzDomain {
        assert!(self.len() > 0);
        let (min, max) = self.0.iter().minmax().into_option().unwrap();
        TimestampTzDomain(
            SimpleDomain {
                min: *min,
                max: *max,
            },
            self.1.clone(),
        )
    }
}

impl PartialOrd for TimestampTzScalar {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.1 == other.1 {
            self.0.partial_cmp(&other.0)
        } else {
            None
        }
    }
}

impl PartialOrd for TimestampTzColumn {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.1 == other.1 {
            self.0.iter().partial_cmp(other.0.iter())
        } else {
            None
        }
    }
}

impl ValueType for TimestampTzType {
    type Scalar = i64;
    type ScalarRef<'a> = i64;
    type Column = Buffer<i64>;
    type Domain = SimpleDomain<i64>;
    type ColumnIterator<'a> = std::iter::Cloned<std::slice::Iter<'a, i64>>;
    type ColumnBuilder = Vec<i64>;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: i64) -> i64 {
        long
    }

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Timestamp(scalar) => Some(*scalar),
            _ => None,
        }
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        match col {
            Column::TimestampTz(column) => Some(column.0.clone()),
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Option<SimpleDomain<i64>> {
        domain.as_timestamp().map(SimpleDomain::clone)
    }

    fn try_downcast_builder<'a>(
        builder: &'a mut ColumnBuilder,
    ) -> Option<&'a mut Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::TimestampTz(builder) => Some(&mut builder.0),
            _ => None,
        }
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::TimestampTz(TimestampTzScalar(scalar, None))
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::TimestampTz(TimestampTzColumn(col, None))
    }

    fn upcast_domain(domain: SimpleDomain<i64>) -> Domain {
        Domain::TimestampTz(TimestampTzDomain(domain, None))
    }

    fn column_len<'a>(col: &'a Self::Column) -> usize {
        col.len()
    }

    fn index_column<'a>(col: &'a Self::Column, index: usize) -> Option<Self::ScalarRef<'a>> {
        col.get(index).cloned()
    }

    unsafe fn index_column_unchecked<'a>(
        col: &'a Self::Column,
        index: usize,
    ) -> Self::ScalarRef<'a> {
        *col.get_unchecked(index)
    }

    fn slice_column<'a>(col: &'a Self::Column, range: Range<usize>) -> Self::Column {
        col.clone().sliced(range.start, range.end - range.start)
    }

    fn iter_column<'a>(col: &'a Self::Column) -> Self::ColumnIterator<'a> {
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
}
