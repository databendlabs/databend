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

use std::ops::Range;

use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::trusted_len::TrustedLen;
use serde::Deserialize;
use serde::Serialize;

use crate::property::Domain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::util::buffer_into_mut;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampType;

impl ValueType for TimestampType {
    type Scalar = Timestamp;
    type ScalarRef<'a> = Timestamp;
    type Column = TimestampColumn;
    type Domain = TimestampDomain;
    type ColumnIterator<'a> = TimestampIterator<'a>;
    type ColumnBuilder = TimestampColumnBuilder;

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
            Column::Timestamp(column) => Some(column.clone()),
            _ => None,
        }
    }

    fn try_downcast_builder<'a>(
        builder: &'a mut ColumnBuilder,
    ) -> Option<&'a mut Self::ColumnBuilder> {
        match builder {
            crate::ColumnBuilder::Timestamp(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_timestamp().map(TimestampDomain::clone)
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Timestamp(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Timestamp(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::Timestamp(domain)
    }

    fn column_len<'a>(col: &'a Self::Column) -> usize {
        col.len()
    }

    fn index_column<'a>(col: &'a Self::Column, index: usize) -> Option<Self::ScalarRef<'a>> {
        col.index(index)
    }

    unsafe fn index_column_unchecked<'a>(
        col: &'a Self::Column,
        index: usize,
    ) -> Self::ScalarRef<'a> {
        col.index_unchecked(index)
    }

    fn slice_column<'a>(col: &'a Self::Column, range: Range<usize>) -> Self::Column {
        col.slice(range)
    }

    fn iter_column<'a>(col: &'a Self::Column) -> Self::ColumnIterator<'a> {
        col.iter()
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        TimestampColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.push(item);
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push_default();
    }

    fn append_builder(builder: &mut Self::ColumnBuilder, other_builder: &Self::ColumnBuilder) {
        builder.append(other_builder);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }
}

impl ArgType for TimestampType {
    fn data_type() -> DataType {
        DataType::Timestamp
    }

    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        TimestampColumnBuilder::with_capacity(capacity)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Timestamp {
    /// Milliseconds since UNIX epoch.
    pub ts: i64,
    /// Fractional digits to display.
    pub precision: u8,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TimestampColumn {
    pub ts: Buffer<i64>,
    pub precision: u8,
}

impl TimestampColumn {
    pub fn len(&self) -> usize {
        self.ts.len()
    }

    pub fn index(&self, index: usize) -> Option<Timestamp> {
        Some(Timestamp {
            ts: self.ts.get(index).cloned()?,
            precision: self.precision,
        })
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, index: usize) -> Timestamp {
        Timestamp {
            ts: *self.ts.get_unchecked(index),
            precision: self.precision,
        }
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        TimestampColumn {
            ts: self.ts.clone().slice(range.start, range.end - range.start),
            precision: self.precision,
        }
    }

    pub fn iter(&self) -> TimestampIterator {
        TimestampIterator {
            ts: self.ts.iter(),
            precision: self.precision,
        }
    }
}

pub struct TimestampIterator<'a> {
    ts: std::slice::Iter<'a, i64>,
    precision: u8,
}

impl<'a> Iterator for TimestampIterator<'a> {
    type Item = Timestamp;

    fn next(&mut self) -> Option<Self::Item> {
        self.ts.next().cloned().map(|ts| Timestamp {
            ts,
            precision: self.precision,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.ts.size_hint()
    }
}

unsafe impl<'a> TrustedLen for TimestampIterator<'a> {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampColumnBuilder {
    pub ts: Vec<i64>,
    pub precision: u8,
}

impl TimestampColumnBuilder {
    pub fn with_capacity(len: usize) -> Self {
        let ts = Vec::with_capacity(len);
        TimestampColumnBuilder { ts, precision: 0 }
    }

    pub fn from_column(col: TimestampColumn) -> Self {
        TimestampColumnBuilder {
            ts: buffer_into_mut(col.ts),
            precision: col.precision,
        }
    }

    pub fn repeat(scalar: Timestamp, n: usize) -> Self {
        TimestampColumnBuilder {
            ts: vec![scalar.ts; n],
            precision: scalar.precision,
        }
    }

    pub fn len(&self) -> usize {
        self.ts.len()
    }

    pub fn push(&mut self, scalar: Timestamp) {
        self.ts.push(scalar.ts);
        self.precision = self.precision.max(scalar.precision);
    }

    pub fn push_default(&mut self) {
        self.ts.push(0);
    }

    pub fn append(&mut self, other: &Self) {
        self.ts.extend_from_slice(&other.ts);
        self.precision = self.precision.max(other.precision);
    }

    pub fn build(self) -> TimestampColumn {
        TimestampColumn {
            ts: self.ts.into(),
            precision: self.precision,
        }
    }

    pub fn build_scalar(self) -> Timestamp {
        assert_eq!(self.ts.len(), 1);
        Timestamp {
            ts: self.ts[0],
            precision: self.precision,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampDomain {
    pub min: i64,
    pub max: i64,
    pub precision: u8,
}
