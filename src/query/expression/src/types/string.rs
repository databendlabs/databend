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

use std::iter::once;
use std::ops::Range;

use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::trusted_len::TrustedLen;
use serde::Deserialize;
use serde::Serialize;

use super::SimpleDomain;
use crate::property::Domain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::utils::arrow::buffer_into_mut;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StringType;

impl ValueType for StringType {
    type Scalar = Vec<u8>;
    type ScalarRef<'a> = &'a [u8];
    type Column = StringColumn;
    type Domain = StringDomain;
    type ColumnIterator<'a> = StringIterator<'a>;
    type ColumnBuilder = StringColumnBuilder;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: &'long [u8]) -> &'short [u8] {
        long
    }

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        scalar.to_vec()
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        scalar.as_string().cloned()
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        col.as_string().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_string().map(StringDomain::clone)
    }

    fn try_downcast_builder<'a>(
        builder: &'a mut ColumnBuilder,
    ) -> Option<&'a mut Self::ColumnBuilder> {
        match builder {
            crate::ColumnBuilder::String(builder) => Some(builder),
            _ => None,
        }
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::String(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::String(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::String(domain)
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
        StringColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.put_slice(item);
        builder.commit_row();
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
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

    fn scalar_memory_size<'a>(scalar: &Self::ScalarRef<'a>) -> usize {
        scalar.len()
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        col.data().len() + col.offsets().len() * 8
    }
}

impl ArgType for StringType {
    fn data_type() -> DataType {
        DataType::String
    }

    fn full_domain() -> Self::Domain {
        StringDomain {
            min: vec![],
            max: None,
        }
    }

    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        StringColumnBuilder::with_capacity(capacity, 0)
    }
}

#[derive(Clone, PartialEq)]
pub struct StringColumn {
    data: Buffer<u8>,
    offsets: Buffer<u64>,
}

impl StringColumn {
    pub fn new(data: Buffer<u8>, offsets: Buffer<u64>) -> Self {
        debug_assert!({ offsets.windows(2).all(|w| w[0] <= w[1]) });
        StringColumn { data, offsets }
    }

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn data(&self) -> &Buffer<u8> {
        &self.data
    }

    pub fn offsets(&self) -> &Buffer<u64> {
        &self.offsets
    }

    pub fn memory_size(&self) -> usize {
        let offsets = self.offsets.as_slice();
        let len = offsets.len();
        len * 8 + (offsets[len - 1] - offsets[0]) as usize
    }

    pub fn index(&self, index: usize) -> Option<&[u8]> {
        if index + 1 < self.offsets.len() {
            Some(&self.data[(self.offsets[index] as usize)..(self.offsets[index + 1] as usize)])
        } else {
            None
        }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    #[inline]
    pub unsafe fn index_unchecked(&self, index: usize) -> &[u8] {
        &self.data[(self.offsets[index] as usize)..(self.offsets[index + 1] as usize)]
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        let offsets = self
            .offsets
            .clone()
            .sliced(range.start, range.end - range.start + 1);
        StringColumn {
            data: self.data.clone(),
            offsets,
        }
    }

    pub fn iter(&self) -> StringIterator {
        StringIterator {
            data: &self.data,
            offsets: self.offsets.windows(2),
        }
    }
}

pub struct StringIterator<'a> {
    data: &'a [u8],
    offsets: std::slice::Windows<'a, u64>,
}

impl<'a> Iterator for StringIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.offsets
            .next()
            .map(|range| &self.data[(range[0] as usize)..(range[1] as usize)])
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.offsets.size_hint()
    }
}

unsafe impl<'a> TrustedLen for StringIterator<'a> {}
unsafe impl<'a> std::iter::TrustedLen for StringIterator<'a> {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StringColumnBuilder {
    // if the StringColumnBuilder is created with `data_capacity`, need_estimated is false
    pub need_estimated: bool,
    pub data: Vec<u8>,
    pub offsets: Vec<u64>,
}

impl StringColumnBuilder {
    pub fn with_capacity(len: usize, data_capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(len + 1);
        offsets.push(0);
        StringColumnBuilder {
            need_estimated: data_capacity == 0 && len > 0,
            data: Vec::with_capacity(data_capacity),
            offsets,
        }
    }

    pub fn from_column(col: StringColumn) -> Self {
        StringColumnBuilder {
            need_estimated: col.data.is_empty(),
            data: buffer_into_mut(col.data),
            offsets: col.offsets.to_vec(),
        }
    }

    pub fn from_data(data: Vec<u8>, offsets: Vec<u64>) -> Self {
        StringColumnBuilder {
            need_estimated: false,
            data,
            offsets,
        }
    }

    pub fn repeat(scalar: &[u8], n: usize) -> Self {
        let len = scalar.len();
        let mut data = Vec::with_capacity(len * n);
        for _ in 0..n {
            data.extend_from_slice(scalar);
        }
        let offsets = once(0)
            .chain((0..n).map(|i| (len * (i + 1)) as u64))
            .collect();
        StringColumnBuilder {
            data,
            offsets,
            need_estimated: false,
        }
    }

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn memory_size(&self) -> usize {
        self.offsets.len() * 8 + self.data.len()
    }

    pub fn put_u8(&mut self, item: u8) {
        self.data.push(item);
    }

    pub fn put_char(&mut self, item: char) {
        self.data
            .extend_from_slice(item.encode_utf8(&mut [0; 4]).as_bytes());
    }

    #[inline]
    pub fn put_str(&mut self, item: &str) {
        self.data.extend_from_slice(item.as_bytes());
    }

    #[inline]
    pub fn put_slice(&mut self, item: &[u8]) {
        self.data.extend_from_slice(item);
    }

    pub fn put_char_iter(&mut self, iter: impl Iterator<Item = char>) {
        for c in iter {
            let mut buf = [0; 4];
            let result = c.encode_utf8(&mut buf);
            self.data.extend_from_slice(result.as_bytes());
        }
    }

    pub fn put(&mut self, item: &[u8]) {
        self.data.extend_from_slice(item);
    }

    #[inline]
    pub fn commit_row(&mut self) {
        self.offsets.push(self.data.len() as u64);

        if self.need_estimated
            && self.offsets.len() - 1 == 64
            && self.offsets.len() < self.offsets.capacity()
        {
            let bytes_per_row = self.data.len() / 64 + 1;
            let bytes_estimate = bytes_per_row * self.offsets.capacity();

            const MAX_HINT_SIZE: usize = 1_000_000_000;
            // if we are more than 10% over the capacity, we reserve more
            if bytes_estimate < MAX_HINT_SIZE
                && bytes_estimate as f64 > self.data.capacity() as f64 * 1.10f64
            {
                self.data.reserve(bytes_estimate - self.data.capacity());
            }
        }
    }

    pub fn append_column(&mut self, other: &StringColumn) {
        // the first offset of other column may not be zero
        let other_start = *other.offsets.first().unwrap();
        let other_last = *other.offsets.last().unwrap();
        let start = self.offsets.last().cloned().unwrap();
        self.data
            .extend_from_slice(&other.data[(other_start as usize)..(other_last as usize)]);
        self.offsets.extend(
            other
                .offsets
                .iter()
                .skip(1)
                .map(|offset| start + offset - other_start),
        );
    }

    pub fn build(self) -> StringColumn {
        StringColumn::new(self.data.into(), self.offsets.into())
    }

    pub fn build_scalar(self) -> Vec<u8> {
        assert_eq!(self.offsets.len(), 2);
        self.data[(self.offsets[0] as usize)..(self.offsets[1] as usize)].to_vec()
    }

    #[inline]
    pub fn may_resize(&self, add_size: usize) -> bool {
        self.data.len() + add_size > self.data.capacity()
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, row: usize) -> &[u8] {
        let start = *self.offsets.get_unchecked(row) as usize;
        let end = *self.offsets.get_unchecked(row + 1) as usize;
        // soundness: the invariant of the struct
        self.data.get_unchecked(start..end)
    }

    pub fn pop(&mut self) -> Option<Vec<u8>> {
        if self.len() > 0 {
            let index = self.len() - 1;
            let start = unsafe { *self.offsets.get_unchecked(index) as usize };
            self.offsets.pop();
            let val = self.data.split_off(start);
            Some(val)
        } else {
            None
        }
    }
}

impl<'a> FromIterator<&'a [u8]> for StringColumnBuilder {
    fn from_iter<T: IntoIterator<Item = &'a [u8]>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let mut builder = StringColumnBuilder::with_capacity(iter.size_hint().0, 0);
        for item in iter {
            builder.put_slice(item);
            builder.commit_row();
        }
        builder
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StringDomain {
    pub min: Vec<u8>,
    // max value is None for full domain
    pub max: Option<Vec<u8>>,
}

impl StringDomain {
    pub fn unify(&self, other: &Self) -> (SimpleDomain<Vec<u8>>, SimpleDomain<Vec<u8>>) {
        let mut max_size = self.min.len().max(other.min.len());
        if let Some(max) = &self.max {
            max_size = max_size.max(max.len());
        }
        if let Some(max) = &other.max {
            max_size = max_size.max(max.len());
        }

        let max_value = vec![255; max_size + 1];

        (
            SimpleDomain {
                min: self.min.clone(),
                max: self.max.clone().unwrap_or_else(|| max_value.clone()),
            },
            SimpleDomain {
                min: other.min.clone(),
                max: other.max.clone().unwrap_or_else(|| max_value.clone()),
            },
        )
    }
}
