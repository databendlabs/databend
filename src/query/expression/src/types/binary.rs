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

use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_arrow::arrow::trusted_len::TrustedLen;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

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
pub struct BinaryType;

impl ValueType for BinaryType {
    type Scalar = Vec<u8>;
    type ScalarRef<'a> = &'a [u8];
    type Column = BinaryColumn;
    type Domain = ();
    type ColumnIterator<'a> = BinaryIterator<'a>;
    type ColumnBuilder = BinaryColumnBuilder;

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
        scalar.as_binary().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        col.as_binary().cloned()
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
            ColumnBuilder::Binary(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Binary(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        _decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::Binary(builder))
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Binary(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Binary(col)
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
        BinaryColumnBuilder::from_column(col)
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
        col.data().len() + col.offsets().len() * 8
    }
}

impl ArgType for BinaryType {
    fn data_type() -> DataType {
        DataType::Binary
    }

    fn full_domain() -> Self::Domain {}

    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        BinaryColumnBuilder::with_capacity(capacity, 0)
    }
}

#[derive(Clone, PartialEq)]
pub struct BinaryColumn {
    pub(crate) data: Buffer<u8>,
    pub(crate) offsets: Buffer<u64>,
}

impl BinaryColumn {
    pub fn new(data: Buffer<u8>, offsets: Buffer<u64>) -> Self {
        debug_assert!({ offsets.windows(2).all(|w| w[0] <= w[1]) });

        BinaryColumn { data, offsets }
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
        let start = *self.offsets.get_unchecked(index) as usize;
        let end = *self.offsets.get_unchecked(index + 1) as usize;
        self.data.get_unchecked(start..end)
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        let offsets = self
            .offsets
            .clone()
            .sliced(range.start, range.end - range.start + 1);
        BinaryColumn {
            data: self.data.clone(),
            offsets,
        }
    }

    pub fn iter(&self) -> BinaryIterator {
        BinaryIterator {
            data: &self.data,
            offsets: self.offsets.windows(2),
        }
    }

    pub fn into_buffer(self) -> (Buffer<u8>, Buffer<u64>) {
        (self.data, self.offsets)
    }

    pub fn check_valid(&self) -> Result<()> {
        let offsets = self.offsets.as_slice();
        let len = offsets.len();
        if len < 1 {
            return Err(ErrorCode::Internal(format!(
                "BinaryColumn offsets length must be equal or greater than 1, but got {}",
                len
            )));
        }

        for i in 1..len {
            if offsets[i] < offsets[i - 1] {
                return Err(ErrorCode::Internal(format!(
                    "BinaryColumn offsets value must be equal or greater than previous value, but got {}",
                    offsets[i]
                )));
            }
        }
        Ok(())
    }
}

pub struct BinaryIterator<'a> {
    pub(crate) data: &'a [u8],
    pub(crate) offsets: std::slice::Windows<'a, u64>,
}

impl<'a> Iterator for BinaryIterator<'a> {
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

unsafe impl<'a> TrustedLen for BinaryIterator<'a> {}

unsafe impl<'a> std::iter::TrustedLen for BinaryIterator<'a> {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinaryColumnBuilder {
    // if the BinaryColumnBuilder is created with `data_capacity`, need_estimated is false
    pub need_estimated: bool,
    pub data: Vec<u8>,
    pub offsets: Vec<u64>,
}

impl BinaryColumnBuilder {
    pub fn with_capacity(len: usize, data_capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(len + 1);
        offsets.push(0);
        BinaryColumnBuilder {
            need_estimated: data_capacity == 0 && len > 0,
            data: Vec::with_capacity(data_capacity),
            offsets,
        }
    }

    pub fn from_column(col: BinaryColumn) -> Self {
        BinaryColumnBuilder {
            need_estimated: col.data.is_empty(),
            data: buffer_into_mut(col.data),
            offsets: col.offsets.to_vec(),
        }
    }

    pub fn from_data(data: Vec<u8>, offsets: Vec<u64>) -> Self {
        debug_assert!({ offsets.windows(2).all(|w| w[0] <= w[1]) });

        BinaryColumnBuilder {
            need_estimated: false,
            data,
            offsets,
        }
    }

    pub fn repeat(scalar: &[u8], n: usize) -> Self {
        let len = scalar.len();
        let data = scalar.repeat(n);
        let offsets = once(0)
            .chain((0..n).map(|i| (len * (i + 1)) as u64))
            .collect();
        BinaryColumnBuilder {
            data,
            offsets,
            need_estimated: false,
        }
    }

    pub fn repeat_default(n: usize) -> Self {
        BinaryColumnBuilder {
            data: vec![],
            offsets: vec![0; n + 1],
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

    pub fn append_column(&mut self, other: &BinaryColumn) {
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

    pub fn build(self) -> BinaryColumn {
        BinaryColumn::new(self.data.into(), self.offsets.into())
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
        debug_assert!(row + 1 < self.offsets.len());

        let start = *self.offsets.get_unchecked(row) as usize;
        let end = *self.offsets.get_unchecked(row + 1) as usize;
        self.data.get_unchecked(start..end)
    }

    pub fn push_repeat(&mut self, item: &[u8], n: usize) {
        self.data.reserve(item.len() * n);
        if self.need_estimated && self.offsets.len() - 1 < 64 {
            for _ in 0..n {
                self.data.extend_from_slice(item);
                self.commit_row();
            }
        } else {
            let start = self.data.len();
            let len = item.len();
            for _ in 0..n {
                self.data.extend_from_slice(item)
            }
            self.offsets
                .extend((1..=n).map(|i| (start + len * i) as u64));
        }
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

impl<'a> FromIterator<&'a [u8]> for BinaryColumnBuilder {
    fn from_iter<T: IntoIterator<Item = &'a [u8]>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let mut builder = BinaryColumnBuilder::with_capacity(iter.size_hint().0, 0);
        for item in iter {
            builder.put_slice(item);
            builder.commit_row();
        }
        builder
    }
}
