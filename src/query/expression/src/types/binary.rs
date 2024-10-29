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
use std::iter::once;
use std::ops::Range;

use databend_common_arrow::arrow::array::BinaryViewArray;
use databend_common_arrow::arrow::array::MutableBinaryViewArray;
use databend_common_arrow::arrow::trusted_len::TrustedLen;
use serde::Deserialize;
use serde::Serialize;

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
        col.memory_size()
    }

    #[inline(always)]
    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Ordering {
        lhs.cmp(rhs)
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
    pub(crate) data: BinaryViewArray,
}

impl BinaryColumn {
    pub fn new(data: BinaryViewArray) -> Self {
        BinaryColumn { data }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn current_buffer_len(&self) -> usize {
        self.data.total_bytes_len()
    }

    pub fn memory_size(&self) -> usize {
        self.data.total_buffer_len()
    }

    pub fn index(&self, index: usize) -> Option<&[u8]> {
        if index >= self.len() {
            return None;
        }

        Some(unsafe { self.index_unchecked(index) })
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    #[inline]
    pub unsafe fn index_unchecked(&self, index: usize) -> &[u8] {
        debug_assert!(index < self.data.len());
        self.data.value_unchecked(index)
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        let data = self
            .data
            .clone()
            .sliced(range.start, range.end - range.start);
        BinaryColumn { data }
    }

    pub fn iter(&self) -> BinaryIterator {
        BinaryIterator {
            col: &self.data,
            index: 0,
        }
    }

    pub fn into_inner(self) -> BinaryViewArray {
        self.data
    }
}

pub struct BinaryIterator<'a> {
    pub(crate) col: &'a BinaryViewArray,
    pub(crate) index: usize,
}

impl<'a> Iterator for BinaryIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.col.len() {
            return None;
        }
        let value = self.col.value(self.index);
        self.index += 1;
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.col.len() - self.index;
        (remaining, Some(remaining))
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
            need_estimated: false,
            data: Vec::with_capacity(data_capacity),
            offsets,
        }
    }

    pub fn from_column(col: BinaryColumn) -> Self {
        let mut builder = BinaryColumnBuilder::with_capacity(col.len(), col.current_buffer_len());
        for item in col.iter() {
            builder.put_slice(item);
            builder.commit_row();
        }
        builder
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
        self.data.reserve(other.current_buffer_len());
        for item in other.iter() {
            self.put_slice(item);
            self.commit_row();
        }
    }

    pub fn build(self) -> BinaryColumn {
        let mut bulider = MutableBinaryViewArray::with_capacity(self.len());
        for (start, end) in self
            .offsets
            .windows(2)
            .map(|w| (w[0] as usize, w[1] as usize))
        {
            bulider.push_value(&self.data[start..end]);
        }
        BinaryColumn {
            data: bulider.into(),
        }
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
