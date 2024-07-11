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

use std::ops::Range;

use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_arrow::arrow::trusted_len::TrustedLen;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

use super::binary::BinaryColumn;
use super::binary::BinaryColumnBuilder;
use super::binary::BinaryIterator;
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
pub struct StringType;

impl ValueType for StringType {
    type Scalar = String;
    type ScalarRef<'a> = &'a str;
    type Column = StringColumn;
    type Domain = StringDomain;
    type ColumnIterator<'a> = StringIterator<'a>;
    type ColumnBuilder = StringColumnBuilder;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: &'long str) -> &'short str {
        long
    }

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar.to_string()
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        scalar.as_string().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        col.as_string().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_string().cloned()
    }

    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::String(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::String(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        _decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::String(builder))
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

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.index(index)
    }

    #[inline]
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
        StringColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.put_str(item);
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

impl ArgType for StringType {
    fn data_type() -> DataType {
        DataType::String
    }

    fn full_domain() -> Self::Domain {
        StringDomain {
            min: "".to_string(),
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
        let col = BinaryColumn::new(data, offsets);

        col.check_utf8().unwrap();

        unsafe { Self::from_binary_unchecked(col) }
    }

    /// # Safety
    /// This function is unsound iff:
    /// * the offsets are not monotonically increasing
    /// * The `data` between two consecutive `offsets` are not valid utf8
    pub unsafe fn new_unchecked(data: Buffer<u8>, offsets: Buffer<u64>) -> Self {
        let col = BinaryColumn::new(data, offsets);

        #[cfg(debug_assertions)]
        col.check_utf8().unwrap();

        unsafe { Self::from_binary_unchecked(col) }
    }

    /// # Safety
    /// This function is unsound iff:
    /// * the offsets are not monotonically increasing
    /// * The `data` between two consecutive `offsets` are not valid utf8
    pub unsafe fn from_binary_unchecked(col: BinaryColumn) -> Self {
        #[cfg(debug_assertions)]
        col.check_utf8().unwrap();

        StringColumn {
            data: col.data,
            offsets: col.offsets,
        }
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

    pub fn index(&self, index: usize) -> Option<&str> {
        if index + 1 >= self.offsets.len() {
            return None;
        }

        let bytes = &self.data[(self.offsets[index] as usize)..(self.offsets[index + 1] as usize)];

        #[cfg(debug_assertions)]
        bytes.check_utf8().unwrap();

        unsafe { Some(std::str::from_utf8_unchecked(bytes)) }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    #[inline]
    pub unsafe fn index_unchecked(&self, index: usize) -> &str {
        debug_assert!(index + 1 < self.offsets.len());

        let start = *self.offsets.get_unchecked(index) as usize;
        let end = *self.offsets.get_unchecked(index + 1) as usize;
        let bytes = &self.data.get_unchecked(start..end);

        #[cfg(debug_assertions)]
        bytes.check_utf8().unwrap();

        std::str::from_utf8_unchecked(bytes)
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    #[inline]
    pub unsafe fn index_unchecked_bytes(&self, index: usize) -> &[u8] {
        debug_assert!(index + 1 < self.offsets.len());

        let start = *self.offsets.get_unchecked(index) as usize;
        let end = *self.offsets.get_unchecked(index + 1) as usize;
        self.data.get_unchecked(start..end)
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

    pub fn iter_binary(&self) -> BinaryIterator {
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
                "StringColumn offsets length must be equal or greater than 1, but got {}",
                len
            )));
        }

        for i in 1..len {
            if offsets[i] < offsets[i - 1] {
                return Err(ErrorCode::Internal(format!(
                    "StringColumn offsets value must be equal or greater than previous value, but got {}",
                    offsets[i]
                )));
            }
        }
        Ok(())
    }
}

impl From<StringColumn> for BinaryColumn {
    fn from(col: StringColumn) -> BinaryColumn {
        BinaryColumn {
            data: col.data,
            offsets: col.offsets,
        }
    }
}

impl TryFrom<BinaryColumn> for StringColumn {
    type Error = ErrorCode;

    fn try_from(col: BinaryColumn) -> Result<StringColumn> {
        col.check_utf8()?;
        Ok(StringColumn {
            data: col.data,
            offsets: col.offsets,
        })
    }
}

pub struct StringIterator<'a> {
    data: &'a [u8],
    offsets: std::slice::Windows<'a, u64>,
}

impl<'a> Iterator for StringIterator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let bytes = self
            .offsets
            .next()
            .map(|range| &self.data[(range[0] as usize)..(range[1] as usize)])?;

        #[cfg(debug_assertions)]
        bytes.check_utf8().unwrap();

        unsafe { Some(std::str::from_utf8_unchecked(bytes)) }
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
        let builder = BinaryColumnBuilder::from_data(data, offsets);
        builder.check_utf8().unwrap();
        unsafe { StringColumnBuilder::from_binary_unchecked(builder) }
    }

    /// # Safety
    /// This function is unsound iff:
    /// * the offsets are not monotonically increasing
    /// * The `data` between two consecutive `offsets` are not valid utf8
    pub unsafe fn from_binary_unchecked(col: BinaryColumnBuilder) -> Self {
        #[cfg(debug_assertions)]
        col.check_utf8().unwrap();

        StringColumnBuilder {
            need_estimated: col.need_estimated,
            data: col.data,
            offsets: col.offsets,
        }
    }

    pub fn repeat(scalar: &str, n: usize) -> Self {
        let mut builder = StringColumnBuilder {
            data: Vec::new(), // lazy allocate
            offsets: Vec::with_capacity(n + 1),
            need_estimated: false,
        };
        builder.offsets.push(0);
        builder.push_repeat(scalar, n);
        builder
    }

    pub fn repeat_default(n: usize) -> Self {
        StringColumnBuilder {
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

    pub fn put_char(&mut self, item: char) {
        self.data
            .extend_from_slice(item.encode_utf8(&mut [0; 4]).as_bytes());
    }

    #[inline]
    #[deprecated]
    pub fn put_slice(&mut self, item: &[u8]) {
        #[cfg(debug_assertions)]
        item.check_utf8().unwrap();

        self.data.extend_from_slice(item);
    }

    #[inline]
    pub fn put_str(&mut self, item: &str) {
        self.data.extend_from_slice(item.as_bytes());
    }

    pub fn put_char_iter(&mut self, iter: impl Iterator<Item = char>) {
        for c in iter {
            let mut buf = [0; 4];
            let result = c.encode_utf8(&mut buf);
            self.data.extend_from_slice(result.as_bytes());
        }
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
        unsafe { StringColumn::new_unchecked(self.data.into(), self.offsets.into()) }
    }

    pub fn build_scalar(self) -> String {
        assert_eq!(self.offsets.len(), 2);

        let bytes = self.data[(self.offsets[0] as usize)..(self.offsets[1] as usize)].to_vec();

        #[cfg(debug_assertions)]
        bytes.check_utf8().unwrap();

        unsafe { String::from_utf8_unchecked(bytes) }
    }

    #[inline]
    pub fn may_resize(&self, add_size: usize) -> bool {
        self.data.len() + add_size > self.data.capacity()
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, row: usize) -> &str {
        debug_assert!(row + 1 < self.offsets.len());

        let start = *self.offsets.get_unchecked(row) as usize;
        let end = *self.offsets.get_unchecked(row + 1) as usize;
        let bytes = self.data.get_unchecked(start..end);

        #[cfg(debug_assertions)]
        bytes.check_utf8().unwrap();

        std::str::from_utf8_unchecked(bytes)
    }

    pub fn push_repeat(&mut self, item: &str, n: usize) {
        if self.need_estimated && self.offsets.len() - 1 < 64 {
            self.data.reserve(item.len() * n);
            for _ in 0..n {
                self.data.extend_from_slice(item.as_bytes());
                self.commit_row();
            }
        } else {
            let start = self.data.len();
            let len = item.len();
            self.data
                .extend(item.as_bytes().iter().cloned().cycle().take(len * n));
            self.offsets
                .extend((1..=n).map(|i| (start + len * i) as u64));
        }
    }

    pub fn pop(&mut self) -> Option<String> {
        if self.len() > 0 {
            let index = self.len() - 1;
            let start = unsafe { *self.offsets.get_unchecked(index) as usize };
            self.offsets.pop();
            let val = self.data.split_off(start);

            #[cfg(debug_assertions)]
            val.check_utf8().unwrap();

            Some(unsafe { String::from_utf8_unchecked(val) })
        } else {
            None
        }
    }
}

impl<'a> FromIterator<&'a str> for StringColumnBuilder {
    fn from_iter<T: IntoIterator<Item = &'a str>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let mut builder = StringColumnBuilder::with_capacity(iter.size_hint().0, 0);
        for item in iter {
            builder.put_str(item);
            builder.commit_row();
        }
        builder
    }
}

impl From<StringColumnBuilder> for BinaryColumnBuilder {
    fn from(builder: StringColumnBuilder) -> BinaryColumnBuilder {
        BinaryColumnBuilder {
            need_estimated: builder.need_estimated,
            data: builder.data,
            offsets: builder.offsets,
        }
    }
}

impl TryFrom<BinaryColumnBuilder> for StringColumnBuilder {
    type Error = ErrorCode;

    fn try_from(builder: BinaryColumnBuilder) -> Result<StringColumnBuilder> {
        builder.check_utf8()?;
        Ok(StringColumnBuilder {
            need_estimated: builder.need_estimated,
            data: builder.data,
            offsets: builder.offsets,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StringDomain {
    pub min: String,
    // max value is None for full domain
    pub max: Option<String>,
}

pub trait CheckUTF8 {
    fn check_utf8(&self) -> Result<()>;
}

impl CheckUTF8 for &[u8] {
    fn check_utf8(&self) -> Result<()> {
        simdutf8::basic::from_utf8(self).map_err(|_| {
            ErrorCode::InvalidUtf8String(format!(
                "Encountered invalid utf8 data for string type, \
                if you were reading column with string type from a table, \
                it's recommended to alter the column type to `BINARY`.\n\
                Example: `ALTER TABLE <table> MODIFY COLUMN <column> BINARY;`\n\
                Invalid utf8 data: `{}`",
                hex::encode_upper(self)
            ))
        })?;
        Ok(())
    }
}

impl CheckUTF8 for Vec<u8> {
    fn check_utf8(&self) -> Result<()> {
        self.as_slice().check_utf8()
    }
}

impl CheckUTF8 for BinaryColumn {
    fn check_utf8(&self) -> Result<()> {
        check_utf8_column(&self.offsets, &self.data)
    }
}

impl CheckUTF8 for BinaryColumnBuilder {
    fn check_utf8(&self) -> Result<()> {
        check_utf8_column(&self.offsets, &self.data)
    }
}

/// # Check if any slice of `values` between two consecutive pairs from `offsets` is invalid `utf8`
fn check_utf8_column(offsets: &[u64], data: &[u8]) -> Result<()> {
    let res: Option<()> = try {
        if offsets.len() == 1 {
            return Ok(());
        }

        if data.is_ascii() {
            return Ok(());
        }

        simdutf8::basic::from_utf8(data).ok()?;

        let last = if let Some(last) = offsets.last() {
            if *last as usize == data.len() {
                return Ok(());
            } else {
                *last as usize
            }
        } else {
            // given `l = data.len()`, this branch is hit iff either:
            // * `offsets = [0, l, l, ...]`, which was covered by `from_utf8(data)` above
            // * `offsets = [0]`, which never happens because offsets.len() == 1 is short-circuited above
            return Ok(());
        };

        // truncate to relevant offsets. Note: `=last` because last was computed skipping the first item
        // following the example: starts = [0, 5]
        let starts = unsafe { offsets.get_unchecked(..=last) };

        let mut any_invalid = false;
        for start in starts {
            let start = *start as usize;

            // Safety: `try_check_offsets_bounds` just checked for bounds
            let b = *unsafe { data.get_unchecked(start) };

            // A valid code-point iff it does not start with 0b10xxxxxx
            // Bit-magic taken from `std::str::is_char_boundary`
            if (b as i8) < -0x40 {
                any_invalid = true
            }
        }
        if any_invalid {
            None?;
        }
    };
    res.ok_or_else(|| {
        ErrorCode::InvalidUtf8String(
            "Encountered invalid utf8 data for string type, \
                if you were reading column with string type from a table, \
                it's recommended to alter the column type to `BINARY`.\n\
                Example: `ALTER TABLE <table> MODIFY COLUMN <column> BINARY;`"
                .to_string(),
        )
    })
}
