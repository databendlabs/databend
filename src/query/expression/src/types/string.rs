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

use databend_common_arrow::arrow::array::MutableBinaryViewArray;
use databend_common_arrow::arrow::array::Utf8ViewArray;
use databend_common_arrow::arrow::trusted_len::TrustedLen;
use databend_common_base::slice_ext::GetSaferUnchecked;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::binary::BinaryColumn;
use super::binary::BinaryColumnBuilder;
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
        builder.put_and_commit(item);
    }

    fn push_item_repeat(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>, n: usize) {
        builder.push_repeat(item, n);
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.put_and_commit("");
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
    fn compare(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> Ordering {
        left.cmp(right)
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
        StringColumnBuilder::with_capacity(capacity)
    }
}

#[derive(Clone)]
pub struct StringColumn {
    pub(crate) data: Utf8ViewArray,
}

impl StringColumn {
    pub fn new(data: Utf8ViewArray) -> Self {
        Self { data }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn current_buffer_len(&self) -> usize {
        self.data.total_bytes_len()
    }

    pub fn memory_size(&self) -> usize {
        self.data.total_buffer_len() + self.len() * 12
    }

    pub fn index(&self, index: usize) -> Option<&str> {
        if index >= self.len() {
            return None;
        }

        Some(unsafe { self.index_unchecked(index) })
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    #[inline]
    pub unsafe fn index_unchecked(&self, index: usize) -> &str {
        debug_assert!(index < self.data.len());

        self.data.value_unchecked(index)
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    #[inline]
    pub unsafe fn index_unchecked_bytes(&self, index: usize) -> &[u8] {
        debug_assert!(index < self.data.len());

        self.data.value_unchecked(index).as_bytes()
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        let data = self
            .data
            .clone()
            .sliced(range.start, range.end - range.start);
        Self { data }
    }

    pub fn iter(&self) -> StringIterator {
        StringIterator {
            col: self,
            index: 0,
        }
    }

    pub fn into_inner(self) -> Utf8ViewArray {
        self.data
    }

    pub fn try_from_binary(col: BinaryColumn) -> Result<Self> {
        let builder = StringColumnBuilder::try_from_bin_column(col)?;
        Ok(builder.build())
    }

    pub fn compare(col_i: &Self, i: usize, col_j: &Self, j: usize) -> Ordering {
        let view_i = unsafe { col_i.data.views().as_slice().get_unchecked_release(i) };
        let view_j = unsafe { col_j.data.views().as_slice().get_unchecked_release(j) };

        if view_i.prefix == view_j.prefix {
            unsafe {
                let value_i = col_i.data.value_unchecked(i);
                let value_j = col_j.data.value_unchecked(j);
                value_i.cmp(value_j)
            }
        } else {
            view_i
                .prefix
                .to_le_bytes()
                .cmp(&view_j.prefix.to_le_bytes())
        }
    }

    pub fn compare_str(col: &Self, i: usize, value: &str) -> Ordering {
        let view = unsafe { col.data.views().as_slice().get_unchecked_release(i) };
        let prefix = load_prefix(value.as_bytes());

        match view.prefix.to_le_bytes().as_slice().cmp(prefix) {
            Ordering::Equal => unsafe {
                let value_i = col.data.value_unchecked(i);
                value_i.cmp(value)
            },
            non_eq => non_eq,
        }
    }
}

// Loads (up to) the first 4 bytes of s as little-endian, padded with zeros.
#[inline]
fn load_prefix(s: &[u8]) -> &[u8] {
    &s[..s.len().min(4)]
}

impl PartialEq for StringColumn {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for StringColumn {}

impl PartialOrd for StringColumn {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StringColumn {
    fn cmp(&self, other: &Self) -> Ordering {
        for i in 0..self.len().max(other.len()) {
            match (self.data.views().get(i), other.data.views().get(i)) {
                (Some(left), Some(right)) => {
                    match left.prefix.to_le_bytes().cmp(&right.prefix.to_le_bytes()) {
                        Ordering::Equal => unsafe {
                            let left = self.data.value_unchecked(i);
                            let right = other.data.value_unchecked(i);
                            match left.cmp(right) {
                                Ordering::Equal => continue,
                                non_eq => return non_eq,
                            }
                        },
                        non_eq => return non_eq,
                    }
                }
                (Some(_), None) => return Ordering::Greater,
                (None, Some(_)) => return Ordering::Less,
                (None, None) => return Ordering::Equal,
            }
        }

        Ordering::Equal
    }
}

impl TryFrom<BinaryColumn> for StringColumn {
    type Error = ErrorCode;

    fn try_from(col: BinaryColumn) -> Result<StringColumn> {
        StringColumn::try_from_binary(col)
    }
}

impl From<StringColumn> for BinaryColumn {
    fn from(col: StringColumn) -> BinaryColumn {
        BinaryColumnBuilder::from_iter(col.iter().map(|x| x.as_bytes())).build()
    }
}

pub struct StringIterator<'a> {
    col: &'a StringColumn,
    index: usize,
}

impl<'a> Iterator for StringIterator<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.col.len() {
            return None;
        }
        let value = self.col.index(self.index)?;
        self.index += 1;
        Some(value)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.col.len() - self.index;
        (remaining, Some(remaining))
    }
}

unsafe impl<'a> TrustedLen for StringIterator<'a> {}

unsafe impl<'a> std::iter::TrustedLen for StringIterator<'a> {}

type MutableUtf8ViewArray = MutableBinaryViewArray<str>;

#[derive(Debug, Clone)]
pub struct StringColumnBuilder {
    pub data: MutableUtf8ViewArray,
    pub row_buffer: Vec<u8>,
}

impl StringColumnBuilder {
    pub fn with_capacity(len: usize) -> Self {
        let data = MutableUtf8ViewArray::with_capacity(len);
        StringColumnBuilder {
            data,
            row_buffer: Vec::new(),
        }
    }

    pub fn from_column(col: StringColumn) -> Self {
        let data = col.data.make_mut();
        StringColumnBuilder {
            data,
            row_buffer: Vec::new(),
        }
    }

    pub fn try_from_bin_column(col: BinaryColumn) -> Result<Self> {
        let mut data = MutableUtf8ViewArray::with_capacity(col.len());
        col.data.as_slice().check_utf8()?;
        for v in col.iter() {
            data.push_value(unsafe { std::str::from_utf8_unchecked(v) });
        }

        Ok(StringColumnBuilder {
            data,
            row_buffer: Vec::new(),
        })
    }

    pub fn repeat(scalar: &str, n: usize) -> Self {
        let mut data = MutableUtf8ViewArray::with_capacity(n);
        data.extend_constant(n, Some(scalar));
        StringColumnBuilder {
            data,
            row_buffer: Vec::new(),
        }
    }

    pub fn repeat_default(n: usize) -> Self {
        let mut data = MutableUtf8ViewArray::with_capacity(n);
        data.extend_constant(n, Some(""));
        StringColumnBuilder {
            data,
            row_buffer: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn memory_size(&self) -> usize {
        self.data.total_buffer_len
    }

    pub fn put_char(&mut self, item: char) {
        match item.len_utf8() {
            1 => self.row_buffer.push(item as u8),
            _ => self
                .row_buffer
                .extend_from_slice(item.encode_utf8(&mut [0; 4]).as_bytes()),
        }
    }

    #[inline]
    pub fn put_str(&mut self, item: &str) {
        self.row_buffer.extend_from_slice(item.as_bytes());
    }

    #[inline]
    pub fn put_and_commit<V: AsRef<str>>(&mut self, item: V) {
        self.data.push_value_ignore_validity(item);
    }

    #[inline]
    pub fn put_slice(&mut self, item: &[u8]) {
        self.row_buffer.extend_from_slice(item);
    }

    pub fn put_char_iter(&mut self, iter: impl Iterator<Item = char>) {
        for c in iter {
            self.put_char(c);
        }
    }

    #[inline]
    pub fn commit_row(&mut self) {
        debug_assert!(std::str::from_utf8(&self.row_buffer).is_ok());
        let str = unsafe { std::str::from_utf8_unchecked(&self.row_buffer) };
        self.data.push_value(str);
        self.row_buffer.clear();
    }

    pub fn append_column(&mut self, other: &StringColumn) {
        self.data.extend_values(other.iter());
    }

    pub fn build(self) -> StringColumn {
        StringColumn {
            data: self.data.into(),
        }
    }

    pub fn build_scalar(self) -> String {
        assert_eq!(self.len(), 1);

        self.data.values()[0].to_string()
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, row: usize) -> &str {
        self.data.value_unchecked(row)
    }

    pub fn push_repeat(&mut self, item: &str, n: usize) {
        self.data.extend_constant(n, Some(item));
    }

    pub fn pop(&mut self) -> Option<String> {
        self.data.pop()
    }
}

impl<'a> FromIterator<&'a str> for StringColumnBuilder {
    fn from_iter<T: IntoIterator<Item = &'a str>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let mut builder = StringColumnBuilder::with_capacity(iter.size_hint().0);
        for item in iter {
            builder.put_and_commit(item);
        }
        builder
    }
}

impl PartialEq for StringColumnBuilder {
    fn eq(&self, other: &Self) -> bool {
        self.data.values_iter().eq(other.data.values_iter())
    }
}

impl Eq for StringColumnBuilder {}

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
        for bytes in self.iter() {
            bytes.check_utf8()?;
        }
        Ok(())
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
