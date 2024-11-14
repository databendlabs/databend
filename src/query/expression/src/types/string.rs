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
use std::iter::TrustedLen;
use std::ops::Range;

use databend_common_base::slice_ext::GetSaferUnchecked;
use databend_common_column::binview::BinaryViewColumnBuilder;
use databend_common_column::binview::BinaryViewColumnIter;
use databend_common_column::binview::Utf8ViewColumn;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::property::Domain;
use crate::types::binary::BinaryColumn;
use crate::types::binary::BinaryColumnBuilder;
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
        col.value_unchecked(index)
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        col.clone().sliced(range.start, range.end - range.start)
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

pub type StringColumn = Utf8ViewColumn;
pub type StringIterator<'a> = BinaryViewColumnIter<'a, str>;

type Utf8ViewColumnBuilder = BinaryViewColumnBuilder<str>;

#[derive(Debug, Clone)]
pub struct StringColumnBuilder {
    pub data: Utf8ViewColumnBuilder,
    pub row_buffer: Vec<u8>,
}

impl StringColumnBuilder {
    pub fn with_capacity(len: usize) -> Self {
        let data = Utf8ViewColumnBuilder::with_capacity(len);
        StringColumnBuilder {
            data,
            row_buffer: Vec::new(),
        }
    }

    pub fn from_column(col: StringColumn) -> Self {
        let data = col.make_mut();
        StringColumnBuilder {
            data,
            row_buffer: Vec::new(),
        }
    }

    pub fn try_from_bin_column(col: BinaryColumn) -> Result<Self> {
        let data = Utf8ViewColumnBuilder::try_from_bin_column(col)?;
        Ok(StringColumnBuilder {
            data,
            row_buffer: Vec::new(),
        })
    }

    pub fn repeat(scalar: &str, n: usize) -> Self {
        let mut data = Utf8ViewColumnBuilder::with_capacity(n);
        data.extend_constant(n, scalar);
        StringColumnBuilder {
            data,
            row_buffer: Vec::new(),
        }
    }

    pub fn repeat_default(n: usize) -> Self {
        let mut data = Utf8ViewColumnBuilder::with_capacity(n);
        data.extend_constant(n, "");
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
        self.data.push_value(item);
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
        self.data.into()
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
        self.data.extend_constant(n, item);
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
        self.data.iter().eq(other.data.iter())
    }
}

impl Eq for StringColumnBuilder {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StringDomain {
    pub min: String,
    // max value is None for full domain
    pub max: Option<String>,
}
