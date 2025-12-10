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

use databend_common_exception::Result;
use databend_common_io::deserialize_bitmap;
use databend_common_io::HybridBitmap;

use super::binary::BinaryColumn;
use super::binary::BinaryColumnBuilder;
use super::column_type_error;
use super::domain_type_error;
use super::scalar_type_error;
use super::AccessType;
use super::ArgType;
use super::BuilderMut;
use super::DataType;
use super::GenericMap;
use super::ReturnType;
use super::ValueType;
use crate::property::Domain;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq)]
pub struct BitmapColumn {
    raw: BinaryColumn,
    data: Vec<HybridBitmap>,
}

impl BitmapColumn {
    pub fn from_binary(raw: BinaryColumn) -> Result<Self> {
        let data = raw
            .iter()
            .map(deserialize_bitmap)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { raw, data })
    }

    pub fn from_data(data: Vec<HybridBitmap>) -> Self {
        let mut raw = BinaryColumnBuilder::with_capacity(data.len(), 0);
        for value in &data {
            value.serialize_into(&mut raw.data).unwrap();
            raw.commit_row();
        }
        Self {
            raw: raw.build(),
            data,
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn index(&self, index: usize) -> Option<&HybridBitmap> {
        self.data.get(index)
    }

    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn index_unchecked(&self, index: usize) -> &HybridBitmap {
        self.data.get_unchecked(index)
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        let raw = self.raw.slice(range.clone());
        let data = self.data[range].to_vec();
        Self { raw, data }
    }

    pub fn iter(&self) -> BitmapColumnIter<'_> {
        BitmapColumnIter {
            inner: self.data.iter(),
        }
    }

    pub fn raw(&self) -> &BinaryColumn {
        &self.raw
    }

    pub fn into_raw(self) -> BinaryColumn {
        self.raw
    }

    pub fn memory_size(&self) -> usize {
        let raw = self.raw.data().len() + self.raw.offsets().len() * 8;
        let decoded = self.data.iter().map(|b| b.memory_size()).sum::<usize>();
        raw + decoded
    }

    pub fn check_valid(&self) -> Result<()> {
        Ok(self.raw.check_valid()?)
    }

    pub fn data(&self) -> &[u8] {
        self.raw.data()
    }

    pub fn offsets(&self) -> &[u64] {
        self.raw.offsets()
    }
}

#[derive(Debug, Clone)]
pub struct BitmapColumnIter<'a> {
    inner: std::slice::Iter<'a, HybridBitmap>,
}

impl<'a> Iterator for BitmapColumnIter<'a> {
    type Item = &'a HybridBitmap;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

unsafe impl<'a> TrustedLen for BitmapColumnIter<'a> {}

#[derive(Debug, Clone)]
pub struct BitmapColumnBuilder {
    raw: BinaryColumnBuilder,
    data: Vec<HybridBitmap>,
}

impl BitmapColumnBuilder {
    pub fn with_capacity(capacity: usize, raw_capacity: usize) -> Self {
        Self {
            raw: BinaryColumnBuilder::with_capacity(capacity, raw_capacity),
            data: Vec::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn memory_size(&self) -> usize {
        let raw = self.raw.memory_size();
        let decoded = self.data.iter().map(|b| b.memory_size()).sum::<usize>();
        raw + decoded
    }

    pub fn push(&mut self, item: &HybridBitmap) {
        item.serialize_into(&mut self.raw.data).unwrap();
        self.raw.commit_row();
        self.data.push(item.clone());
    }

    pub fn push_repeat(&mut self, item: &HybridBitmap, n: usize) {
        let mut buf = Vec::new();
        item.serialize_into(&mut buf).unwrap();
        self.raw.push_repeat(&buf, n);
        self.data
            .extend(std::iter::repeat_with(|| item.clone()).take(n));
    }

    pub fn push_default(&mut self) {
        self.push(&HybridBitmap::default());
    }

    pub fn push_raw_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.raw.put_slice(bytes);
        self.raw.commit_row();
        let bitmap = deserialize_bitmap(bytes)?;
        self.data.push(bitmap);
        Ok(())
    }

    pub fn append_column(&mut self, other: &BitmapColumn) {
        self.raw.append_column(other.raw());
        self.data.extend_from_slice(&other.data);
    }

    pub fn repeat(item: &HybridBitmap, n: usize) -> Self {
        let mut builder = Self::with_capacity(n, 0);
        builder.push_repeat(item, n);
        builder
    }

    pub fn repeat_default(len: usize) -> Self {
        let mut builder = Self::with_capacity(len, 0);
        for _ in 0..len {
            builder.push_default();
        }
        builder
    }

    pub fn pop(&mut self) -> Option<HybridBitmap> {
        let _ = self.raw.pop();
        self.data.pop()
    }

    pub fn into_raw_builder(self) -> BinaryColumnBuilder {
        self.raw
    }

    pub fn build(self) -> BitmapColumn {
        BitmapColumn {
            raw: self.raw.build(),
            data: self.data,
        }
    }

    pub fn build_scalar(self) -> HybridBitmap {
        debug_assert_eq!(self.data.len(), 1);
        self.data.into_iter().next().unwrap_or_default()
    }

    pub fn from_column(col: BitmapColumn) -> Self {
        Self {
            raw: BinaryColumnBuilder::from_column(col.raw),
            data: col.data,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitmapType;

impl AccessType for BitmapType {
    type Scalar = HybridBitmap;
    type ScalarRef<'a> = &'a HybridBitmap;
    type Column = BitmapColumn;
    type Domain = ();
    type ColumnIterator<'a> = BitmapColumnIter<'a>;

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar.clone()
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        scalar
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Result<Self::ScalarRef<'a>> {
        scalar
            .as_bitmap()
            .cloned()
            .ok_or_else(|| scalar_type_error::<Self>(scalar))
    }

    fn try_downcast_column(col: &Column) -> Result<Self::Column> {
        col.as_bitmap()
            .map(|column| column.as_ref())
            .cloned()
            .ok_or_else(|| column_type_error::<Self>(col))
    }

    fn try_downcast_domain(domain: &Domain) -> Result<Self::Domain> {
        if domain.is_undefined() {
            Ok(())
        } else {
            Err(domain_type_error::<Self>(domain))
        }
    }

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.index(index)
    }

    #[inline(always)]
    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        col.index_unchecked(index)
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        col.slice(range)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        col.iter()
    }

    fn scalar_memory_size(scalar: &Self::ScalarRef<'_>) -> usize {
        scalar.memory_size()
    }

    fn column_memory_size(col: &Self::Column, _gc: bool) -> usize {
        col.memory_size()
    }

    #[inline(always)]
    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Ordering {
        lhs.iter().cmp(rhs.iter())
    }
}

impl ValueType for BitmapType {
    type ColumnBuilder = BitmapColumnBuilder;
    type ColumnBuilderMut<'a> = BuilderMut<'a, Self>;

    fn upcast_scalar_with_type(scalar: Self::Scalar, data_type: &DataType) -> Scalar {
        debug_assert!(data_type.is_bitmap());
        Scalar::Bitmap(Box::new(scalar))
    }

    fn upcast_domain_with_type(_domain: Self::Domain, data_type: &DataType) -> Domain {
        debug_assert!(data_type.is_bitmap());
        Domain::Undefined
    }

    fn upcast_column_with_type(col: Self::Column, data_type: &DataType) -> Column {
        debug_assert!(data_type.is_bitmap());
        Column::Bitmap(Box::new(col))
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Self::ColumnBuilderMut<'_> {
        builder.as_bitmap_mut().unwrap().into()
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        data_type: &DataType,
    ) -> Option<ColumnBuilder> {
        debug_assert!(data_type.is_bitmap());
        Some(ColumnBuilder::Bitmap(builder))
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        BitmapColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn builder_len_mut(builder: &Self::ColumnBuilderMut<'_>) -> usize {
        builder.len()
    }

    fn push_item_mut(builder: &mut Self::ColumnBuilderMut<'_>, item: Self::ScalarRef<'_>) {
        builder.push(item);
    }

    fn push_item_repeat_mut(
        builder: &mut Self::ColumnBuilderMut<'_>,
        item: Self::ScalarRef<'_>,
        n: usize,
    ) {
        builder.push_repeat(item, n);
    }

    fn push_default_mut(builder: &mut Self::ColumnBuilderMut<'_>) {
        builder.push_default();
    }

    fn append_column_mut(builder: &mut Self::ColumnBuilderMut<'_>, other: &Self::Column) {
        builder.append_column(other);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }
}

impl ArgType for BitmapType {
    fn data_type() -> DataType {
        DataType::Bitmap
    }

    fn full_domain() -> Self::Domain {}
}

impl ReturnType for BitmapType {
    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        BitmapColumnBuilder::with_capacity(capacity, 0)
    }
}
