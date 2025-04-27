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
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::Range;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::Result;
use databend_common_io::geography::*;
use databend_common_io::wkb::make_point;
use databend_common_io::wkb::read_wkb_header;
pub use databend_common_io::wkb::WkbInfo;
use geozero::wkb::Ewkb;
use geozero::ToWkt;
use serde::Deserialize;
use serde::Serialize;

use super::binary::BinaryColumnIter;
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
use crate::values::ScalarRef;
use crate::ColumnBuilder;

#[derive(
    Clone,
    Default,
    Debug,
    PartialOrd,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    PartialEq,
    Eq,
)]
pub struct Geography(pub Vec<u8>);

impl Geography {
    pub fn as_ref(&self) -> GeographyRef<'_> {
        GeographyRef(self.0.as_ref())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GeographyRef<'a>(pub &'a [u8]);

impl Debug for GeographyRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Geography")
            .field_with(|f| write!(f, "0x{}", hex::encode(self.0)))
            .finish()
    }
}

impl GeographyRef<'_> {
    pub fn to_owned(&self) -> Geography {
        Geography(self.0.to_owned())
    }

    pub fn to_ewkt(&self) -> std::result::Result<String, String> {
        let info = read_wkb_header(self.0)?;
        Ewkb(self.0).to_ewkt(info.srid).map_err(|e| e.to_string())
    }

    pub fn info(&self) -> WkbInfo {
        assert!(!self.0.is_empty(), "null geography");
        read_wkb_header(self.0).unwrap()
    }
}

impl AsRef<[u8]> for GeographyRef<'_> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GeographyType;

impl GeographyType {
    pub fn check_point(lon: f64, lat: f64) -> Result<()> {
        check_point(lon, lat)
    }

    pub fn point(lon: f64, lat: f64) -> Geography {
        Geography(make_point(lon, lat))
    }
}

impl ValueType for GeographyType {
    type Scalar = Geography;
    type ScalarRef<'a> = GeographyRef<'a>;
    type Column = GeographyColumn;
    type Domain = ();
    type ColumnIterator<'a> = GeographyIterator<'a>;
    type ColumnBuilder = BinaryColumnBuilder;

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar.to_owned()
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        scalar.as_ref()
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Option<Self::ScalarRef<'a>> {
        scalar.as_geography().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        col.as_geography().cloned()
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
            ColumnBuilder::Geography(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Geography(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        _decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::Geography(builder))
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Geography(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Geography(col)
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

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        BinaryColumnBuilder::from_column(col.0)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.put_slice(item.0);
        builder.commit_row()
    }

    fn push_item_repeat(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>, n: usize) {
        builder.push_repeat(item.0, n)
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.commit_row()
    }

    fn append_column(builder: &mut Self::ColumnBuilder, other: &Self::Column) {
        builder.append_column(&other.0)
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        GeographyColumn(builder.build())
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        Geography(builder.build_scalar())
    }

    fn scalar_memory_size(scalar: &Self::ScalarRef<'_>) -> usize {
        scalar.0.len()
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        col.memory_size()
    }

    #[inline(always)]
    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Ordering {
        lhs.cmp(&rhs)
    }
}

impl ArgType for GeographyType {
    fn data_type() -> DataType {
        DataType::Geography
    }

    fn full_domain() -> Self::Domain {}

    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        BinaryColumnBuilder::with_capacity(capacity, 0)
    }
}

#[derive(Clone, PartialEq)]
pub struct GeographyColumn(pub BinaryColumn);

impl Debug for GeographyColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("GeographyColumn")
            .field_with(|f| {
                let mut f = f.debug_list();
                for x in self.iter() {
                    f.entry_with(|f| write!(f, "0x{}", hex::encode(x.0)));
                }
                f.finish()
            })
            .finish()
    }
}

impl GeographyColumn {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn memory_size(&self) -> usize {
        self.0.memory_size()
    }

    pub fn index(&self, index: usize) -> Option<GeographyRef> {
        self.0.index(index).map(GeographyRef)
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    #[inline]
    pub unsafe fn index_unchecked(&self, index: usize) -> GeographyRef<'_> {
        GeographyRef(self.0.index_unchecked(index))
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        Self(self.0.slice(range))
    }

    pub fn iter(&self) -> GeographyIterator<'_> {
        GeographyIterator {
            inner: self.0.iter(),
        }
    }

    pub fn check_valid(&self) -> Result<()> {
        Ok(self.0.check_valid()?)
    }
}

pub struct GeographyIterator<'a> {
    inner: BinaryColumnIter<'a>,
}

impl<'a> Iterator for GeographyIterator<'a> {
    type Item = GeographyRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(GeographyRef)
    }
}

unsafe impl std::iter::TrustedLen for GeographyIterator<'_> {}
