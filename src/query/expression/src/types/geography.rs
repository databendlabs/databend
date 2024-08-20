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
use std::hash::Hash;
use std::ops::Range;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_arrow::arrow::trusted_len::TrustedLen;
use databend_common_geo::read_wkb_header;
use databend_common_geo::wkb::make_point;
use geozero::wkb::Ewkb;
use geozero::ToWkt;
use serde::Deserialize;
use serde::Serialize;

use crate::property::Domain;
use crate::types::binary::BinaryColumn;
use crate::types::binary::BinaryColumnBuilder;
use crate::types::binary::BinaryIterator;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::DecimalSize;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::values::Column;
use crate::values::Scalar;
use crate::values::ScalarRef;
use crate::ColumnBuilder;

pub const LONGITUDE_MIN: f64 = -180.0;
pub const LONGITUDE_MAX: f64 = 180.0;
pub const LATITUDE_MIN: f64 = -90.0;
pub const LATITUDE_MAX: f64 = 90.0;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Hash)]
pub struct GeographyRef<'a>(pub &'a [u8]);

impl<'a> GeographyRef<'a> {
    pub fn to_owned(&self) -> Geography {
        Geography(self.0.to_owned())
    }

    pub fn to_ewkt(&self) -> Result<String, String> {
        let info = read_wkb_header(self.0)?;
        Ewkb(self.0).to_ewkt(info.srid).map_err(|e| e.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GeographyType;

impl GeographyType {
    pub fn check_point(lon: f64, lat: f64) -> Result<(), String> {
        if !(LONGITUDE_MIN..=LONGITUDE_MAX).contains(&lon)
            || !(LATITUDE_MIN..=LATITUDE_MAX).contains(&lat)
        {
            Err("latitude is out of range".to_string())
        } else {
            Ok(())
        }
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

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: GeographyRef<'long>) -> GeographyRef<'short> {
        long
    }

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar.to_owned()
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        scalar.as_ref()
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
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

    fn compare(a: Self::ScalarRef<'_>, b: Self::ScalarRef<'_>) -> Option<std::cmp::Ordering> {
        a.partial_cmp(&b)
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

#[derive(Debug, Clone, PartialEq)]
pub struct GeographyColumn(pub BinaryColumn);

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
        GeographyIterator(self.0.iter())
    }
}

pub struct GeographyIterator<'a>(BinaryIterator<'a>);

unsafe impl<'a> TrustedLen for GeographyIterator<'a> {}

unsafe impl<'a> std::iter::TrustedLen for GeographyIterator<'a> {}

impl<'a> Iterator for GeographyIterator<'a> {
    type Item = GeographyRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(GeographyRef)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}
