use std::fmt::Display;
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
use std::hash::Hash;
use std::io;
use std::marker::PhantomData;
use std::ops::Range;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_arrow::arrow;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_io::prelude::StatBuffer;
use geo::Geometry;
use geo::Point;
use geozero::ToWkt;
use micromarshal::Marshal;
use micromarshal::Unmarshal;
use serde::Deserialize;
use serde::Serialize;

use super::number::SimpleDomain;
use crate::arrow::buffer_into_mut;
use crate::property::Domain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::DecimalSize;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::values::Column;
use crate::values::Scalar;
use crate::values::ScalarRef;
use crate::ColumnBuilder;

pub const LATITUDE_MIN: f64 = -90.0;
pub const LATITUDE_MAX: f64 = 90.0;
pub const LONGITUDE_MIN: f64 = -180.0;
pub const LONGITUDE_MAX: f64 = 180.0;

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    PartialOrd,
    BorshSerialize,
    BorshDeserialize,
    Serialize,
    Deserialize,
)]
pub struct Geography {
    pub longitude: f64,
    pub latitude: f64,
}

impl Geography {
    pub const ITEM_SIZE: usize = 16;

    pub fn check(&self) -> Result<(), String> {
        if (LONGITUDE_MIN..=LONGITUDE_MAX).contains(&self.longitude)
            && (LATITUDE_MIN..=LATITUDE_MAX).contains(&self.latitude)
        {
            Ok(())
        } else {
            Err("geography is out of range".to_string())
        }
    }

    pub fn to_point(&self) -> Point {
        (self.longitude, self.latitude).into()
    }
}

impl Hash for Geography {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.longitude.to_bits().hash(state);
        self.latitude.to_bits().hash(state);
    }
}

impl Default for Geography {
    fn default() -> Self {
        Self {
            longitude: 0.0,
            latitude: 0.0,
        }
    }
}

impl Eq for Geography {}

impl StatBuffer for Geography {
    type Buffer = [u8; Geography::ITEM_SIZE];

    fn buffer() -> Self::Buffer {
        [0; Geography::ITEM_SIZE]
    }
}

impl Marshal for Geography {
    fn marshal(&self, scratch: &mut [u8]) {
        self.longitude.marshal(scratch);
        self.latitude.marshal(&mut scratch[4..]);
    }
}

impl Unmarshal<Geography> for Geography {
    fn unmarshal(scratch: &[u8]) -> Geography {
        Self::try_unmarshal(scratch).unwrap()
    }

    fn try_unmarshal(scratch: &[u8]) -> io::Result<Geography> {
        if scratch.len() == Self::ITEM_SIZE {
            Ok(Geography {
                longitude: f64::unmarshal(&scratch[0..4]),
                latitude: f64::unmarshal(&scratch[4..8]),
            })
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "can't unmarshal Geography",
            ))
        }
    }
}

impl Display for Geography {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = Geometry::Point(self.to_point()).to_wkt().unwrap();
        write!(f, "{s}")
    }
}

impl FixSize for Geography {
    const SIZE: usize = Geography::ITEM_SIZE;
}

impl TryFrom<Point> for Geography {
    type Error = String;

    fn try_from(p: Point) -> Result<Self, Self::Error> {
        let geog = Geography {
            longitude: p.0.x,
            latitude: p.0.y,
        };
        geog.check()?;
        Ok(geog)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GeographyType;

impl ValueType for GeographyType {
    type Scalar = Geography;
    type ScalarRef<'a> = Geography;
    type Column = GeographyColumn;
    type Domain = SimpleDomain<Geography>;
    type ColumnIterator<'a> = FixSizeIterator<'a, Geography>;
    type ColumnBuilder = Vec<u8>;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: Geography) -> Geography {
        long
    }

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        scalar.as_geography().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        col.as_geography().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_geography().cloned()
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
        buffer_into_mut(col.data)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len() / Geography::ITEM_SIZE
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        let mut scratch = Geography::buffer();

        item.marshal(&mut scratch);
        builder.extend_from_slice(&scratch)
    }

    fn push_item_repeat(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>, n: usize) {
        let mut scratch = [0u8; Geography::ITEM_SIZE];
        item.marshal(&mut scratch);
        for _ in 0..n {
            builder.extend_from_slice(&scratch)
        }
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        Self::push_item(builder, Geography::default())
    }

    fn append_column(builder: &mut Self::ColumnBuilder, other: &Self::Column) {
        builder.extend_from_slice(&other.data);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        GeographyColumn::new(builder.into())
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        assert_eq!(builder.len(), Geography::ITEM_SIZE);
        Geography::unmarshal(&builder[0..Geography::ITEM_SIZE])
    }

    fn scalar_memory_size(_: &Self::ScalarRef<'_>) -> usize {
        Geography::ITEM_SIZE
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        col.len() * Geography::ITEM_SIZE
    }
}

impl ArgType for GeographyType {
    fn data_type() -> DataType {
        DataType::Geography
    }

    fn full_domain() -> Self::Domain {
        SimpleDomain::<Geography> {
            min: Geography {
                longitude: LONGITUDE_MIN,
                latitude: LATITUDE_MIN,
            },
            max: Geography {
                longitude: LONGITUDE_MAX,
                latitude: LATITUDE_MAX,
            },
        }
    }

    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        Vec::with_capacity(capacity * Geography::ITEM_SIZE)
    }
}

impl GeographyType {
    pub fn builder_pop(
        builder: &mut <Self as ValueType>::ColumnBuilder,
    ) -> Option<<Self as ValueType>::Scalar> {
        let length = builder.len();
        if length < Geography::ITEM_SIZE {
            return None;
        }

        let scratch = &builder[length - Geography::ITEM_SIZE..length];
        let scalar = Geography::unmarshal(scratch);
        builder.resize(length - Geography::ITEM_SIZE, 0);
        Some(scalar)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GeographyColumn {
    pub(crate) data: Buffer<u8>,
}

impl GeographyColumn {
    const ITEM_SIZE: usize = Geography::ITEM_SIZE;

    pub fn new(data: Buffer<u8>) -> Self {
        debug_assert!(data.len() % Self::ITEM_SIZE == 0);

        GeographyColumn { data }
    }

    pub fn len(&self) -> usize {
        self.data.len() / Self::ITEM_SIZE
    }

    pub fn data(&self) -> &Buffer<u8> {
        &self.data
    }

    pub fn memory_size(&self) -> usize {
        self.data.len()
    }

    pub fn index(&self, index: usize) -> Option<Geography> {
        if index * Self::ITEM_SIZE < self.data.len() {
            let start = index * Self::ITEM_SIZE;
            let end = start + Self::ITEM_SIZE;
            Some(Geography::unmarshal(&self.data[start..end]))
        } else {
            None
        }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    #[inline]
    pub unsafe fn index_unchecked(&self, index: usize) -> Geography {
        Geography::unmarshal(self.index_unchecked_bytes(index))
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    #[inline]
    pub unsafe fn index_unchecked_bytes(&self, index: usize) -> &[u8] {
        let start = index * Self::ITEM_SIZE;
        let end = start + Self::ITEM_SIZE;
        self.data.get_unchecked(start..end)
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        GeographyColumn {
            data: self.data.clone().sliced(
                range.start * Self::ITEM_SIZE,
                (range.end - range.start) * Self::ITEM_SIZE,
            ),
        }
    }

    pub fn iter(&self) -> FixSizeIterator<'_, Geography> {
        FixSizeIterator {
            data: &self.data,
            _t: PhantomData,
        }
    }

    pub fn into_buffer(self) -> Buffer<u8> {
        self.data
    }

    // pub fn check_valid(&self) -> Result<()> {
    //     let offsets = self.offsets.as_slice();
    //     let len = offsets.len();
    //     if len < 1 {
    //         return Err(ErrorCode::Internal(format!(
    //             "BinaryColumn offsets length must be equal or greater than 1, but got {}",
    //             len
    //         )));
    //     }

    //     for i in 1..len {
    //         if offsets[i] < offsets[i - 1] {
    //             return Err(ErrorCode::Internal(format!(
    //                 "BinaryColumn offsets value must be equal or greater than previous value, but got {}",
    //                 offsets[i]
    //             )));
    //         }
    //     }
    //     Ok(())
    // }
}

pub trait FixSize: Unmarshal<Self> + StatBuffer + Sized {
    const SIZE: usize;
}

pub struct FixSizeIterator<'a, T>
where T: FixSize
{
    data: &'a [u8],
    _t: PhantomData<T>,
}

impl<'a, T> Iterator for FixSizeIterator<'a, T>
where T: FixSize
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let length = self.data.len();
        if length == 0 {
            return None;
        }

        let scratch = &self.data[0..Self::Item::SIZE];
        let scalar = Self::Item::unmarshal(scratch);
        self.data = &self.data[Self::Item::SIZE..];

        Some(scalar)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.data.len() / Self::Item::SIZE;
        (size, Some(size))
    }
}

unsafe impl<'a, T: FixSize> arrow::trusted_len::TrustedLen for FixSizeIterator<'a, T> {}

unsafe impl<'a, T: FixSize> std::iter::TrustedLen for FixSizeIterator<'a, T> {}
