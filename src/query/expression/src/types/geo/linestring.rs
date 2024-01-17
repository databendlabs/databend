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

use std::borrow::Cow;
use std::ops::Range;

use databend_common_arrow::arrow::array::Array;
use databend_common_arrow::arrow::array::ListArray;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_arrow::arrow::datatypes::DataType as ArrowDataType;
use databend_common_arrow::arrow::offset::OffsetsBuffer;
use enum_as_inner::EnumAsInner;

use crate::types::geo::coord::CoordColumn;
use crate::types::geo::geo_trait::AsArrow;
use crate::types::geo::geo_trait::GeometryColumnAccessor;
use crate::types::geo::point::PointScalar;
use crate::types::geo::utils::coord_type_to_arrow2_type;
use crate::types::geo::utils::offset_buffer_eq;

#[derive(Clone, Debug, EnumAsInner)]
pub struct LineStringColumn {
    pub coords: CoordColumn,
    pub offsets: Buffer<u64>,
}

impl LineStringColumn {
    pub fn offsets_buffer(&self) -> OffsetsBuffer<i64> {
        let offsets: Buffer<i64> = self.offsets.iter().map(|offset| *offset as i64).collect();
        unsafe { OffsetsBuffer::new_unchecked(offsets) }
    }

    pub fn num_coords(&self, index: usize) -> usize {
        let (start, end) = self.offsets_buffer().start_end(index);
        end - start
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        Self {
            coords: self.coords.clone(),
            offsets: self
                .offsets
                .clone()
                .sliced(range.start, range.end - range.start),
        }
    }
}

impl<'a> GeometryColumnAccessor<'a> for LineStringColumn {
    type Item = LineStringScalar<'a>;

    fn len(&self) -> usize {
        self.offsets_buffer().len_proxy()
    }

    unsafe fn get_unchecked(&'a self, index: usize) -> Self::Item {
        LineStringScalar::new_borrowed(&self.coords, &self.offsets_buffer(), index)
    }
}

impl AsArrow for LineStringColumn {
    fn as_arrow(&self, arrow_type: ArrowDataType) -> Box<dyn Array> {
        Box::new(ListArray::new(
            arrow_type,
            self.offsets_buffer(),
            self.coords.as_arrow(coord_type_to_arrow2_type()),
            None,
        ))
    }
}

impl PartialEq for LineStringColumn {
    fn eq(&self, other: &Self) -> bool {
        // todo: check validity
        if !offset_buffer_eq(&self.offsets_buffer(), &other.offsets_buffer()) {
            return false;
        }

        if self.coords != other.coords {
            return false;
        }

        true
    }
}

pub struct LineStringScalar<'a> {
    pub coords: Cow<'a, CoordColumn>,
    pub geom_offsets: Cow<'a, OffsetsBuffer<i64>>,
    pub geom_index: usize,
}

impl<'a> LineStringScalar<'a> {
    pub fn new(
        coords: Cow<'a, CoordColumn>,
        geom_offsets: Cow<'a, OffsetsBuffer<i64>>,
        geom_index: usize,
    ) -> Self {
        Self {
            coords,
            geom_offsets,
            geom_index,
        }
    }

    pub fn new_borrowed(
        coords: &'a CoordColumn,
        geom_offsets: &'a OffsetsBuffer<i64>,
        geom_index: usize,
    ) -> Self {
        Self {
            coords: Cow::Borrowed(coords),
            geom_offsets: Cow::Borrowed(geom_offsets),
            geom_index,
        }
    }

    pub fn coords(&self) -> LineStringIterator<'a> {
        LineStringIterator::new(self)
    }

    pub fn num_coords(&self) -> usize {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        end - start
    }

    pub fn coord(&self, i: usize) -> Option<PointScalar> {
        let (start, end) = self.geom_offsets.start_end(self.geom_index);
        if i > (end - start) {
            return None;
        }
        Some(PointScalar(self.coords.get(start + i).unwrap()))
    }
}

// Geometry Iterator
pub struct LineStringIterator<'a> {
    geom: &'a LineStringScalar<'a>,
    index: usize,
    end: usize,
}

impl<'a> LineStringIterator<'_> {
    #[inline]
    pub fn new(geom: &'a LineStringScalar) -> Self {
        Self {
            geom,
            index: 0,
            end: geom.num_coords(),
        }
    }
}

impl<'a> Iterator for LineStringIterator<'a> {
    type Item = PointScalar;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let current = self.index;
        self.index += 1;
        self.geom.coord(current)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}
