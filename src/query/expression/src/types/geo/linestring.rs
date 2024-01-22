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

use databend_common_arrow::arrow::array::Array;
use databend_common_arrow::arrow::array::ListArray;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_arrow::arrow::datatypes::DataType as ArrowDataType;
use databend_common_arrow::arrow::offset::OffsetsBuffer;
use databend_common_exception::Result;
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

    pub fn memory_size(&self) -> usize {
        todo!()
    }

    pub fn iter(&self) -> LineStringIterator {
        todo!()
    }
}

impl<'a> GeometryColumnAccessor<'a> for LineStringColumn {
    type Item = LineStringScalar;

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

#[derive(Clone, Debug)]
pub struct LineStringColumnIter<'a> {
    column: &'a LineStringColumn,
    current: usize,
    current_end: usize,
}

impl<'a> LineStringColumnIter<'a> {
    #[inline]
    pub fn new(column: &'a LineStringColumn) -> Self {
        let len = column.len();
        Self {
            column,
            current: 0,
            current_end: len,
        }
    }
}

impl<'a> Iterator for LineStringColumnIter<'a> {
    type Item = Option<LineStringScalar>;

    // todo: check null
    // else if self.is_null(self.current) {
    // self.current += 1;
    // Some(None)
    // }
    fn next(&mut self) -> Option<Self::Item> {
        if self.current = self.current_end {
            None
        } else {
            let old = self.current;
            self.current += 1;

            unsafe { Some(Some(self.column.get_unchecked(old))) }
        }
    }
}

pub struct LineStringScalar {
    pub coords: CoordColumn,
    pub geom_offsets: OffsetsBuffer<i64>,
    pub geom_index: usize,
}

impl LineStringScalar {
    pub fn new(coords: CoordColumn, geom_offsets: OffsetsBuffer<i64>, geom_index: usize) -> Self {
        Self {
            coords,
            geom_offsets,
            geom_index,
        }
    }

    pub fn coords(&self) -> LineStringIterator {
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
    geom: &'a LineStringScalar,
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

pub struct LineStringColumnBuilder;

impl LineStringColumnBuilder {
    pub fn from_column(col: LineStringColumn) -> Self {
        todo!()
    }

    pub fn repeat(scalar: LineStringScalar, n: usize) -> Self {
        todo!()
    }

    pub fn len(&self) -> usize {
        todo!()
    }

    pub fn memory_size(&self) -> usize {
        todo!()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        todo!()
    }

    pub fn push(&mut self, item: LineStringScalar) {
        todo!()
    }

    pub fn push_default(&mut self) {
        todo!()
    }

    pub fn push_binary(&mut self, bytes: &mut &[u8]) -> Result<()> {
        todo!()
    }

    pub fn pop(&mut self) -> Option<LineStringScalar> {
        todo!()
    }

    pub fn append_column(&mut self, other: &LineStringColumn) {
        todo!()
    }
    pub fn build(self) -> LineStringColumn {
        todo!()
    }

    pub fn build_scalar(self) -> LineStringScalar {
        todo!()
    }
}
