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
use databend_common_arrow::arrow::datatypes::DataType as ArrowDataType;
use databend_common_exception::Result;

use crate::types::geo::coord::CoordColumn;
use crate::types::geo::coord::CoordColumnBuilder;
use crate::types::geo::coord::CoordScalar;
use crate::types::geo::geo_trait::AsArrow;
use crate::types::geo::utils::coord_eq_allow_nan;

#[derive(Clone, Debug)]
pub struct PointColumn {
    pub coords: CoordColumn,
}

impl PointColumn {
    pub fn len(&self) -> usize {
        self.coords.len()
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        Self {
            coords: self.coords.slice(range),
        }
    }
    pub fn get(&self, index: usize) -> Option<PointScalar> {
        self.coords.get(index).map(|c| PointScalar(c))
    }

    pub unsafe fn get_unchecked(&self, index: usize) -> PointScalar {
        PointScalar(self.coords.get_unchecked(index))
    }

    pub fn memory_size(&self) -> usize {
        self.len() * 8 * 2
    }
}

impl AsArrow for PointColumn {
    fn as_arrow(&self, arrow_type: ArrowDataType) -> Box<dyn Array> {
        self.coords.as_arrow(arrow_type)
    }
}

impl PartialEq for PointColumn {
    fn eq(&self, other: &Self) -> bool {
        if self.coords.len() != other.coords.len() {
            return false;
        }

        if self.coords == other.coords {
            return true;
        }

        for coord_idx in 0..self.coords.len() {
            let c1 = self.coords.get(coord_idx).unwrap();
            let c2 = other.coords.get(coord_idx).unwrap();

            if !coord_eq_allow_nan(&c1, &c2) {
                return false;
            }
        }

        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Default, Hash)]
pub struct PointScalar(CoordScalar);

pub struct PointColumnBuilder(CoordColumnBuilder);

impl PointColumnBuilder {
    pub fn from_column(col: PointColumn) -> Self {
        Self(CoordColumnBuilder::from_column(col.coords))
    }

    pub fn repeat(scalar: PointScalar, n: usize) -> Self {
        let coords = CoordColumnBuilder::repeat(scalar.0, n);
        Self(coords)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn memory_size(&self) -> usize {
        self.0.memory_size()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(CoordColumnBuilder::with_capacity(capacity))
    }

    pub fn push(&mut self, item: PointScalar) {
        self.0.push(item.0)
    }

    pub fn push_default(&mut self) {
        self.0.push_default()
    }

    pub fn push_binary(&mut self, bytes: &mut &[u8]) -> Result<()> {
        self.0.push_binary(bytes)
    }

    pub fn pop(&mut self) -> Option<PointScalar> {
        self.0.pop().map(|c| PointScalar(c))
    }

    pub fn append_column(&mut self, other: &PointColumn) {
        self.0.append_column(&other.coords)
    }

    pub fn build(self) -> PointColumn {
        PointColumn {
            coords: self.0.build(),
        }
    }

    pub fn build_scalar(self) -> PointScalar {
        PointScalar(self.0.build_scalar())
    }
}
