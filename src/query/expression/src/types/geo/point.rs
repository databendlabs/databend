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
use enum_as_inner::EnumAsInner;

use crate::types::geo::coord::CoordColumn;
use crate::types::geo::coord::CoordScalar;
use crate::types::geo::geo_trait::AsArrow;
use crate::types::geo::utils::coord_eq_allow_nan;

#[derive(Clone, Debug, EnumAsInner)]
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
        self.coords.get(index).map(|coord| PointScalar(coord))
    }

    pub unsafe fn get_unchecked(&self, index: usize) -> PointScalar {
        PointScalar(self.coords.get_unchecked(index))
    }
}

impl AsArrow for PointColumn {
    fn as_arrow(&self, arrow_type: ArrowDataType) -> Box<dyn Array> {
        self.coords.as_arrow(arrow_type)
    }
}

impl PartialEq for PointColumn {
    fn eq(&self, other: &Self) -> bool {
        // todo: check validity
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

pub struct PointScalar(CoordScalar);
