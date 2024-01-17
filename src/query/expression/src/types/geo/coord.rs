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
use databend_common_arrow::arrow::array::FixedSizeListArray;
use databend_common_arrow::arrow::array::Float64Array;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_arrow::arrow::datatypes::DataType as ArrowDataType;
use enum_as_inner::EnumAsInner;

use crate::types::geo::geo_trait::AsArrow;
use crate::types::F64;

// Coord
#[derive(Clone, PartialEq, EnumAsInner)]
pub struct CoordColumn {
    pub coords: Buffer<F64>,
}

impl CoordColumn {
    pub fn len(&self) -> usize {
        self.coords.len() / 2
    }
    pub fn values_array(&self) -> Float64Array {
        Float64Array::from_values(self.coords.clone())
    }
    pub fn slice(&self, range: Range<usize>) -> Self {
        Self {
            coords: self
                .coords
                .clone()
                .sliced(range.start * 2, (range.end - range.start) * 2),
        }
    }

    pub fn get(&self, index: usize) -> Option<CoordScalar> {
        if index > self.len() {
            None
        } else {
            if let Some(x) = self.coords.get(index*2).cloned() && let Some(y) = self.coords.get(index*2+1).cloned() {
                Some(CoordScalar {
                    x,
                    y
                })
            } else {
                None
            }
        }
    }

    pub unsafe fn get_unchecked(&self, index: usize) -> CoordScalar {
        CoordScalar {
            x: *self.coords.get_unchecked(index * 2),
            y: *self.coords.get_unchecked(index * 2 + 1),
        }
    }
}

impl AsArrow for CoordColumn {
    fn as_arrow(&self, arrow_type: ArrowDataType) -> Box<dyn Array> {
        Box::new(FixedSizeListArray::new(
            arrow_type,
            self.values_array().boxed(),
            None,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CoordScalar {
    pub x: F64,
    pub y: F64,
}
