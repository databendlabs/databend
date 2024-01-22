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
use databend_common_exception::Result;
use databend_common_io::prelude::BinaryRead;
use enum_as_inner::EnumAsInner;

use crate::types::geo::geo_trait::AsArrow;
use crate::types::string::StringIterator;
use crate::types::ValueType;
use crate::types::F64;
use crate::Column;
use crate::ColumnBuilder;
use crate::Domain;
use crate::Scalar;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordType;

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

    pub fn iter(&self) -> CoordColumnIterator {
        CoordColumnIterator {
            col: &self,
            current: 0,
            current_end: self.len(),
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

#[derive(Debug, Clone, Copy, PartialEq, Default, Hash)]
pub struct CoordScalar {
    pub x: F64,
    pub y: F64,
}

impl CoordScalar {
    pub fn new(x: f64, y: f64) -> Self {
        Self {
            x: x.into(),
            y: y.into(),
        }
    }
}

pub struct CoordColumnIterator<'a> {
    pub col: &'a CoordColumn,
    pub current: usize,
    pub current_end: usize,
}

impl<'a> Iterator for CoordColumnIterator<'a> {
    type Item = CoordScalar;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current = self.current_end {
            None
        } else {
            let index = self.current;
            self.current += 1;
            self.col.get(index)
        }
    }
}

pub struct CoordColumnBuilder {
    pub coords: Vec<F64>,
}

impl CoordColumnBuilder {
    pub fn from_column(col: CoordColumn) -> Self {
        Self {
            coords: col.coords.into(),
        }
    }

    pub fn repeat(scalar: CoordScalar, n: usize) -> CoordColumnBuilder {
        let mut coords = Vec::with_capacity(n * 2);
        (0..n).for_each(|| {
            coords.push(scalar.x);
            coords.push(scalar.y);
        });
        Self { coords }
    }

    pub fn len(&self) -> usize {
        self.coords.len() / 2
    }

    pub fn memory_size(&self) -> usize {
        self.len() * 2 * 8
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            coords: Vec::with_capacity(capacity * 2),
        }
    }

    pub fn push(&mut self, item: CoordScalar) {
        self.coords.push(item.x);
        self.coords.push(item.y);
    }

    pub fn push_default(&mut self) {
        self.coords.push(F64::from(0));
        self.coords.push(F64::from(0));
    }

    pub fn push_binary(&mut self, bytes: &mut &[u8]) -> Result<()> {
        let x: F64 = bytes.read_scalar()?;
        let y: F64 = bytes.read_scalar()?;
        Ok(self.push(CoordScalar { x, y }))
    }

    pub fn append_column(&mut self, other: &CoordColumn) {
        self.coords.extend_from_slice(other.coords.into())
    }

    pub fn build(self) -> CoordColumn {
        CoordColumn {
            coords: self.coords.into(),
        }
    }

    pub fn build_scalar(self) -> CoordScalar {
        assert_eq!(self.len(), 2);

        CoordScalar::new(self.coords[0].0, self.coords[1].0)
    }

    pub fn pop(&mut self) -> Option<CoordScalar> {
        assert_eq!(self.len() % 2, 0);
        let x = self.coords.pop();
        let y = self.coords.pop();
        match (x, y) {
            (Some(x), Some(y)) => Some(CoordScalar::new(x.0, y.0)),
            _ => None,
        }
    }
}
