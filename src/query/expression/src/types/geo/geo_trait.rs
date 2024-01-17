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

use databend_common_arrow::arrow::array::Array;
use databend_common_arrow::arrow::datatypes::DataType as ArrowDataType;

pub trait AsArrow {
    fn as_arrow(&self, arrow_type: ArrowDataType) -> Box<dyn Array>;
}

pub trait GeometryScalarTrait {
    /// The [`geo`] scalar object for this geometry array type.
    type ScalarGeo;

    fn to_geo(&self) -> Self::ScalarGeo;
}

pub trait GeometryColumnAccessor<'a> {
    /// The [`geo`] scalar for this geometry array type.
    type Item: Send + Sync + GeometryScalarTrait;

    /// The number of geometries contained in this array.
    fn len(&self) -> usize;

    /// Returns the element at index `i`
    /// # Panics
    /// Panics if the value is outside the bounds of the array
    fn get(&'a self, index: usize) -> Option<Self::Item> {
        if index > self.len() {
            None
        } else {
            unsafe { Some(self.get_unchecked(index)) }
        }
    }

    /// Returns the element at index `i`
    /// # Safety
    /// Caller is responsible for ensuring that the index is within the bounds of the array
    unsafe fn get_unchecked(&'a self, index: usize) -> Self::Item;
}
