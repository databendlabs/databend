// Copyright 2020-2022 Jorge C. Leitão
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

use std::any::Any;

use super::Scalar;
use crate::arrow::array::*;
use crate::arrow::datatypes::DataType;

/// The scalar equivalent of [`FixedSizeListArray`]. Like [`FixedSizeListArray`], this struct holds a dynamically-typed
/// [`Array`]. The only difference is that this has only one element.
#[derive(Debug, Clone)]
pub struct FixedSizeListScalar {
    values: Option<Box<dyn Array>>,
    data_type: DataType,
}

impl PartialEq for FixedSizeListScalar {
    fn eq(&self, other: &Self) -> bool {
        (self.data_type == other.data_type)
            && (self.values.is_some() == other.values.is_some())
            && ((self.values.is_none()) | (self.values.as_ref() == other.values.as_ref()))
    }
}

impl FixedSizeListScalar {
    /// returns a new [`FixedSizeListScalar`]
    /// # Panics
    /// iff
    /// * the `data_type` is not `FixedSizeList`
    /// * the child of the `data_type` is not equal to the `values`
    /// * the size of child array is not equal
    #[inline]
    pub fn new(data_type: DataType, values: Option<Box<dyn Array>>) -> Self {
        let (field, size) = FixedSizeListArray::get_child_and_size(&data_type);
        let inner_data_type = field.data_type();
        let values = values.inspect(|x| {
            assert_eq!(inner_data_type, x.data_type());
            assert_eq!(size, x.len());
        });
        Self { values, data_type }
    }

    /// The values of the [`FixedSizeListScalar`]
    #[allow(clippy::borrowed_box)]
    pub fn values(&self) -> Option<&Box<dyn Array>> {
        self.values.as_ref()
    }
}

impl Scalar for FixedSizeListScalar {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn is_valid(&self) -> bool {
        self.values.is_some()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}
