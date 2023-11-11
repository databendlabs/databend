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

/// The scalar equivalent of [`MapArray`]. Like [`MapArray`], this struct holds a dynamically-typed
/// [`Array`]. The only difference is that this has only one element.
#[derive(Debug, Clone)]
pub struct MapScalar {
    values: Box<dyn Array>,
    is_valid: bool,
    data_type: DataType,
}

impl PartialEq for MapScalar {
    fn eq(&self, other: &Self) -> bool {
        (self.data_type == other.data_type)
            && (self.is_valid == other.is_valid)
            && ((!self.is_valid) | (self.values.as_ref() == other.values.as_ref()))
    }
}

impl MapScalar {
    /// returns a new [`MapScalar`]
    /// # Panics
    /// iff
    /// * the `data_type` is not `Map`
    /// * the child of the `data_type` is not equal to the `values`
    #[inline]
    pub fn new(data_type: DataType, values: Option<Box<dyn Array>>) -> Self {
        let inner_field = MapArray::try_get_field(&data_type).unwrap();
        let inner_data_type = inner_field.data_type();
        let (is_valid, values) = match values {
            Some(values) => {
                assert_eq!(inner_data_type, values.data_type());
                (true, values)
            }
            None => (false, new_empty_array(inner_data_type.clone())),
        };
        Self {
            values,
            is_valid,
            data_type,
        }
    }

    /// The values of the [`MapScalar`]
    #[allow(clippy::borrowed_box)]
    pub fn values(&self) -> &Box<dyn Array> {
        &self.values
    }
}

impl Scalar for MapScalar {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}
