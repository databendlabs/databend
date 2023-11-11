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

use super::Scalar;
use crate::arrow::datatypes::DataType;

/// A single entry of a [`crate::arrow::array::StructArray`].
#[derive(Debug, Clone)]
pub struct StructScalar {
    values: Vec<Box<dyn Scalar>>,
    is_valid: bool,
    data_type: DataType,
}

impl PartialEq for StructScalar {
    fn eq(&self, other: &Self) -> bool {
        (self.data_type == other.data_type)
            && (self.is_valid == other.is_valid)
            && ((!self.is_valid) | (self.values == other.values))
    }
}

impl StructScalar {
    /// Returns a new [`StructScalar`]
    #[inline]
    pub fn new(data_type: DataType, values: Option<Vec<Box<dyn Scalar>>>) -> Self {
        let is_valid = values.is_some();
        Self {
            values: values.unwrap_or_default(),
            is_valid,
            data_type,
        }
    }

    /// Returns the values irrespectively of the validity.
    #[inline]
    pub fn values(&self) -> &[Box<dyn Scalar>] {
        &self.values
    }
}

impl Scalar for StructScalar {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn is_valid(&self) -> bool {
        self.is_valid
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}
