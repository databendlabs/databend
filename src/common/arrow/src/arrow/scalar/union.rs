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

/// A single entry of a [`crate::arrow::array::UnionArray`].
#[derive(Debug, Clone, PartialEq)]
pub struct UnionScalar {
    value: Box<dyn Scalar>,
    type_: i8,
    data_type: DataType,
}

impl UnionScalar {
    /// Returns a new [`UnionScalar`]
    #[inline]
    pub fn new(data_type: DataType, type_: i8, value: Box<dyn Scalar>) -> Self {
        Self {
            value,
            type_,
            data_type,
        }
    }

    /// Returns the inner value
    #[inline]
    #[allow(clippy::borrowed_box)]
    pub fn value(&self) -> &Box<dyn Scalar> {
        &self.value
    }

    /// Returns the type of the union scalar
    #[inline]
    pub fn type_(&self) -> i8 {
        self.type_
    }
}

impl Scalar for UnionScalar {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn is_valid(&self) -> bool {
        true
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}
