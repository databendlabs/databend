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

/// The [`Scalar`] implementation of a boolean.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BooleanScalar {
    value: Option<bool>,
}

impl BooleanScalar {
    /// Returns a new [`BooleanScalar`]
    #[inline]
    pub fn new(value: Option<bool>) -> Self {
        Self { value }
    }

    /// The value
    #[inline]
    pub fn value(&self) -> Option<bool> {
        self.value
    }
}

impl Scalar for BooleanScalar {
    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    #[inline]
    fn is_valid(&self) -> bool {
        self.value.is_some()
    }

    #[inline]
    fn data_type(&self) -> &DataType {
        &DataType::Boolean
    }
}

impl From<Option<bool>> for BooleanScalar {
    #[inline]
    fn from(v: Option<bool>) -> Self {
        Self::new(v)
    }
}
