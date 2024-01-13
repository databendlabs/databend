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
use crate::arrow::error::Error;
use crate::arrow::types::NativeType;

/// The implementation of [`Scalar`] for primitive, semantically equivalent to [`Option<T>`]
/// with [`DataType`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimitiveScalar<T: NativeType> {
    value: Option<T>,
    data_type: DataType,
}

impl<T: NativeType> PrimitiveScalar<T> {
    /// Returns a new [`PrimitiveScalar`].
    #[inline]
    pub fn new(data_type: DataType, value: Option<T>) -> Self {
        if !data_type.to_physical_type().eq_primitive(T::PRIMITIVE) {
            panic!(
                "{:?}",
                Error::InvalidArgumentError(format!(
                    "Type {} does not support logical type {:?}",
                    std::any::type_name::<T>(),
                    data_type
                ))
            )
        }
        Self { value, data_type }
    }

    /// Returns the optional value.
    #[inline]
    pub fn value(&self) -> &Option<T> {
        &self.value
    }

    /// Returns a new `PrimitiveScalar` with the same value but different [`DataType`]
    /// # Panic
    /// This function panics if the `data_type` is not valid for self's physical type `T`.
    pub fn to(self, data_type: DataType) -> Self {
        Self::new(data_type, self.value)
    }
}

impl<T: NativeType> From<Option<T>> for PrimitiveScalar<T> {
    #[inline]
    fn from(v: Option<T>) -> Self {
        Self::new(T::PRIMITIVE.into(), v)
    }
}

impl<T: NativeType> Scalar for PrimitiveScalar<T> {
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
        &self.data_type
    }
}
