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

#[derive(Debug, Clone, PartialEq, Eq)]
/// The [`Scalar`] implementation of fixed size binary ([`Option<Box<[u8]>>`]).
pub struct FixedSizeBinaryScalar {
    value: Option<Box<[u8]>>,
    data_type: DataType,
}

impl FixedSizeBinaryScalar {
    /// Returns a new [`FixedSizeBinaryScalar`].
    /// # Panics
    /// iff
    /// * the `data_type` is not `FixedSizeBinary`
    /// * the size of child binary is not equal
    #[inline]
    pub fn new<P: Into<Vec<u8>>>(data_type: DataType, value: Option<P>) -> Self {
        assert_eq!(
            data_type.to_physical_type(),
            crate::arrow::datatypes::PhysicalType::FixedSizeBinary
        );
        Self {
            value: value.map(|x| {
                let x: Vec<u8> = x.into();
                assert_eq!(
                    data_type.to_logical_type(),
                    &DataType::FixedSizeBinary(x.len())
                );
                x.into_boxed_slice()
            }),
            data_type,
        }
    }

    /// Its value
    #[inline]
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_ref().map(|x| x.as_ref())
    }
}

impl Scalar for FixedSizeBinaryScalar {
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
