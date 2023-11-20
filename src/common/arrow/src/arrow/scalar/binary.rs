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
use crate::arrow::offset::Offset;

/// The [`Scalar`] implementation of binary ([`Option<Vec<u8>>`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryScalar<O: Offset> {
    value: Option<Vec<u8>>,
    phantom: std::marker::PhantomData<O>,
}

impl<O: Offset> BinaryScalar<O> {
    /// Returns a new [`BinaryScalar`].
    #[inline]
    pub fn new<P: Into<Vec<u8>>>(value: Option<P>) -> Self {
        Self {
            value: value.map(|x| x.into()),
            phantom: std::marker::PhantomData,
        }
    }

    /// Its value
    #[inline]
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_ref().map(|x| x.as_ref())
    }
}

impl<O: Offset, P: Into<Vec<u8>>> From<Option<P>> for BinaryScalar<O> {
    #[inline]
    fn from(v: Option<P>) -> Self {
        Self::new(v)
    }
}

impl<O: Offset> Scalar for BinaryScalar<O> {
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
        if O::IS_LARGE {
            &DataType::LargeBinary
        } else {
            &DataType::Binary
        }
    }
}
