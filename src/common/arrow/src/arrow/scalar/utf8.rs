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

/// The implementation of [`Scalar`] for utf8, semantically equivalent to [`Option<String>`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Utf8Scalar<O: Offset> {
    value: Option<String>,
    phantom: std::marker::PhantomData<O>,
}

impl<O: Offset> Utf8Scalar<O> {
    /// Returns a new [`Utf8Scalar`]
    #[inline]
    pub fn new<P: Into<String>>(value: Option<P>) -> Self {
        Self {
            value: value.map(|x| x.into()),
            phantom: std::marker::PhantomData,
        }
    }

    /// Returns the value irrespectively of the validity.
    #[inline]
    pub fn value(&self) -> Option<&str> {
        self.value.as_ref().map(|x| x.as_ref())
    }
}

impl<O: Offset, P: Into<String>> From<Option<P>> for Utf8Scalar<O> {
    #[inline]
    fn from(v: Option<P>) -> Self {
        Self::new(v)
    }
}

impl<O: Offset> Scalar for Utf8Scalar<O> {
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
            &DataType::LargeUtf8
        } else {
            &DataType::Utf8
        }
    }
}
