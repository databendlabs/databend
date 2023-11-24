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

/// The [`DictionaryArray`] equivalent of [`Array`] for [`Scalar`].
#[derive(Debug, Clone)]
pub struct DictionaryScalar<K: DictionaryKey> {
    value: Option<Box<dyn Scalar>>,
    phantom: std::marker::PhantomData<K>,
    data_type: DataType,
}

impl<K: DictionaryKey> PartialEq for DictionaryScalar<K> {
    fn eq(&self, other: &Self) -> bool {
        (self.data_type == other.data_type) && (self.value.as_ref() == other.value.as_ref())
    }
}

impl<K: DictionaryKey> DictionaryScalar<K> {
    /// returns a new [`DictionaryScalar`]
    /// # Panics
    /// iff
    /// * the `data_type` is not `List` or `LargeList` (depending on this scalar's offset `O`)
    /// * the child of the `data_type` is not equal to the `values`
    #[inline]
    pub fn new(data_type: DataType, value: Option<Box<dyn Scalar>>) -> Self {
        Self {
            value,
            phantom: std::marker::PhantomData,
            data_type,
        }
    }

    /// The values of the [`DictionaryScalar`]
    #[allow(clippy::borrowed_box)]
    pub fn value(&self) -> Option<&Box<dyn Scalar>> {
        self.value.as_ref()
    }
}

impl<K: DictionaryKey> Scalar for DictionaryScalar<K> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn is_valid(&self) -> bool {
        self.value.is_some()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}
