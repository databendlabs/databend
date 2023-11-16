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

use std::sync::Arc;

use super::Growable;
use crate::arrow::array::Array;
use crate::arrow::array::NullArray;
use crate::arrow::datatypes::DataType;

/// Concrete [`Growable`] for the [`NullArray`].
pub struct GrowableNull {
    data_type: DataType,
    length: usize,
}

impl Default for GrowableNull {
    fn default() -> Self {
        Self::new(DataType::Null)
    }
}

impl GrowableNull {
    /// Creates a new [`GrowableNull`].
    pub fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            length: 0,
        }
    }
}

impl<'a> Growable<'a> for GrowableNull {
    fn extend(&mut self, _: usize, _: usize, len: usize) {
        self.length += len;
    }

    fn extend_validity(&mut self, additional: usize) {
        self.length += additional;
    }

    #[inline]
    fn len(&self) -> usize {
        self.length
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(NullArray::new(self.data_type.clone(), self.length))
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(NullArray::new(self.data_type.clone(), self.length))
    }
}

impl From<GrowableNull> for NullArray {
    fn from(val: GrowableNull) -> Self {
        NullArray::new(val.data_type, val.length)
    }
}
