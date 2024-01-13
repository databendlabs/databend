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

use super::utils::build_extend_null_bits;
use super::utils::ExtendNullBits;
use super::Growable;
use crate::arrow::array::Array;
use crate::arrow::array::BooleanArray;
use crate::arrow::bitmap::MutableBitmap;
use crate::arrow::datatypes::DataType;

/// Concrete [`Growable`] for the [`BooleanArray`].
pub struct GrowableBoolean<'a> {
    arrays: Vec<&'a BooleanArray>,
    data_type: DataType,
    validity: MutableBitmap,
    values: MutableBitmap,
    extend_null_bits: Vec<ExtendNullBits<'a>>,
}

impl<'a> GrowableBoolean<'a> {
    /// Creates a new [`GrowableBoolean`] bound to `arrays` with a pre-allocated `capacity`.
    /// # Panics
    /// If `arrays` is empty.
    pub fn new(arrays: Vec<&'a BooleanArray>, mut use_validity: bool, capacity: usize) -> Self {
        let data_type = arrays[0].data_type().clone();

        // if any of the arrays has nulls, insertions from any array requires setting bits
        // as there is at least one array with nulls.
        if !use_validity & arrays.iter().any(|array| array.null_count() > 0) {
            use_validity = true;
        };

        let extend_null_bits = arrays
            .iter()
            .map(|array| build_extend_null_bits(*array, use_validity))
            .collect();

        Self {
            arrays,
            data_type,
            values: MutableBitmap::with_capacity(capacity),
            validity: MutableBitmap::with_capacity(capacity),
            extend_null_bits,
        }
    }

    fn to(&mut self) -> BooleanArray {
        let validity = std::mem::take(&mut self.validity);
        let values = std::mem::take(&mut self.values);

        BooleanArray::new(self.data_type.clone(), values.into(), validity.into())
    }
}

impl<'a> Growable<'a> for GrowableBoolean<'a> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        (self.extend_null_bits[index])(&mut self.validity, start, len);

        let array = self.arrays[index];
        let values = array.values();

        let (slice, offset, _) = values.as_slice();
        // safety: invariant offset + length <= slice.len()
        unsafe {
            self.values
                .extend_from_slice_unchecked(slice, start + offset, len);
        }
    }

    fn extend_validity(&mut self, additional: usize) {
        self.values.extend_constant(additional, false);
        self.validity.extend_constant(additional, false);
    }

    #[inline]
    fn len(&self) -> usize {
        self.values.len()
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(self.to())
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(self.to())
    }
}

impl<'a> From<GrowableBoolean<'a>> for BooleanArray {
    fn from(val: GrowableBoolean<'a>) -> Self {
        BooleanArray::new(val.data_type, val.values.into(), val.validity.into())
    }
}
