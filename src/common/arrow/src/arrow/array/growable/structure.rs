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

use super::make_growable;
use super::utils::build_extend_null_bits;
use super::utils::ExtendNullBits;
use super::Growable;
use crate::arrow::array::Array;
use crate::arrow::array::StructArray;
use crate::arrow::bitmap::MutableBitmap;

/// Concrete [`Growable`] for the [`StructArray`].
pub struct GrowableStruct<'a> {
    arrays: Vec<&'a StructArray>,
    validity: MutableBitmap,
    values: Vec<Box<dyn Growable<'a> + 'a>>,
    extend_null_bits: Vec<ExtendNullBits<'a>>,
}

impl<'a> GrowableStruct<'a> {
    /// Creates a new [`GrowableStruct`] bound to `arrays` with a pre-allocated `capacity`.
    /// # Panics
    /// If `arrays` is empty.
    pub fn new(arrays: Vec<&'a StructArray>, mut use_validity: bool, capacity: usize) -> Self {
        assert!(!arrays.is_empty());

        // if any of the arrays has nulls, insertions from any array requires setting bits
        // as there is at least one array with nulls.
        if arrays.iter().any(|array| array.null_count() > 0) {
            use_validity = true;
        };

        let extend_null_bits = arrays
            .iter()
            .map(|array| build_extend_null_bits(*array, use_validity))
            .collect();

        let arrays = arrays
            .iter()
            .map(|array| array.as_any().downcast_ref::<StructArray>().unwrap())
            .collect::<Vec<_>>();

        // ([field1, field2], [field3, field4]) -> ([field1, field3], [field2, field3])
        let values = (0..arrays[0].values().len())
            .map(|i| {
                make_growable(
                    &arrays
                        .iter()
                        .map(|x| x.values()[i].as_ref())
                        .collect::<Vec<_>>(),
                    use_validity,
                    capacity,
                )
            })
            .collect::<Vec<Box<dyn Growable>>>();

        Self {
            arrays,
            values,
            validity: MutableBitmap::with_capacity(capacity),
            extend_null_bits,
        }
    }

    fn to(&mut self) -> StructArray {
        let validity = std::mem::take(&mut self.validity);
        let values = std::mem::take(&mut self.values);
        let values = values.into_iter().map(|mut x| x.as_box()).collect();

        StructArray::new(self.arrays[0].data_type().clone(), values, validity.into())
    }
}

impl<'a> Growable<'a> for GrowableStruct<'a> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        (self.extend_null_bits[index])(&mut self.validity, start, len);

        let array = self.arrays[index];
        if array.null_count() == 0 {
            self.values
                .iter_mut()
                .for_each(|child| child.extend(index, start, len))
        } else {
            (start..start + len).for_each(|i| {
                if array.is_valid(i) {
                    self.values
                        .iter_mut()
                        .for_each(|child| child.extend(index, i, 1))
                } else {
                    self.values
                        .iter_mut()
                        .for_each(|child| child.extend_validity(1))
                }
            })
        }
    }

    fn extend_validity(&mut self, additional: usize) {
        self.values
            .iter_mut()
            .for_each(|child| child.extend_validity(additional));
        self.validity.extend_constant(additional, false);
    }

    #[inline]
    fn len(&self) -> usize {
        // All children should have the same indexing, so just use the first
        // one. If we don't have children, we might still have a validity
        // array, so use that.
        if let Some(child) = self.values.first() {
            child.len()
        } else {
            self.validity.len()
        }
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(self.to())
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(self.to())
    }
}

impl<'a> From<GrowableStruct<'a>> for StructArray {
    fn from(val: GrowableStruct<'a>) -> Self {
        let values = val.values.into_iter().map(|mut x| x.as_box()).collect();

        StructArray::new(
            val.arrays[0].data_type().clone(),
            values,
            val.validity.into(),
        )
    }
}
