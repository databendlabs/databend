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
use crate::arrow::array::DictionaryArray;
use crate::arrow::array::DictionaryKey;
use crate::arrow::array::PrimitiveArray;
use crate::arrow::bitmap::MutableBitmap;
use crate::arrow::datatypes::DataType;

/// Concrete [`Growable`] for the [`DictionaryArray`].
/// # Implementation
/// This growable does not perform collision checks and instead concatenates
/// the values of each [`DictionaryArray`] one after the other.
pub struct GrowableDictionary<'a, K: DictionaryKey> {
    data_type: DataType,
    keys_values: Vec<&'a [K]>,
    key_values: Vec<K>,
    key_validity: MutableBitmap,
    offsets: Vec<usize>,
    values: Box<dyn Array>,
    extend_null_bits: Vec<ExtendNullBits<'a>>,
}

fn concatenate_values<K: DictionaryKey>(
    arrays_keys: &[&PrimitiveArray<K>],
    arrays_values: &[&dyn Array],
    capacity: usize,
) -> (Box<dyn Array>, Vec<usize>) {
    let mut mutable = make_growable(arrays_values, false, capacity);
    let mut offsets = Vec::with_capacity(arrays_keys.len() + 1);
    offsets.push(0);
    for (i, values) in arrays_values.iter().enumerate() {
        mutable.extend(i, 0, values.len());
        offsets.push(offsets[i] + values.len());
    }
    (mutable.as_box(), offsets)
}

impl<'a, T: DictionaryKey> GrowableDictionary<'a, T> {
    /// Creates a new [`GrowableDictionary`] bound to `arrays` with a pre-allocated `capacity`.
    /// # Panics
    /// If `arrays` is empty.
    pub fn new(arrays: &[&'a DictionaryArray<T>], mut use_validity: bool, capacity: usize) -> Self {
        let data_type = arrays[0].data_type().clone();

        // if any of the arrays has nulls, insertions from any array requires setting bits
        // as there is at least one array with nulls.
        if arrays.iter().any(|array| array.null_count() > 0) {
            use_validity = true;
        };

        let arrays_keys = arrays.iter().map(|array| array.keys()).collect::<Vec<_>>();
        let keys_values = arrays_keys
            .iter()
            .map(|array| array.values().as_slice())
            .collect::<Vec<_>>();

        let extend_null_bits = arrays
            .iter()
            .map(|array| build_extend_null_bits(array.keys(), use_validity))
            .collect();

        let arrays_values = arrays
            .iter()
            .map(|array| array.values().as_ref())
            .collect::<Vec<_>>();

        let (values, offsets) = concatenate_values(&arrays_keys, &arrays_values, capacity);

        Self {
            data_type,
            offsets,
            values,
            keys_values,
            key_values: Vec::with_capacity(capacity),
            key_validity: MutableBitmap::with_capacity(capacity),
            extend_null_bits,
        }
    }

    #[inline]
    fn to(&mut self) -> DictionaryArray<T> {
        let validity = std::mem::take(&mut self.key_validity);
        let key_values = std::mem::take(&mut self.key_values);

        #[cfg(debug_assertions)]
        {
            crate::arrow::array::specification::check_indexes(&key_values, self.values.len())
                .unwrap();
        }
        let keys =
            PrimitiveArray::<T>::new(T::PRIMITIVE.into(), key_values.into(), validity.into());

        // Safety - the invariant of this struct ensures that this is up-held
        unsafe {
            DictionaryArray::<T>::try_new_unchecked(
                self.data_type.clone(),
                keys,
                self.values.clone(),
            )
            .unwrap()
        }
    }
}

impl<'a, T: DictionaryKey> Growable<'a> for GrowableDictionary<'a, T> {
    #[inline]
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        (self.extend_null_bits[index])(&mut self.key_validity, start, len);

        let values = &self.keys_values[index][start..start + len];
        let offset = self.offsets[index];
        self.key_values.extend(
            values
                .iter()
                // `.unwrap_or(0)` because this operation does not check for null values, which may contain any key.
                .map(|x| {
                    let x: usize = offset + (*x).try_into().unwrap_or(0);
                    let x: T = match x.try_into() {
                        Ok(key) => key,
                        // todo: convert this to an error.
                        Err(_) => {
                            panic!("The maximum key is too small")
                        }
                    };
                    x
                }),
        );
    }

    #[inline]
    fn len(&self) -> usize {
        self.key_values.len()
    }

    #[inline]
    fn extend_validity(&mut self, additional: usize) {
        self.key_values
            .resize(self.key_values.len() + additional, T::default());
        self.key_validity.extend_constant(additional, false);
    }

    #[inline]
    fn as_arc(&mut self) -> Arc<dyn Array> {
        Arc::new(self.to())
    }

    #[inline]
    fn as_box(&mut self) -> Box<dyn Array> {
        Box::new(self.to())
    }
}

impl<'a, T: DictionaryKey> From<GrowableDictionary<'a, T>> for DictionaryArray<T> {
    #[inline]
    fn from(mut val: GrowableDictionary<'a, T>) -> Self {
        val.to()
    }
}
