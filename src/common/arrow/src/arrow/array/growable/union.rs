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
use super::Growable;
use crate::arrow::array::Array;
use crate::arrow::array::UnionArray;

/// Concrete [`Growable`] for the [`UnionArray`].
pub struct GrowableUnion<'a> {
    arrays: Vec<&'a UnionArray>,
    types: Vec<i8>,
    offsets: Option<Vec<i32>>,
    fields: Vec<Box<dyn Growable<'a> + 'a>>,
}

impl<'a> GrowableUnion<'a> {
    /// Creates a new [`GrowableUnion`] bound to `arrays` with a pre-allocated `capacity`.
    /// # Panics
    /// Panics iff
    /// * `arrays` is empty.
    /// * any of the arrays has a different
    pub fn new(arrays: Vec<&'a UnionArray>, capacity: usize) -> Self {
        assert!(!arrays.is_empty());
        let first = arrays[0].data_type();
        assert!(arrays.iter().all(|x| x.data_type() == first));

        let has_offsets = arrays[0].offsets().is_some();

        let fields = (0..arrays[0].fields().len())
            .map(|i| {
                make_growable(
                    &arrays
                        .iter()
                        .map(|x| x.fields()[i].as_ref())
                        .collect::<Vec<_>>(),
                    false,
                    capacity,
                )
            })
            .collect::<Vec<Box<dyn Growable>>>();

        Self {
            arrays,
            fields,
            offsets: if has_offsets {
                Some(Vec::with_capacity(capacity))
            } else {
                None
            },
            types: Vec::with_capacity(capacity),
        }
    }

    fn to(&mut self) -> UnionArray {
        let types = std::mem::take(&mut self.types);
        let fields = std::mem::take(&mut self.fields);
        let offsets = std::mem::take(&mut self.offsets);
        let fields = fields.into_iter().map(|mut x| x.as_box()).collect();

        UnionArray::new(
            self.arrays[0].data_type().clone(),
            types.into(),
            fields,
            offsets.map(|x| x.into()),
        )
    }
}

impl<'a> Growable<'a> for GrowableUnion<'a> {
    fn extend(&mut self, index: usize, start: usize, len: usize) {
        let array = self.arrays[index];

        let types = &array.types()[start..start + len];
        self.types.extend(types);
        if let Some(x) = self.offsets.as_mut() {
            let offsets = &array.offsets().unwrap()[start..start + len];

            // in a dense union, each slot has its own offset. We extend the fields accordingly.
            for (&type_, &offset) in types.iter().zip(offsets.iter()) {
                let field = &mut self.fields[type_ as usize];
                // The offset for the element that is about to be extended is the current length
                // of the child field of the corresponding type. Note that this may be very
                // different than the original offset from the array we are extending from as
                // it is a function of the previous extensions to this child.
                x.push(field.len() as i32);
                field.extend(index, offset as usize, 1);
            }
        } else {
            // in a sparse union, every field has the same length => extend all fields equally
            self.fields
                .iter_mut()
                .for_each(|field| field.extend(index, start, len))
        }
    }

    fn extend_validity(&mut self, _additional: usize) {}

    #[inline]
    fn len(&self) -> usize {
        self.types.len()
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        self.to().arced()
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        self.to().boxed()
    }
}

impl<'a> From<GrowableUnion<'a>> for UnionArray {
    fn from(val: GrowableUnion<'a>) -> Self {
        let fields = val.fields.into_iter().map(|mut x| x.as_box()).collect();

        UnionArray::new(
            val.arrays[0].data_type().clone(),
            val.types.into(),
            fields,
            val.offsets.map(|x| x.into()),
        )
    }
}
