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

use super::new_empty_array;
use super::specification::try_check_offsets_bounds;
use super::Array;
use crate::arrow::bitmap::Bitmap;
use crate::arrow::datatypes::DataType;
use crate::arrow::datatypes::Field;
use crate::arrow::error::Error;
use crate::arrow::offset::OffsetsBuffer;

#[cfg(feature = "arrow")]
mod data;
mod ffi;
pub(super) mod fmt;
mod iterator;

/// An array representing a (key, value), both of arbitrary logical types.
#[derive(Clone)]
pub struct MapArray {
    data_type: DataType,
    // invariant: field.len() == offsets.len()
    offsets: OffsetsBuffer<i32>,
    field: Box<dyn Array>,
    // invariant: offsets.len() - 1 == Bitmap::len()
    validity: Option<Bitmap>,
}

impl MapArray {
    /// Returns a new [`MapArray`].
    /// # Errors
    /// This function errors iff:
    /// * The last offset is not equal to the field' length
    /// * The `data_type`'s physical type is not [`crate::arrow::datatypes::PhysicalType::Map`]
    /// * The fields' `data_type` is not equal to the inner field of `data_type`
    /// * The validity is not `None` and its length is different from `offsets.len() - 1`.
    pub fn try_new(
        data_type: DataType,
        offsets: OffsetsBuffer<i32>,
        field: Box<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Result<Self, Error> {
        try_check_offsets_bounds(&offsets, field.len())?;

        let inner_field = Self::try_get_field(&data_type)?;
        if let DataType::Struct(inner) = inner_field.data_type() {
            if inner.len() != 2 {
                return Err(Error::InvalidArgumentError(
                    "MapArray's inner `Struct` must have 2 fields (keys and maps)".to_string(),
                ));
            }
        } else {
            return Err(Error::InvalidArgumentError(
                "MapArray expects `DataType::Struct` as its inner logical type".to_string(),
            ));
        }
        if field.data_type() != inner_field.data_type() {
            return Err(Error::InvalidArgumentError(
                "MapArray expects `field.data_type` to match its inner DataType".to_string(),
            ));
        }

        if validity
            .as_ref()
            .map_or(false, |validity| validity.len() != offsets.len_proxy())
        {
            return Err(Error::oos(
                "validity mask length must match the number of values",
            ));
        }

        Ok(Self {
            data_type,
            field,
            offsets,
            validity,
        })
    }

    /// Creates a new [`MapArray`].
    /// # Panics
    /// * The last offset is not equal to the field' length.
    /// * The `data_type`'s physical type is not [`crate::arrow::datatypes::PhysicalType::Map`],
    /// * The validity is not `None` and its length is different from `offsets.len() - 1`.
    pub fn new(
        data_type: DataType,
        offsets: OffsetsBuffer<i32>,
        field: Box<dyn Array>,
        validity: Option<Bitmap>,
    ) -> Self {
        Self::try_new(data_type, offsets, field, validity).unwrap()
    }

    /// Returns a new null [`MapArray`] of `length`.
    pub fn new_null(data_type: DataType, length: usize) -> Self {
        let field = new_empty_array(Self::get_field(&data_type).data_type().clone());
        Self::new(
            data_type,
            vec![0i32; 1 + length].try_into().unwrap(),
            field,
            Some(Bitmap::new_zeroed(length)),
        )
    }

    /// Returns a new empty [`MapArray`].
    pub fn new_empty(data_type: DataType) -> Self {
        let field = new_empty_array(Self::get_field(&data_type).data_type().clone());
        Self::new(data_type, OffsetsBuffer::default(), field, None)
    }
}

impl MapArray {
    /// Returns a slice of this [`MapArray`].
    /// # Panics
    /// panics iff `offset + length > self.len()`
    pub fn slice(&mut self, offset: usize, length: usize) {
        assert!(
            offset + length <= self.len(),
            "the offset of the new Buffer cannot exceed the existing length"
        );
        unsafe { self.slice_unchecked(offset, length) }
    }

    /// Returns a slice of this [`MapArray`].
    /// # Safety
    /// The caller must ensure that `offset + length < self.len()`.
    #[inline]
    pub unsafe fn slice_unchecked(&mut self, offset: usize, length: usize) {
        self.validity.as_mut().and_then(|bitmap| {
            bitmap.slice_unchecked(offset, length);
            (bitmap.unset_bits() > 0).then_some(bitmap)
        });
        self.offsets.slice_unchecked(offset, length + 1);
    }

    impl_sliced!();
    impl_mut_validity!();
    impl_into_array!();

    pub(crate) fn try_get_field(data_type: &DataType) -> Result<&Field, Error> {
        if let DataType::Map(field, _) = data_type.to_logical_type() {
            Ok(field.as_ref())
        } else {
            Err(Error::oos(
                "The data_type's logical type must be DataType::Map",
            ))
        }
    }

    pub(crate) fn get_field(data_type: &DataType) -> &Field {
        Self::try_get_field(data_type).unwrap()
    }
}

// Accessors
impl MapArray {
    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len_proxy()
    }

    /// Returns `true` if the array has a length of 0.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// returns the offsets
    #[inline]
    pub fn offsets(&self) -> &OffsetsBuffer<i32> {
        &self.offsets
    }

    /// Returns the field (guaranteed to be a `Struct`)
    #[inline]
    #[allow(clippy::borrowed_box)]
    pub fn field(&self) -> &Box<dyn Array> {
        &self.field
    }

    /// Returns the element at index `i`.
    #[inline]
    pub fn value(&self, i: usize) -> Box<dyn Array> {
        assert!(i < self.len());
        unsafe { self.value_unchecked(i) }
    }

    /// Returns the element at index `i`.
    /// # Safety
    /// Assumes that the `i < self.len`.
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> Box<dyn Array> {
        // soundness: the invariant of the function
        let (start, end) = self.offsets.start_end_unchecked(i);
        let length = end - start;

        // soundness: the invariant of the struct
        self.field.sliced_unchecked(start, length)
    }
}

impl Array for MapArray {
    impl_common_array!();

    fn validity(&self) -> Option<&Bitmap> {
        self.validity.as_ref()
    }

    #[inline]
    fn with_validity(&self, validity: Option<Bitmap>) -> Box<dyn Array> {
        Box::new(self.clone().with_validity(validity))
    }
}
