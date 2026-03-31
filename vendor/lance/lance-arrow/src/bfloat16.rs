// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! bfloat16 support for Apache Arrow.

use std::fmt::Formatter;
use std::slice;

use arrow_array::{
    builder::BooleanBufferBuilder, iterator::ArrayIter, Array, ArrayAccessor, ArrayRef,
    FixedSizeBinaryArray,
};
use arrow_buffer::MutableBuffer;
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, Field as ArrowField};
use half::bf16;

use crate::{FloatArray, ARROW_EXT_NAME_KEY};

/// The name of the bfloat16 extension in Arrow metadata
pub const BFLOAT16_EXT_NAME: &str = "lance.bfloat16";

/// Check whether the given field is a bfloat16 field
///
/// A field is a bfloat16 field if it has a data type of `FixedSizeBinary(2)` and the metadata
/// contains the bfloat16 extension name.
pub fn is_bfloat16_field(field: &ArrowField) -> bool {
    field.data_type() == &DataType::FixedSizeBinary(2)
        && field
            .metadata()
            .get(ARROW_EXT_NAME_KEY)
            .map(|name| name == BFLOAT16_EXT_NAME)
            .unwrap_or_default()
}

/// The bfloat16 data type
///
/// This implements the [`ArrowFloatType`](crate::floats::ArrowFloatType) trait for bfloat16 values.
#[derive(Debug)]
pub struct BFloat16Type {}

/// An array of bfloat16 values
///
/// This implements the [`Array`](arrow_array::Array) trait for bfloat16 values.  Note that
/// bfloat16 is not the same thing as fp16 which is supported natively
/// by arrow-rs.
#[derive(Clone)]
pub struct BFloat16Array {
    inner: FixedSizeBinaryArray,
}

impl std::fmt::Debug for BFloat16Array {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BFloat16Array\n[\n")?;
        from_arrow::print_long_array(&self.inner, f, |array, i, f| {
            if array.is_null(i) {
                write!(f, "null")
            } else {
                let binary_values = array.value(i);
                let value =
                    bf16::from_bits(u16::from_le_bytes([binary_values[0], binary_values[1]]));
                write!(f, "{:?}", value)
            }
        })?;
        write!(f, "]")
    }
}

impl BFloat16Array {
    pub fn from_iter_values(iter: impl IntoIterator<Item = bf16>) -> Self {
        let values: Vec<bf16> = iter.into_iter().collect();
        values.into()
    }

    pub fn iter(&self) -> BFloat16Iter<'_> {
        BFloat16Iter::new(self)
    }

    pub fn value(&self, i: usize) -> bf16 {
        assert!(
            i < self.len(),
            "Trying to access an element at index {} from a BFloat16Array of length {}",
            i,
            self.len()
        );
        // Safety:
        // `i < self.len()
        unsafe { self.value_unchecked(i) }
    }

    /// # Safety
    /// Caller must ensure that `i < self.len()`
    pub unsafe fn value_unchecked(&self, i: usize) -> bf16 {
        let binary_value = self.inner.value_unchecked(i);
        bf16::from_bits(u16::from_le_bytes([binary_value[0], binary_value[1]]))
    }

    pub fn into_inner(self) -> FixedSizeBinaryArray {
        self.inner
    }
}

impl ArrayAccessor for &BFloat16Array {
    type Item = bf16;

    fn value(&self, index: usize) -> Self::Item {
        BFloat16Array::value(self, index)
    }

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        BFloat16Array::value_unchecked(self, index)
    }
}

impl Array for BFloat16Array {
    fn as_any(&self) -> &dyn std::any::Any {
        self.inner.as_any()
    }

    fn to_data(&self) -> arrow_data::ArrayData {
        self.inner.to_data()
    }

    fn into_data(self) -> arrow_data::ArrayData {
        self.inner.into_data()
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        let inner_array: &dyn Array = &self.inner;
        inner_array.slice(offset, length)
    }

    fn nulls(&self) -> Option<&arrow_buffer::NullBuffer> {
        self.inner.nulls()
    }

    fn data_type(&self) -> &DataType {
        self.inner.data_type()
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn offset(&self) -> usize {
        self.inner.offset()
    }

    fn get_array_memory_size(&self) -> usize {
        self.inner.get_array_memory_size()
    }

    fn get_buffer_memory_size(&self) -> usize {
        self.inner.get_buffer_memory_size()
    }
}

impl FromIterator<Option<bf16>> for BFloat16Array {
    fn from_iter<I: IntoIterator<Item = Option<bf16>>>(iter: I) -> Self {
        let mut buffer = MutableBuffer::new(10);
        // No null buffer builder :(
        let mut nulls = BooleanBufferBuilder::new(10);
        let mut len = 0;

        for maybe_value in iter {
            if let Some(value) = maybe_value {
                let bytes = value.to_le_bytes();
                buffer.extend(bytes);
            } else {
                buffer.extend([0u8, 0u8]);
            }
            nulls.append(maybe_value.is_some());
            len += 1;
        }

        let null_buffer = nulls.finish();
        let num_valid = null_buffer.count_set_bits();
        let null_buffer = if num_valid == len {
            None
        } else {
            Some(null_buffer.into_inner())
        };

        let array_data = ArrayData::builder(DataType::FixedSizeBinary(2))
            .len(len)
            .add_buffer(buffer.into())
            .null_bit_buffer(null_buffer);
        let array_data = unsafe { array_data.build_unchecked() };
        Self {
            inner: FixedSizeBinaryArray::from(array_data),
        }
    }
}

impl FromIterator<bf16> for BFloat16Array {
    fn from_iter<I: IntoIterator<Item = bf16>>(iter: I) -> Self {
        Self::from_iter_values(iter)
    }
}

impl From<Vec<bf16>> for BFloat16Array {
    fn from(data: Vec<bf16>) -> Self {
        let mut buffer = MutableBuffer::with_capacity(data.len() * 2);

        let bytes = data.iter().flat_map(|val| {
            let bytes = val.to_bits().to_le_bytes();
            bytes.to_vec()
        });

        buffer.extend(bytes);
        let array_data = ArrayData::builder(DataType::FixedSizeBinary(2))
            .len(data.len())
            .add_buffer(buffer.into());
        let array_data = unsafe { array_data.build_unchecked() };
        Self {
            inner: FixedSizeBinaryArray::from(array_data),
        }
    }
}

impl TryFrom<FixedSizeBinaryArray> for BFloat16Array {
    type Error = ArrowError;

    fn try_from(value: FixedSizeBinaryArray) -> Result<Self, Self::Error> {
        if value.value_length() == 2 {
            Ok(Self { inner: value })
        } else {
            Err(ArrowError::InvalidArgumentError(
                "FixedSizeBinaryArray must have a value length of 2".to_string(),
            ))
        }
    }
}

impl PartialEq<Self> for BFloat16Array {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

type BFloat16Iter<'a> = ArrayIter<&'a BFloat16Array>;

/// Methods that are lifted from arrow-rs temporarily until they are made public.
mod from_arrow {
    use arrow_array::Array;

    /// Helper function for printing potentially long arrays.
    pub(super) fn print_long_array<A, F>(
        array: &A,
        f: &mut std::fmt::Formatter,
        print_item: F,
    ) -> std::fmt::Result
    where
        A: Array,
        F: Fn(&A, usize, &mut std::fmt::Formatter) -> std::fmt::Result,
    {
        let head = std::cmp::min(10, array.len());

        for i in 0..head {
            if array.is_null(i) {
                writeln!(f, "  null,")?;
            } else {
                write!(f, "  ")?;
                print_item(array, i, f)?;
                writeln!(f, ",")?;
            }
        }
        if array.len() > 10 {
            if array.len() > 20 {
                writeln!(f, "  ...{} elements...,", array.len() - 20)?;
            }

            let tail = std::cmp::max(head, array.len() - 10);

            for i in tail..array.len() {
                if array.is_null(i) {
                    writeln!(f, "  null,")?;
                } else {
                    write!(f, "  ")?;
                    print_item(array, i, f)?;
                    writeln!(f, ",")?;
                }
            }
        }
        Ok(())
    }
}

impl FloatArray<BFloat16Type> for BFloat16Array {
    type FloatType = BFloat16Type;

    fn as_slice(&self) -> &[bf16] {
        unsafe {
            slice::from_raw_parts(
                self.inner.value_data().as_ptr() as *const bf16,
                self.inner.value_data().len() / 2,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basics() {
        let values: Vec<f32> = vec![1.0, 2.0, 3.0];
        let values: Vec<bf16> = values.iter().map(|v| bf16::from_f32(*v)).collect();

        let array = BFloat16Array::from_iter_values(values.clone());
        let array2 = BFloat16Array::from(values.clone());
        assert_eq!(array, array2);
        assert_eq!(array.len(), 3);

        let expected_fmt = "BFloat16Array\n[\n  1.0,\n  2.0,\n  3.0,\n]";
        assert_eq!(expected_fmt, format!("{:?}", array));

        for (expected, value) in values.iter().zip(array.iter()) {
            assert_eq!(Some(*expected), value);
        }

        for (expected, value) in values.as_slice().iter().zip(array2.iter()) {
            assert_eq!(Some(*expected), value);
        }
    }

    #[test]
    fn test_nulls() {
        let values: Vec<Option<bf16>> =
            vec![Some(bf16::from_f32(1.0)), None, Some(bf16::from_f32(3.0))];
        let array = BFloat16Array::from_iter(values.clone());
        assert_eq!(array.len(), 3);
        assert_eq!(array.null_count(), 1);

        let expected_fmt = "BFloat16Array\n[\n  1.0,\n  null,\n  3.0,\n]";
        assert_eq!(expected_fmt, format!("{:?}", array));

        for (expected, value) in values.iter().zip(array.iter()) {
            assert_eq!(*expected, value);
        }
    }
}
