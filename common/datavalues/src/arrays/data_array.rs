// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryFrom;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::ops::BitAnd;
use std::sync::Arc;

use common_arrow::arrow::array as arrow_array;
use common_arrow::arrow::array::*;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::datatypes::IntervalUnit;
use common_arrow::arrow::datatypes::TimeUnit;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::data_df_type::*;
use crate::prelude::AlignedVec;
use crate::series::IntoSeries;
use crate::series::Series;
use crate::series::SeriesTrait;
use crate::DataType;
use crate::DataValue;

/// DataArray is generic struct which implements DataArray
pub struct DataArray<T> {
    pub array: arrow_array::ArrayRef,
    t: PhantomData<T>,
}

impl<T> DataArray<T> {
    pub fn new(array: arrow_array::ArrayRef) -> Self {
        Self {
            array,
            t: PhantomData::<T>,
        }
    }

    pub fn data_type(&self) -> DataType {
        DataType::try_from(self.array.data_type()).unwrap()
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_null(&self, row: usize) -> bool {
        self.array.is_null(row)
    }

    pub fn null_count(&self) -> usize {
        self.array.null_count()
    }

    pub fn all_is_null(&self) -> bool {
        self.null_count() == self.len()
    }

    pub fn get_array_ref(&self) -> ArrayRef {
        self.array.clone()
    }

    /// Get the null count and the buffer of bits representing null values
    pub fn null_bits(&self) -> (usize, Option<Buffer>) {
        let data = self.array.data();

        (
            data.null_count(),
            data.null_bitmap().as_ref().map(|bitmap| {
                let buff = bitmap.buffer_ref();
                buff.clone()
            }),
        )
    }

    pub fn limit(&self, num_elements: usize) -> Self {
        self.slice(0, num_elements)
    }

    pub fn get_array_memory_size(&self) -> usize {
        self.array.get_array_memory_size()
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let array = self.array.slice(offset, length);
        array.into()
    }

    /// Unpack a array to the same physical type.
    ///
    /// # Safety
    ///
    /// This is unsafe as the data_type may be uncorrect and
    /// is assumed to be correct in other unsafe code.
    pub unsafe fn unpack_array_matching_physical_type(
        &self,
        array: &Series,
    ) -> Result<&DataArray<T>> {
        let array_trait = &**array;
        if self.data_type() == array.data_type() {
            let ca = &*(array_trait as *const dyn SeriesTrait as *const DataArray<T>);
            Ok(ca)
        } else {
            use DataType::*;
            match (self.data_type(), array.data_type()) {
                (Int64, Date64) | (Int32, Date32) => {
                    let ca = &*(array_trait as *const dyn SeriesTrait as *const DataArray<T>);
                    Ok(ca)
                }
                _ => Err(ErrorCode::IllegalDataType(format!(
                    "cannot unpack array {:?} into matching type {:?}",
                    array,
                    self.data_type()
                ))),
            }
        }
    }
}

impl<T> DataArray<T>
where T: DFDataType
{
    pub fn name(&self) -> String {
        format!("DataArray<{:?}>", T::data_type())
    }

    /// # Safety
    /// Note this doesn't do any bound checking, for performance reason.
    #[inline]
    pub unsafe fn try_get(&self, index: usize) -> Result<DataValue> {
        let arr = &*self.array;
        macro_rules! downcast_and_pack {
            ($CAST_TYPE:ident, $SCALAR: ident) => {{
                let array = &*(arr as *const dyn Array as *const $CAST_TYPE);

                Ok(DataValue::$SCALAR(match array.is_null(index) {
                    true => None,
                    false => Some(array.value_unchecked(index).into()),
                }))
            }};
        }

        // TODO: insert types
        match T::data_type() {
            DataType::Utf8 => downcast_and_pack!(StringArray, Utf8),
            DataType::Boolean => downcast_and_pack!(BooleanArray, Boolean),
            DataType::UInt8 => downcast_and_pack!(UInt8Array, UInt8),
            DataType::UInt16 => downcast_and_pack!(UInt16Array, UInt16),
            DataType::UInt32 => downcast_and_pack!(UInt32Array, UInt32),
            DataType::UInt64 => downcast_and_pack!(UInt64Array, UInt64),
            DataType::Int8 => downcast_and_pack!(Int8Array, Int8),
            DataType::Int16 => downcast_and_pack!(Int16Array, Int16),
            DataType::Int32 => downcast_and_pack!(Int32Array, Int32),
            DataType::Int64 => downcast_and_pack!(Int64Array, Int64),
            DataType::Float32 => downcast_and_pack!(Float32Array, Float32),
            DataType::Float64 => downcast_and_pack!(Float64Array, Float64),

            DataType::Binary => {
                downcast_and_pack!(BinaryArray, Binary)
            }

            DataType::List(fs) => {
                let list_array = &*(arr as *const dyn Array as *const ListArray);
                let value = match list_array.is_null(index) {
                    true => None,
                    false => {
                        let nested_array = list_array.value(index);
                        let series = nested_array.into_series();
                        let scalar_vec = (0..series.len())
                            .map(|i| series.try_get(i))
                            .collect::<Result<Vec<_>>>()?;

                        Some(scalar_vec)
                    }
                };
                Ok(DataValue::List(value, fs.data_type().clone()))
            }

            DataType::Struct(_) => {
                let struct_array = &*(arr as *const dyn Array as *const StructArray);
                let nested_array = struct_array.column(index);
                let series = nested_array.clone().into_series();

                let scalar_vec = (0..nested_array.len())
                    .map(|i| series.try_get(i))
                    .collect::<Result<Vec<_>>>()?;
                Ok(DataValue::Struct(scalar_vec))
            }

            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "DataValue Error: Can't create a functions of array of type \"{:?}\"",
                other
            ))),
        }
    }

    // Apply BitAnd with the null masks and generate a new ArrayData
    pub fn apply_null_mask(&self, mask: impl AsRef<[u8]>) -> Result<Self> {
        let mask = mask.as_ref();
        if mask.len() != self.len() {
            return Err(ErrorCode::BadDataArrayLength(format!(
                "cannot apply null mask, size not matched, got: {}, expect: {}",
                mask.len(),
                self.len(),
            )));
        }
        let mut builder = BooleanBufferBuilder::new(mask.len());
        for b in mask.iter() {
            builder.append(*b > 0);
        }
        let buffer = builder.finish();
        let data = self.array.data();
        let bitmap = Bitmap::from(buffer);

        let bitmap_and = if let Some(b) = data.null_bitmap() {
            b.bitand(&bitmap)?
        } else {
            bitmap
        };

        let array_data = ArrayData::new(
            T::data_type().to_arrow(),
            data.len(),
            None,
            Some(bitmap_and.into_buffer()),
            data.offset(),
            data.buffers().to_owned(),
            data.child_data().to_owned(),
        );
        Ok(make_array(array_data).into())
    }
}

impl<T> DataArray<T>
where T: DFPrimitiveType
{
    /// Create a new DataArray by taking ownership of the AlignedVec. This operation is zero copy.
    pub fn new_from_aligned_vec(v: AlignedVec<T::Native>) -> Self {
        let array = v.into_primitive_array::<T>(None);
        Self::new(Arc::new(array))
    }

    /// Nullify values in slice with an existing null bitmap
    pub fn new_from_owned_with_null_bitmap(
        values: AlignedVec<T::Native>,
        buffer: Option<Buffer>,
    ) -> Self {
        let array = values.into_primitive_array::<T>(buffer);
        Self::new(Arc::new(array))
    }

    /// Get slices of the underlying arrow data.
    /// NOTE: null values should be taken into account by the user of these slices as they are handled
    /// separately

    pub fn data_views(
        &self,
    ) -> impl Iterator<Item = &T::Native> + '_ + Send + Sync + ExactSizeIterator + DoubleEndedIterator
    {
        self.downcast_ref().values().iter()
    }

    #[allow(clippy::wrong_self_convention)]
    pub fn into_no_null_iter(
        &self,
    ) -> impl Iterator<Item = T::Native> + '_ + Send + Sync + ExactSizeIterator + DoubleEndedIterator
    {
        // .copied was significantly slower in benchmark, next call did not inline?
        #[allow(clippy::map_clone)]
        self.data_views().map(|v| *v)
    }
}

impl DFListArray {
    pub fn sub_data_type(&self) -> DataType {
        match self.data_type() {
            DataType::List(sub_types) => sub_types.data_type().clone(),
            _ => unreachable!(),
        }
    }
}

impl<T> From<arrow_array::ArrayRef> for DataArray<T> {
    fn from(array: arrow_array::ArrayRef) -> Self {
        Self::new(array)
    }
}

impl<T> From<&arrow_array::ArrayRef> for DataArray<T> {
    fn from(array: &arrow_array::ArrayRef) -> Self {
        Self::new(array.clone())
    }
}

impl<T> Clone for DataArray<T> {
    fn clone(&self) -> Self {
        Self::new(self.array.clone())
    }
}

impl<T> std::fmt::Debug for DataArray<T>
where T: DFDataType
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DataArray<{:?}>", self.data_type())
    }
}
