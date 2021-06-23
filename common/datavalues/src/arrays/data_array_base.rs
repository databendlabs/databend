// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryFrom;
use std::marker::PhantomData;
use std::sync::Arc;

use common_arrow::arrow;
use common_arrow::arrow::array::*;
use common_arrow::arrow::array::{self as arrow_array};
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::datatypes::IntervalUnit;
use common_arrow::arrow::datatypes::TimeUnit;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::arrays::ops::ArrayCast;
use crate::arrays::DataArrayRef;
use crate::arrays::DataArrayWrap;
use crate::data_df_type::*;
use crate::vec::AlignedVec;
use crate::DataType;
use crate::DataValue;
use crate::*;

/// DataArrayBase is generic struct which implements DataArray
pub struct DataArrayBase<T> {
    pub array: arrow_array::ArrayRef,
    t: PhantomData<T>,
}

impl<T> DataArrayBase<T> {
    pub fn data_type(&self) -> DataType {
        DataType::try_from(self.array.data_type()).unwrap()
    }

    pub fn len(&self) -> usize {
        self.array.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn null_count(&self) -> usize {
        self.array.null_count()
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
}

impl<T> DataArrayBase<T>
where T: DFDataType
{
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
            DataType::Utf8 => downcast_and_pack!(LargeStringArray, Utf8),
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
            DataType::Date32 => downcast_and_pack!(Date32Array, Date32),
            DataType::Date64 => downcast_and_pack!(Date64Array, Date64),

            DataType::Timestamp(TimeUnit::Second, _) => {
                downcast_and_pack!(TimestampSecondArray, TimestampSecond)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                downcast_and_pack!(TimestampMillisecondArray, TimestampMillisecond)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                downcast_and_pack!(TimestampMicrosecondArray, TimestampMicrosecond)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                downcast_and_pack!(TimestampNanosecondArray, TimestampNanosecond)
            }

            DataType::Interval(IntervalUnit::YearMonth) => {
                downcast_and_pack!(IntervalYearMonthArray, IntervalYearMonth)
            }

            DataType::Interval(IntervalUnit::DayTime) => {
                downcast_and_pack!(IntervalDayTimeArray, IntervalDayTime)
            }

            DataType::List(_) => {
                todo!();
            }
            _ => unimplemented!(),
        }
    }
}

impl<T> DataArrayBase<T>
where T: DFNumericType
{
    /// Create a new DataArrayBase by taking ownership of the AlignedVec. This operation is zero copy.
    pub fn new_from_aligned_vec(name: &str, v: AlignedVec<T::Native>) -> Self {
        let arr = v.into_primitive_array::<T>(None);
        Self {
            array: Arc::new(arr),
            t: PhantomData,
        }
    }

    /// Nullify values in slice with an existing null bitmap
    pub fn new_from_owned_with_null_bitmap(
        name: &str,
        values: AlignedVec<T::Native>,
        buffer: Option<Buffer>,
    ) -> Self {
        let array = Arc::new(values.into_primitive_array::<T>(buffer));
        Self {
            array,
            t: PhantomData,
        }
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

impl<T> From<arrow_array::ArrayRef> for DataArrayBase<T> {
    fn from(array: arrow_array::ArrayRef) -> Self {
        Self {
            array,
            t: PhantomData::<T>,
        }
    }
}

impl<T> From<&arrow_array::ArrayRef> for DataArrayBase<T> {
    fn from(array: &arrow_array::ArrayRef) -> Self {
        Self {
            array: array.clone(),
            t: PhantomData::<T>,
        }
    }
}

impl<T> Clone for DataArrayBase<T> {
    fn clone(&self) -> Self {
        Self {
            array: self.array.clone(),
            t: PhantomData,
        }
    }
}
