// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_arrow::arrow::array::ArrayRef;
use common_exception::Result;

use crate::arrays::*;
use crate::prelude::*;
use crate::series::wrap::SeriesWrap;
use crate::series::*;
use crate::*;

impl<T> DataArray<T> {
    /// get the physical memory type of a date type
    fn physical_type(&self) -> DataType {
        match self.data_type() {
            DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Interval(IntervalUnit::DayTime) => DataType::Int64,
            DataType::Date32 | DataType::Interval(IntervalUnit::YearMonth) => DataType::Int32,
            dt => panic!("already a physical type: {:?}", dt),
        }
    }
}

macro_rules! try_physical_dispatch {
    ($s: expr, $method: ident, $($args:expr),*) => {{
        let data_type = $s.data_type();
        let phys_type = $s.physical_type();
        let s = $s.cast_with_type(&phys_type).unwrap();
        let s = s.$method($($args),*)?;

        // if the type is unchanged we return the original type
        if s.data_type() == phys_type {
            s.cast_with_type(&data_type)
        }
        // else the change of type is part of the operation.
        else {
            Ok(s)
        }
    }}
}

macro_rules! try_physical_dispatch_vec {
    ($s: expr, $method: ident, $($args:expr),*) => {{
        let data_type = $s.data_type();
        let phys_type = $s.physical_type();
        let s = $s.cast_with_type(&phys_type).unwrap();

        let results = s.$method($($args),*)?;

        if results.is_empty() {
           return Ok(vec![]);
        }

        let mut cast_results = Vec::with_capacity(results.len());
        for result in results {
            cast_results.push(result.cast_with_type(&data_type)?);
        }

        Ok(cast_results)
    }}
}
/// Same as physical dispatch, but doesnt care about return type
macro_rules! cast_and_apply {
    ($s: expr, $method: ident, $($args:expr),*) => {{
        let phys_type = $s.physical_type();
        let s = $s.cast_with_type(&phys_type).unwrap();
        s.$method($($args),*)
    }}
}

macro_rules! impl_dyn_arrays {
    ($da: ident) => {
        impl IntoSeries for $da {
            fn into_series(self) -> Series {
                Series(Arc::new(SeriesWrap(self)))
            }
        }

        impl Debug for SeriesWrap<$da> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
                write!(
                    f,
                    "Column: data_type: {:?}, size: {:?}",
                    self.data_type(),
                    self.len()
                )
            }
        }

        impl SeriesTrait for SeriesWrap<$da> {
            fn data_type(&self) -> DataType {
                self.0.data_type()
            }
            fn len(&self) -> usize {
                self.0.len()
            }

            fn is_empty(&self) -> bool {
                self.0.is_empty()
            }

            fn is_null(&self, row: usize) -> bool {
                self.0.is_null(row)
            }

            fn null_count(&self) -> usize {
                self.0.null_count()
            }

            fn get_array_memory_size(&self) -> usize {
                self.0.get_array_memory_size()
            }

            fn get_array_ref(&self) -> ArrayRef {
                self.0.get_array_ref()
            }

            fn to_values(&self) -> Result<Vec<DataValue>> {
                self.0.to_values()
            }

            fn slice(&self, offset: usize, length: usize) -> Series {
                self.0.slice(offset, length).into_series()
            }

            unsafe fn equal_element(
                &self,
                idx_self: usize,
                idx_other: usize,
                other: &Series,
            ) -> bool {
                self.0.equal_element(idx_self, idx_other, other)
            }

            fn cast_with_type(&self, data_type: &DataType) -> Result<Series> {
                ArrayCast::cast_with_type(&self.0, data_type)
            }

            fn try_get(&self, index: usize) -> Result<DataValue> {
                unsafe { self.0.try_get(index) }
            }

            fn vec_hash(&self, hasher: DFHasher) -> Result<DFUInt64Array> {
                cast_and_apply!(self, vec_hash, hasher)
            }

            fn group_hash(&self, ptr: usize, step: usize) -> Result<()> {
                cast_and_apply!(self, group_hash, ptr, step)
            }

            fn subtract(&self, rhs: &Series) -> Result<Series> {
                try_physical_dispatch!(self, subtract, rhs)
            }
            fn add_to(&self, rhs: &Series) -> Result<Series> {
                try_physical_dispatch!(self, add_to, rhs)
            }
            fn multiply(&self, rhs: &Series) -> Result<Series> {
                try_physical_dispatch!(self, multiply, rhs)
            }
            fn divide(&self, rhs: &Series) -> Result<Series> {
                try_physical_dispatch!(self, divide, rhs)
            }
            fn remainder(&self, rhs: &Series, dtype: &DataType) -> Result<Series> {
                try_physical_dispatch!(self, remainder, rhs, dtype)
            }
            fn negative(&self) -> Result<Series> {
                try_physical_dispatch!(self, negative,)
            }

            fn sum(&self) -> Result<DataValue> {
                cast_and_apply!(self, sum,)
            }

            fn max(&self) -> Result<DataValue> {
                cast_and_apply!(self, max,)
            }
            fn min(&self) -> Result<DataValue> {
                cast_and_apply!(self, min,)
            }

            fn arg_max(&self) -> Result<DataValue> {
                cast_and_apply!(self, arg_max,)
            }
            fn arg_min(&self) -> Result<DataValue> {
                cast_and_apply!(self, arg_min,)
            }

            fn take_iter(&self, iter: &mut dyn Iterator<Item = usize>) -> Result<Series> {
                try_physical_dispatch!(self, take_iter, iter.into())
            }

            unsafe fn take_iter_unchecked(
                &self,
                iter: &mut dyn Iterator<Item = usize>,
            ) -> Result<Series> {
                try_physical_dispatch!(self, take_iter_unchecked, iter.into())
            }

            /// scatter the arrays by indices, the size of indices must be equal to the size of array
            unsafe fn scatter_unchecked(
                &self,
                indices: &mut dyn Iterator<Item = u64>,
                scattered_size: usize,
            ) -> Result<Vec<Series>> {
                try_physical_dispatch_vec!(self, scatter_unchecked, indices, scattered_size)
            }
        }
    };
}

impl_dyn_arrays!(DFDate32Array);
impl_dyn_arrays!(DFDate64Array);

impl_dyn_arrays!(DFTimestampSecondArray);
impl_dyn_arrays!(DFTimestampMillisecondArray);
impl_dyn_arrays!(DFTimestampMicrosecondArray);
impl_dyn_arrays!(DFTimestampNanosecondArray);
impl_dyn_arrays!(DFIntervalYearMonthArray);
impl_dyn_arrays!(DFIntervalDayTimeArray);
