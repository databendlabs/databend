// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_arrow::arrow::datatypes::IntervalUnit;
use common_exception::Result;

use crate::arrays::ops::*;
use crate::arrays::*;
use crate::*;

impl<T> DataArrayBase<T> {
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

/// Dispatch the method call to the physical type and coerce back to logical type
macro_rules! physical_dispatch {
    ($s: expr, $method: ident, $($args:expr),*) => {{
        let data_type = $s.data_type();
        let phys_type = $s.physical_type();
        let s = $s.cast_with_data_type(&phys_type).unwrap();
        let s = s.$method($($args),*);

        // if the type is unchanged we return the original type
        if s.data_type() == &phys_type {
            s.cast_with_data_type(data_type).unwrap()
        }
        // else the change of type is part of the operation.
        else {
            s
        }
    }}
}

macro_rules! try_physical_dispatch {
    ($s: expr, $method: ident, $($args:expr),*) => {{
        let data_type = $s.data_type();
        let phys_type = $s.physical_type();
        let s = $s.cast_with_data_type(&phys_type).unwrap();
        let s = s.$method($($args),*)?;

        // if the type is unchanged we return the original type
        if s.data_type() == &phys_type {
            s.cast_with_data_type(data_type)
        }
        // else the change of type is part of the operation.
        else {
            Ok(s)
        }
    }}
}

macro_rules! opt_physical_dispatch {
    ($s: expr, $method: ident, $($args:expr),*) => {{
        let data_type = $s.data_type();
        let phys_type = $s.physical_type();
        let s = $s.cast_with_data_type(&phys_type).unwrap();
        let s = s.$method($($args),*)?;

        // if the type is unchanged we return the original type
        if s.data_type() == &phys_type {
            Some(s.cast_with_data_type(data_type).unwrap())
        }
        // else the change of type is part of the operation.
        else {
            Some(s)
        }
    }}
}

/// Same as physical dispatch, but doesnt care about return type
macro_rules! cast_and_apply {
    ($s: expr, $method: ident, $($args:expr),*) => {{
        let phys_type = $s.physical_type();
        let s = $s.cast_with_data_type(&phys_type).unwrap();
        s.$method($($args),*)
    }}
}

macro_rules! impl_dyn_arrays {
    ($da: ident) => {
        impl IntoDataArray for $da {
            fn into_array(self) -> DataArrayRef {
                Arc::new(DataArrayWrap(self))
            }
        }

        impl Debug for DataArrayWrap<$da> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
                write!(
                    f,
                    "Column: data_type: {:?}, size: {:?}",
                    self.data_type(),
                    self.len()
                )
            }
        }

        impl DataArray for DataArrayWrap<$da> {
            fn data_type(&self) -> DataType {
                self.0.data_type()
            }
            fn len(&self) -> usize {
                self.0.len()
            }

            fn is_empty(&self) -> bool {
                self.0.is_empty()
            }

            fn get_array_memory_size(&self) -> usize {
                self.0.get_array_memory_size()
            }

            fn slice(&self, offset: usize, length: usize) -> DataArrayRef {
                self.0.slice(offset, length).into_array()
            }

            fn cast_with_type(&self, data_type: &DataType) -> Result<DataArrayRef> {
                ArrayCast::cast_with_type(&self.0, data_type)
            }

            fn try_get(&self, index: usize) -> Result<DataValue> {
                unsafe { self.0.try_get(index) }
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
