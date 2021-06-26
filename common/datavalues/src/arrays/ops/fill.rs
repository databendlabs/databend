use common_exception::Result;

use crate::arrays::builders::Utf8ArrayBuilder;
use crate::arrays::DataArray;
use crate::series::Series;
use crate::utils::NoNull;
use crate::DFBooleanArray;
use crate::DFListArray;
use crate::DFPrimitiveType;
use crate::DFStringArray;

// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[derive(Copy, Clone, Debug)]
pub enum FillNoneStrategy {
    /// previous value in array
    Backward,
    /// next value in array
    Forward,
    /// mean value of array
    Mean,
    /// minimal value in array
    Min,
    /// maximum value in array
    Max,
    /// replace with the value zero
    Zero,
    /// replace with the value one
    One,
    /// replace with the maximum value of that data type
    MaxBound,
    /// replace with the minimal value of that data type
    MinBound,
}

/// Replace None values with various strategies
pub trait ArrayFillNone {
    /// Replace None values with one of the following strategies:
    /// * Forward fill (replace None with the previous value)
    /// * Backward fill (replace None with the next value)
    /// * Mean fill (replace None with the mean of the whole array)
    /// * Min fill (replace None with the minimum of the whole array)
    /// * Max fill (replace None with the maximum of the whole array)
    fn fill_none(&self, strategy: FillNoneStrategy) -> Result<Self>
    where Self: Sized;
}
/// Replace None values with a value
pub trait ArrayFillNoneValue<T> {
    /// Replace None values with a give value `T`.
    fn fill_none_with_value(&self, value: T) -> Result<Self>
    where Self: Sized;
}

/// Fill a DataArray with one value.
pub trait ArrayFull<T> {
    /// Create a DataArray with a single value.
    fn full(value: T, length: usize) -> Self
    where Self: std::marker::Sized;
}

pub trait ArrayFullNull {
    fn full_null(_length: usize) -> Self
    where Self: std::marker::Sized;
}

impl<T> ArrayFull<T::Native> for DataArray<T>
where T: DFPrimitiveType
{
    fn full(value: T::Native, length: usize) -> Self
    where T::Native: Copy {
        let ca = (0..length)
            .map(|_| value)
            .collect::<NoNull<DataArray<T>>>()
            .into_inner();
        ca
    }
}

impl<T> ArrayFullNull for DataArray<T>
where T: DFPrimitiveType
{
    fn full_null(length: usize) -> Self {
        let ca = (0..length).map(|_| None).collect::<Self>();
        ca
    }
}
impl ArrayFull<bool> for DFBooleanArray {
    fn full(value: bool, length: usize) -> Self {
        let ca = (0..length).map(|_| value).collect::<DFBooleanArray>();
        ca
    }
}

impl ArrayFullNull for DFBooleanArray {
    fn full_null(length: usize) -> Self {
        let ca = (0..length).map(|_| None).collect::<Self>();
        ca
    }
}

impl<'a> ArrayFull<&'a str> for DFStringArray {
    fn full(value: &'a str, length: usize) -> Self {
        let mut builder = Utf8ArrayBuilder::new(length, length * value.len());

        for _ in 0..length {
            builder.append_value(value);
        }
        builder.finish()
    }
}

impl ArrayFullNull for DFStringArray {
    fn full_null(length: usize) -> Self {
        let mut ca = (0..length)
            .map::<Option<String>, _>(|_| None)
            .collect::<Self>();
        ca
    }
}

impl ArrayFull<&Series> for DFListArray {
    fn full(value: &Series, length: usize) -> DFListArray {
        todo!()
    }
}

impl ArrayFullNull for DFListArray {
    fn full_null(length: usize) -> DFListArray {
        todo!()
    }
}
