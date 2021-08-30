// Copyright 2020 Datafuse Labs.
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

use common_exception::Result;

use crate::prelude::*;
use crate::series::Series;
use crate::utils::NoNull;

#[derive(Copy, Clone, Debug)]
pub enum FillNoneStrategy {
    /// previous value in array
    Forward,
    /// next value in array
    Backward,
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

impl<T> ArrayFull<T> for DFPrimitiveArray<T>
where T: DFPrimitiveType
{
    fn full(value: T, length: usize) -> Self
    where T: Copy {
        std::iter::repeat(value)
            .take(length)
            .map(|_| value)
            .into_iter()
            .collect_trusted::<NoNull<DFPrimitiveArray<T>>>()
            .into_inner()
    }
}

impl<T> ArrayFullNull for DFPrimitiveArray<T>
where T: DFPrimitiveType
{
    fn full_null(length: usize) -> Self {
        std::iter::repeat(None)
            .take(length)
            .collect_trusted::<Self>()
    }
}
impl ArrayFull<bool> for DFBooleanArray {
    fn full(value: bool, length: usize) -> Self {
        std::iter::repeat(value)
            .take(length)
            .collect_trusted::<DFBooleanArray>()
    }
}

impl ArrayFullNull for DFBooleanArray {
    fn full_null(length: usize) -> Self {
        std::iter::repeat(None)
            .take(length)
            .collect_trusted::<Self>()
    }
}

impl<'a> ArrayFull<&'a str> for DFUtf8Array {
    fn full(value: &'a str, length: usize) -> Self {
        let mut builder = Utf8ArrayBuilder::with_capacity(length * value.len());

        for _ in 0..length {
            builder.append_value(value);
        }
        builder.finish()
    }
}

impl ArrayFullNull for DFUtf8Array {
    fn full_null(length: usize) -> Self {
        (0..length)
            .map::<Option<String>, _>(|_| None)
            .collect::<Self>()
    }
}

impl ArrayFull<&Series> for DFListArray {
    fn full(_value: &Series, _length: usize) -> DFListArray {
        todo!()
    }
}

impl ArrayFullNull for DFListArray {
    fn full_null(_length: usize) -> DFListArray {
        todo!()
    }
}

impl ArrayFull<&[u8]> for DFBinaryArray {
    fn full(value: &[u8], length: usize) -> DFBinaryArray {
        let mut builder = BinaryArrayBuilder::with_capacity(length);
        for _ in 0..length {
            builder.append_value(value);
        }
        builder.finish()
    }
}

impl ArrayFullNull for DFBinaryArray {
    fn full_null(length: usize) -> DFBinaryArray {
        let mut builder = BinaryArrayBuilder::with_capacity(length);
        for _ in 0..length {
            builder.append_null();
        }
        builder.finish()
    }
}
