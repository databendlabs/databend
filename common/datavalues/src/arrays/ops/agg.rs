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

use std::fmt::Debug;
use std::ops::Add;
use std::ops::AddAssign;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::compute::aggregate;
use common_arrow::arrow::types::simd::Simd;
use common_arrow::arrow::types::NativeType;
use common_exception::ErrorCode;
use common_exception::Result;
use num::cast::AsPrimitive;
use num::Num;
use num::NumCast;
use num::Zero;

use crate::prelude::*;

/// Same common aggregators
pub trait ArrayAgg: Debug {
    /// Aggregate the sum of the ChunkedArray.
    /// Returns `DataValue::Null` if the array is empty or only contains null values.
    fn sum(&self) -> Result<DataValue> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported aggregate operation: sum for {:?}",
            self,
        )))
    }

    fn min(&self) -> Result<DataValue> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported aggregate operation: sum for {:?}",
            self,
        )))
    }
    /// Returns the maximum value in the array, according to the natural order.
    /// Returns `DataValue::Null` if the array is empty or only contains null values.
    fn max(&self) -> Result<DataValue> {
        Err(ErrorCode::BadDataValueType(format!(
            "max operation not supported for {:?}",
            self,
        )))
    }

    // DataValue::Struct(index, value)
    fn arg_max(&self) -> Result<DataValue> {
        Err(ErrorCode::BadDataValueType(format!(
            "Sum operation not supported for {:?}",
            self,
        )))
    }

    fn arg_min(&self) -> Result<DataValue> {
        Err(ErrorCode::BadDataValueType(format!(
            "Sum operation not supported for {:?}",
            self,
        )))
    }
}

impl<T> ArrayAgg for DataArray<T>
where
    T: DFNumericType,
    T::Native: NativeType
        + Simd
        + PartialOrd
        + Num
        + NumCast
        + Zero
        + Into<DataValue>
        + AsPrimitive<<T::LargestType as DFPrimitiveType>::Native>,

    <T::LargestType as DFPrimitiveType>::Native: Into<DataValue> + AddAssign + Default,

    <T::Native as Simd>::Simd: Add<Output = <T::Native as Simd>::Simd>
        + aggregate::Sum<T::Native>
        + aggregate::SimdOrd<T::Native>,
    Option<T::Native>: Into<DataValue>,
{
    fn sum(&self) -> Result<DataValue> {
        let array = self.downcast_ref();
        // if largest type is self and there is nullable, we just use simd
        // sum is faster in auto vectorized than manual simd
        let null_count = self.null_count();
        if null_count > 0 && (T::SIZE == <T::LargestType as DFNumericType>::SIZE) {
            return Ok(match aggregate::sum(array) {
                Some(x) => x.into(),
                None => DataValue::from(self.data_type()),
            });
        }

        if self.is_empty() {
            return Ok(DataValue::from(self.data_type()));
        }

        let mut sum = <T::LargestType as DFPrimitiveType>::Native::default();
        if null_count == 0 {
            //fast path
            array.values().as_slice().iter().for_each(|f| {
                sum += f.as_();
            });
        } else if let Some(c) = array.validity() {
            array
                .values()
                .as_slice()
                .iter()
                .zip(c.into_iter())
                .for_each(|(f, v)| {
                    if v {
                        sum += f.as_();
                    }
                });
        }
        Ok(sum.into())
    }

    fn min(&self) -> Result<DataValue> {
        Ok(match aggregate::min_primitive(self.downcast_ref()) {
            Some(x) => x.into(),
            None => DataValue::from(self.data_type()),
        })
    }

    fn max(&self) -> Result<DataValue> {
        Ok(match aggregate::max_primitive(self.downcast_ref()) {
            Some(x) => x.into(),
            None => DataValue::from(self.data_type()),
        })
    }

    fn arg_min(&self) -> Result<DataValue> {
        if self.all_is_null() {
            return Ok(DataValue::Struct(vec![
                DataValue::UInt64(None),
                DataValue::from(self.data_type()),
            ]));
        }
        let value = self
            .into_no_null_iter()
            .enumerate()
            .reduce(|acc, (idx, val)| if acc.1 > val { (idx, val) } else { acc });

        Ok(match value {
            Some((index, value)) => DataValue::Struct(vec![(index as u64).into(), value.into()]),
            None => DataValue::Struct(vec![(0_u64).into(), DataValue::from(self.data_type())]),
        })
    }

    fn arg_max(&self) -> Result<DataValue> {
        if self.all_is_null() {
            return Ok(DataValue::Struct(vec![
                DataValue::UInt64(None),
                DataValue::from(self.data_type()),
            ]));
        }
        let value = self
            .into_no_null_iter()
            .enumerate()
            .reduce(|acc, (idx, val)| if acc.1 < val { (idx, val) } else { acc });

        Ok(match value {
            Some((index, value)) => DataValue::Struct(vec![(index as u64).into(), value.into()]),
            None => DataValue::Struct(vec![(0_u64).into(), DataValue::from(self.data_type())]),
        })
    }
}

impl ArrayAgg for DFBooleanArray {
    fn sum(&self) -> Result<DataValue> {
        if self.all_is_null() {
            return Ok(DataValue::Boolean(None));
        }
        let sum = self.downcast_iter().fold(0, |acc: u64, x| match x {
            Some(v) => acc + v as u64,
            None => acc,
        });

        Ok(sum.into())
    }

    fn min(&self) -> Result<DataValue> {
        if self.all_is_null() {
            return Ok(DataValue::Boolean(None));
        }

        Ok(match aggregate::min_boolean(self.downcast_ref()) {
            Some(x) => x.into(),
            None => DataValue::from(self.data_type()),
        })
    }

    fn max(&self) -> Result<DataValue> {
        if self.all_is_null() {
            return Ok(DataValue::Boolean(None));
        }

        Ok(match aggregate::max_boolean(self.downcast_ref()) {
            Some(x) => x.into(),
            None => DataValue::from(self.data_type()),
        })
    }

    fn arg_min(&self) -> Result<DataValue> {
        if self.all_is_null() {
            return Ok(DataValue::Struct(vec![
                DataValue::UInt64(None),
                DataValue::from(self.data_type()),
            ]));
        }
        let value = self
            .into_no_null_iter()
            .enumerate()
            .reduce(|acc, (idx, val)| {
                if acc.1 as u32 > val as u32 {
                    (idx, val)
                } else {
                    acc
                }
            });

        Ok(match value {
            Some((index, value)) => DataValue::Struct(vec![(index as u64).into(), value.into()]),
            None => DataValue::Struct(vec![(0_u64).into(), DataValue::Boolean(None)]),
        })
    }

    fn arg_max(&self) -> Result<DataValue> {
        if self.all_is_null() {
            return Ok(DataValue::Struct(vec![
                DataValue::UInt64(None),
                DataValue::from(self.data_type()),
            ]));
        }
        let value = self
            .into_no_null_iter()
            .enumerate()
            .reduce(|acc, (idx, val)| {
                if acc.1 as u32 >= val as u32 {
                    acc
                } else {
                    (idx, val)
                }
            });

        Ok(match value {
            Some((index, value)) => DataValue::Struct(vec![(index as u64).into(), value.into()]),
            None => DataValue::Struct(vec![(0_u64).into(), DataValue::Boolean(None)]),
        })
    }
}

impl ArrayAgg for DFUtf8Array {
    fn min(&self) -> Result<DataValue> {
        if self.all_is_null() {
            return Ok(DataValue::Utf8(None));
        }

        Ok(match aggregate::min_string(self.downcast_ref()) {
            Some(x) => x.into(),
            None => DataValue::from(self.data_type()),
        })
    }

    fn max(&self) -> Result<DataValue> {
        if self.all_is_null() {
            return Ok(DataValue::Utf8(None));
        }

        Ok(match aggregate::max_string(self.downcast_ref()) {
            Some(x) => x.into(),
            None => DataValue::from(self.data_type()),
        })
    }

    fn arg_max(&self) -> Result<DataValue> {
        if self.all_is_null() {
            return Ok(DataValue::Struct(vec![
                (0_u64).into(),
                DataValue::Utf8(None),
            ]));
        }
        let value = self
            .into_no_null_iter()
            .enumerate()
            .reduce(
                |acc, (idx, val)| {
                    if acc.1 >= val {
                        acc
                    } else {
                        (idx, val)
                    }
                },
            );

        Ok(match value {
            Some((index, value)) => DataValue::Struct(vec![(index as u64).into(), value.into()]),
            None => DataValue::Struct(vec![(0_u64).into(), DataValue::Utf8(None)]),
        })
    }

    fn arg_min(&self) -> Result<DataValue> {
        if self.all_is_null() {
            return Ok(DataValue::Struct(vec![
                (0_u64).into(),
                DataValue::Utf8(None),
            ]));
        }
        let value = self
            .into_no_null_iter()
            .enumerate()
            .reduce(|acc, (idx, val)| if acc.1 < val { acc } else { (idx, val) });

        Ok(match value {
            Some((index, value)) => DataValue::Struct(vec![(index as u64).into(), value.into()]),
            None => DataValue::Struct(vec![(0_u64).into(), DataValue::Utf8(None)]),
        })
    }
}

impl ArrayAgg for DFListArray {}

impl ArrayAgg for DFBinaryArray {}

impl ArrayAgg for DFNullArray {}

impl ArrayAgg for DFStructArray {}
