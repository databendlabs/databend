// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;

use common_arrow::arrow::compute;
use common_exception::ErrorCode;
use common_exception::Result;
use num::Num;
use num::NumCast;
use num::Zero;

use crate::prelude::*;
use crate::DFBinaryArray;
use crate::DFBooleanArray;
use crate::DFListArray;
use crate::DFNullArray;
use crate::DFNumericType;
use crate::DFStructArray;
use crate::DFUtf8Array;

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
    T::Native: PartialOrd + Num + NumCast + Zero + Into<DataValue>,
    Option<T::Native>: Into<DataValue>,
{
    fn sum(&self) -> Result<DataValue> {
        Ok(match compute::sum(self.downcast_ref()) {
            Some(x) => x.into(),
            None => DataValue::from(self.data_type()),
        })
    }

    fn min(&self) -> Result<DataValue> {
        Ok(match compute::min(self.downcast_ref()) {
            Some(x) => x.into(),
            None => DataValue::from(self.data_type()),
        })
    }

    fn max(&self) -> Result<DataValue> {
        Ok(match compute::max(self.downcast_ref()) {
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
        let sum = self.into_iter().fold(0, |acc: u32, x| match x {
            Some(v) => acc + v as u32,
            None => acc,
        });

        Ok(sum.into())
    }

    fn min(&self) -> Result<DataValue> {
        if self.all_is_null() {
            return Ok(DataValue::Boolean(None));
        }
        Ok(min_max_boolean_helper(self, true).into())
    }

    fn max(&self) -> Result<DataValue> {
        if self.all_is_null() {
            return Ok(DataValue::Boolean(None));
        }
        Ok(min_max_boolean_helper(self, false).into())
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

fn min_max_boolean_helper(ca: &DFBooleanArray, min: bool) -> u32 {
    ca.into_iter().fold(0, |acc: u32, x| match x {
        Some(v) => {
            let v = v as u32;
            if min {
                if acc < v {
                    acc
                } else {
                    v
                }
            } else if acc > v {
                acc
            } else {
                v
            }
        }
        None => acc,
    })
}

impl ArrayAgg for DFUtf8Array {
    fn min(&self) -> Result<DataValue> {
        if self.all_is_null() {
            return Ok(DataValue::Utf8(None));
        }

        let d = self.into_iter().reduce(|acc, x| match (acc, x) {
            (None, _) => x,
            (Some(v_acc), Some(v)) => {
                if v_acc < v {
                    acc
                } else {
                    x
                }
            }
            _ => acc,
        });

        if let Some(Some(v)) = d {
            return Ok(DataValue::Utf8(Some(v.to_string())));
        }
        Ok(DataValue::Utf8(None))
    }

    fn max(&self) -> Result<DataValue> {
        if self.all_is_null() {
            return Ok(DataValue::Utf8(None));
        }

        let d = self.into_iter().reduce(|acc, x| match (acc, x) {
            (None, _) => x,
            (Some(v_acc), Some(v)) => {
                if v_acc > v {
                    acc
                } else {
                    x
                }
            }
            _ => acc,
        });

        if let Some(Some(v)) = d {
            return Ok(DataValue::Utf8(Some(v.to_string())));
        }
        Ok(DataValue::Utf8(None))
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
