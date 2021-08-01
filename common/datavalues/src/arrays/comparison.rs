// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt::Debug;
use std::sync::Arc;

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::compute::comparison::boolean_compare_scalar;
use common_arrow::arrow::compute::comparison::compare;
use common_arrow::arrow::compute::comparison::primitive_compare_scalar;
use common_arrow::arrow::compute::comparison::utf8_compare_scalar;
use common_arrow::arrow::compute::comparison::Operator;
use common_arrow::arrow::compute::like;
use common_exception::ErrorCode;
use common_exception::Result;
use num::Num;
use num::NumCast;

use super::DataArray;
use crate::arrays::*;
use crate::series::Series;
use crate::*;

pub trait NumComp: Num + NumCast + PartialOrd {}

impl NumComp for f32 {}
impl NumComp for f64 {}
impl NumComp for i8 {}
impl NumComp for i16 {}
impl NumComp for i32 {}
impl NumComp for i64 {}
impl NumComp for u8 {}
impl NumComp for u16 {}
impl NumComp for u32 {}
impl NumComp for u64 {}

pub trait ArrayCompare<Rhs>: Debug {
    /// Check for equality.
    fn eq(&self, _rhs: Rhs) -> Result<DFBooleanArray> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare operation: eq for {:?}",
            self,
        )))
    }

    /// Check for inequality.
    fn neq(&self, _rhs: Rhs) -> Result<DFBooleanArray> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare operation: neq for {:?}",
            self,
        )))
    }

    /// Greater than comparison.
    fn gt(&self, _rhs: Rhs) -> Result<DFBooleanArray> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare operation: gt for {:?}",
            self,
        )))
    }

    /// Greater than or equal comparison.
    fn gt_eq(&self, _rhs: Rhs) -> Result<DFBooleanArray> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare operation: gt_eq for {:?}",
            self,
        )))
    }

    /// Less than comparison.
    fn lt(&self, _rhs: Rhs) -> Result<DFBooleanArray> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare operation: lt for {:?}",
            self,
        )))
    }

    /// Less than or equal comparison
    fn lt_eq(&self, _rhs: Rhs) -> Result<DFBooleanArray> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare operation: lt_eq for {:?}",
            self,
        )))
    }

    fn like(&self, _rhs: Rhs) -> Result<DFBooleanArray> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare operation: like for {:?}",
            self,
        )))
    }

    fn nlike(&self, _rhs: Rhs) -> Result<DFBooleanArray> {
        Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare operation: nlike for {:?}",
            self,
        )))
    }
}

impl<T> DataArray<T>
where
    T: DFNumericType,
    T::Native: NumComp,
{
    /// First ensure that the Arrays of lhs and rhs match and then iterates over the Arrays and applies
    /// the comparison operator.
    fn comparison(&self, rhs: &DataArray<T>, op: Operator) -> Result<DFBooleanArray> {
        let (lhs, rhs) = (self.array.as_ref(), rhs.array.as_ref());
        let array = Arc::new(compare(lhs, rhs, op)?) as ArrayRef;
        Ok(array.into())
    }

    fn comparison_scalar(&self, rhs: T::Native, op: Operator) -> Result<DFBooleanArray> {
        let array = Arc::new(primitive_compare_scalar(self.as_ref(), rhs, op)?) as ArrayRef;
        Ok(array.into())
    }
}

macro_rules! impl_cmp_common {
    ($self:ident, $rhs:ident, $kop:ident, $neg_func:tt) => {{
        if $self.len() == $rhs.len() {
            $self.comparison($rhs, Operator::$kop)
        } else if $rhs.len() == 1 {
            if let Some(value) = $rhs.get(0) {
                $self.comparison_scalar(value, Operator::$kop)
            } else {
                Ok(DFBooleanArray::full(false, $self.len()))
            }
        } else if $self.len() == 1 {
            $rhs.$neg_func($self)
        } else {
            unreachable!()
        }
    }};
}

impl<T> ArrayCompare<&DataArray<T>> for DataArray<T>
where
    T: DFNumericType,
    T::Native: NumComp,
{
    fn eq(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        impl_cmp_common! {self, rhs, Eq, eq}
    }

    fn neq(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        impl_cmp_common! {self, rhs, Neq, neq}
    }

    fn gt(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        impl_cmp_common! {self, rhs, Gt, lt_eq}
    }

    fn gt_eq(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        impl_cmp_common! {self, rhs, GtEq, lt}
    }

    fn lt(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        impl_cmp_common! {self, rhs, Lt, gt_eq}
    }

    fn lt_eq(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        impl_cmp_common! {self, rhs, LtEq, gt}
    }
}

impl DFBooleanArray {
    /// First ensure that the Arrays of lhs and rhs match and then iterates over the Arrays and applies
    /// the comparison operator.
    fn comparison(&self, rhs: &DFBooleanArray, op: Operator) -> Result<DFBooleanArray> {
        let (lhs, rhs) = (self.array.as_ref(), rhs.array.as_ref());
        let array = Arc::new(compare(lhs, rhs, op)?) as ArrayRef;
        Ok(array.into())
    }

    fn comparison_scalar(&self, rhs: bool, op: Operator) -> Result<DFBooleanArray> {
        let array = Arc::new(boolean_compare_scalar(self.as_ref(), rhs, op)?) as ArrayRef;
        Ok(array.into())
    }
}

impl ArrayCompare<&DFBooleanArray> for DFBooleanArray {
    fn eq(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        impl_cmp_common! {self, rhs, Eq, eq}
    }

    fn neq(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        impl_cmp_common! {self, rhs, Neq, neq}
    }

    fn gt(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        impl_cmp_common! {self, rhs, Gt, lt_eq}
    }

    fn gt_eq(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        impl_cmp_common! {self, rhs, GtEq, lt}
    }

    fn lt(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        impl_cmp_common! {self, rhs, Lt, gt_eq}
    }

    fn lt_eq(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        impl_cmp_common! {self, rhs, LtEq, gt}
    }
}

impl DFUtf8Array {
    fn comparison(&self, rhs: &DFUtf8Array, op: Operator) -> Result<DFBooleanArray> {
        let (lhs, rhs) = (self.array.as_ref(), rhs.array.as_ref());
        let array = Arc::new(compare(lhs, rhs, op)?) as ArrayRef;
        Ok(array.into())
    }

    fn comparison_scalar(&self, rhs: &str, op: Operator) -> Result<DFBooleanArray> {
        let array = Arc::new(utf8_compare_scalar(self.as_ref(), rhs, op)) as ArrayRef;
        Ok(array.into())
    }

    // pub fn like_utf8<O: Offset>(lhs: &Utf8Array<O>, rhs: &Utf8Array<O>)
    fn like(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        let array = like::like_utf8(self.downcast_ref(), rhs.downcast_ref())?;
        Ok(DFBooleanArray::from_arrow_array(array))
    }

    fn like_scalar(&self, rhs: &str) -> Result<DFBooleanArray> {
        let array = like::like_utf8_scalar(self.downcast_ref(), rhs)?;
        Ok(DFBooleanArray::from_arrow_array(array))
    }

    fn nlike(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        let array = like::nlike_utf8(self.downcast_ref(), rhs.downcast_ref())?;
        Ok(DFBooleanArray::from_arrow_array(array))
    }

    fn nlike_scalar(&self, rhs: &str) -> Result<DFBooleanArray> {
        let array = like::nlike_utf8_scalar(self.downcast_ref(), rhs)?;
        Ok(DFBooleanArray::from_arrow_array(array))
    }
}

macro_rules! impl_like_utf8 {
    ($self:ident, $rhs:ident, $op:ident, $scalar_op:ident) => {{
        // broadcast
        if $rhs.len() == 1 {
            if let Some(value) = $rhs.get(0) {
                $self.$scalar_op(value)
            } else {
                Ok(DFBooleanArray::full(false, $self.len()))
            }
        } else if $self.len() == 1 {
            if let Some(value) = $self.get(0) {
                let it = (0..$rhs.len()).map(|_| value);
                let left = DFUtf8Array::new_from_iter(it);
                left.$op($rhs)
            } else {
                Ok(DFBooleanArray::full(false, $rhs.len()))
            }
        } else {
            $self.$op($rhs)
        }
    }};
}

impl ArrayCompare<&DFUtf8Array> for DFUtf8Array {
    fn eq(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_cmp_common! {self, rhs, Eq, eq}
    }

    fn neq(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_cmp_common! {self, rhs, Neq, neq}
    }

    fn gt(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_cmp_common! {self, rhs, Gt, lt_eq}
    }

    fn gt_eq(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_cmp_common! {self, rhs, GtEq, lt}
    }

    fn lt(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_cmp_common! {self, rhs, Lt, gt_eq}
    }

    fn lt_eq(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_cmp_common! {self, rhs, LtEq, gt}
    }

    fn like(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_like_utf8! {self, rhs, like, like_scalar}
    }

    fn nlike(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_like_utf8! {self, rhs, nlike, nlike_scalar}
    }
}

impl ArrayCompare<&DFNullArray> for DFNullArray {}

impl ArrayCompare<&DFBinaryArray> for DFBinaryArray {}

impl ArrayCompare<&DFStructArray> for DFStructArray {}

macro_rules! impl_cmp_numeric_utf8_list {
    ($self:ident, $rhs:ident, $cmp_method:ident) => {{
        match ($self.null_count(), $rhs.null_count()) {
            (0, 0) => $self
                .into_no_null_iter()
                .zip($rhs.into_no_null_iter())
                .map(|(left, right)| left.$cmp_method(&right))
                .collect(),
            (0, _) => $self
                .into_no_null_iter()
                .zip($rhs.into_iter())
                .map(|(left, opt_right)| opt_right.map(|right| left.$cmp_method(&right)))
                .collect(),
            (_, 0) => $self
                .into_iter()
                .zip($rhs.into_no_null_iter())
                .map(|(opt_left, right)| opt_left.map(|left| left.$cmp_method(&right)))
                .collect(),
            (_, _) => $self
                .into_iter()
                .zip($rhs.into_iter())
                .map(|(opt_left, opt_right)| match (opt_left, opt_right) {
                    (None, None) => None,
                    (None, Some(_)) => None,
                    (Some(_), None) => None,
                    (Some(left), Some(right)) => Some(left.$cmp_method(&right)),
                })
                .collect(),
        }
    }};
}

impl ArrayCompare<&DFListArray> for DFListArray {
    fn eq(&self, rhs: &DFListArray) -> Result<DFBooleanArray> {
        Ok(impl_cmp_numeric_utf8_list!(self, rhs, series_equal))
    }

    fn neq(&self, rhs: &DFListArray) -> Result<DFBooleanArray> {
        self.eq(rhs)?.not()
    }
}

// private
pub(crate) trait ArrayEqualElement {
    /// Check if element in self is equal to element in other, assumes same data_types
    ///
    /// # Safety
    ///
    /// No type checks.
    unsafe fn equal_element(&self, _idx_self: usize, _idx_other: usize, _other: &Series) -> bool {
        unimplemented!()
    }
}

impl<T> ArrayEqualElement for DataArray<T>
where
    T: DFNumericType,
    T::Native: PartialEq,
{
    unsafe fn equal_element(&self, idx_self: usize, idx_other: usize, other: &Series) -> bool {
        let ca_other = other.as_ref().as_ref();
        debug_assert!(self.data_type() == other.data_type());
        let ca_other = &*(ca_other as *const DataArray<T>);
        // Should be get and not get_unchecked, because there could be nulls
        self.get(idx_self) == ca_other.get(idx_other)
    }
}

impl ArrayEqualElement for DFBooleanArray {
    unsafe fn equal_element(&self, idx_self: usize, idx_other: usize, other: &Series) -> bool {
        let ca_other = other.as_ref().as_ref();
        debug_assert!(self.data_type() == other.data_type());
        let ca_other = &*(ca_other as *const DFBooleanArray);
        self.get(idx_self) == ca_other.get(idx_other)
    }
}

impl ArrayEqualElement for DFUtf8Array {
    unsafe fn equal_element(&self, idx_self: usize, idx_other: usize, other: &Series) -> bool {
        let ca_other = other.as_ref().as_ref();
        debug_assert!(self.data_type() == other.data_type());
        let ca_other = &*(ca_other as *const DFUtf8Array);
        self.get(idx_self) == ca_other.get(idx_other)
    }
}

impl ArrayEqualElement for DFListArray {}

impl ArrayEqualElement for DFNullArray {}

impl ArrayEqualElement for DFStructArray {}

impl ArrayEqualElement for DFBinaryArray {}
