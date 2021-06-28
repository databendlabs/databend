// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::ops::BitAnd;
use std::ops::BitOr;
use std::ops::Not;
use std::sync::Arc;

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::BooleanArray;
use common_arrow::arrow::array::LargeStringArray;
use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::compute;
use common_arrow::arrow::compute::kernels::comparison;
use common_arrow::arrow::compute::*;
use common_exception::Result;
use num::Num;
use num::NumCast;
use num::ToPrimitive;

use super::DataArray;
use crate::arrays::*;
use crate::series::Series;
use crate::utils::NoNull;
use crate::*;

pub trait ArrayCompare<Rhs> {
    /// Check for equality and regard missing values as equal.
    fn eq_missing(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        unimplemented!()
    }

    /// Check for equality.
    fn eq(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        unimplemented!()
    }

    /// Check for inequality.
    fn neq(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        unimplemented!()
    }

    /// Greater than comparison.
    fn gt(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        unimplemented!()
    }

    /// Greater than or equal comparison.
    fn gt_eq(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        unimplemented!()
    }

    /// Less than comparison.
    fn lt(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        unimplemented!()
    }

    /// Less than or equal comparison
    fn lt_eq(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        unimplemented!()
    }
}

impl<T> DataArray<T>
where T: DFNumericType
{
    /// First ensure that the Arrays of lhs and rhs match and then iterates over the Arrays and applies
    /// the comparison operator.
    fn comparison(
        &self,
        rhs: &DataArray<T>,
        operator: impl Fn(
            &PrimitiveArray<T>,
            &PrimitiveArray<T>,
        ) -> common_arrow::arrow::error::Result<BooleanArray>,
    ) -> Result<DFBooleanArray> {
        let array = Arc::new(operator(self.downcast_ref(), rhs.downcast_ref())?) as ArrayRef;
        Ok(array.into())
    }
}

macro_rules! impl_eq_missing {
    ($self:ident, $rhs:ident) => {{
        match ($self.null_count(), $rhs.null_count()) {
            (0, 0) => $self
                .into_no_null_iter()
                .zip($rhs.into_no_null_iter())
                .map(|(opt_a, opt_b)| opt_a == opt_b)
                .collect(),
            (_, _) => $self
                .downcast_iter()
                .zip($rhs.downcast_iter())
                .map(|(opt_a, opt_b)| opt_a == opt_b)
                .collect(),
        }
    }};
}

macro_rules! impl_cmp_numeric_utf8 {
    ($self:ident, $rhs:ident, $op:ident, $kop:ident, $operand:tt) => {{
        // broadcast
        if $rhs.len() == 1 {
            if let Some(value) = $rhs.get(0) {
                $self.$op(value)
            } else {
                Ok(DFBooleanArray::full(false, $self.len()))
            }
        } else if $self.len() == 1 {
            if let Some(value) = $self.get(0) {
                $rhs.$op(value)
            } else {
                Ok(DFBooleanArray::full(false, $self.len()))
            }
        } else if $self.len() == $rhs.len() {
            $self.comparison($rhs, comparison::$kop)
        } else {
            Ok(apply_operand_on_array_by_iter!($self, $rhs, $operand))
        }
    }};
}

impl<T> ArrayCompare<&DataArray<T>> for DataArray<T>
where
    T: DFNumericType,
    T::Native: NumComp,
{
    fn eq_missing(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        Ok(impl_eq_missing!(self, rhs))
    }

    fn eq(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, eq, eq,  ==}
    }

    fn neq(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, neq, neq,!=}
    }

    fn gt(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, gt,gt, >}
    }

    fn gt_eq(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, gt_eq, gt_eq, >=}
    }

    fn lt(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, lt, lt,  <}
    }

    fn lt_eq(&self, rhs: &DataArray<T>) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, lt_eq, lt_eq, <=}
    }
}

macro_rules! impl_cmp_bool {
    ($self:ident, $rhs:ident, $operand:tt) => {{
        // broadcast
        if $rhs.len() == 1 {
            if let Some(value) = $rhs.get(0) {
                match value {
                    true => Ok($self.clone()),
                    false => Ok($self.not()),
                }
            } else {
                Ok(DFBooleanArray::full(false, $self.len()))
            }
        } else if $self.len() == 1 {
            if let Some(value) = $self.get(0) {
                Ok(match value {
                    true => $rhs.clone(),
                    false => $rhs.not(),
                })
            } else {
                Ok(DFBooleanArray::full(false, $self.len()))
            }
        } else {
            Ok(apply_operand_on_array_by_iter!($self, $rhs, $operand))
        }
    }};
}

impl ArrayCompare<&DFBooleanArray> for DFBooleanArray {
    fn eq_missing(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        Ok(impl_eq_missing!(self, rhs))
    }

    fn eq(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        impl_cmp_bool! {self, rhs, == }
    }

    fn neq(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        impl_cmp_bool! {self, rhs, != }
    }

    fn gt(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        impl_cmp_bool! {self, rhs, > }
    }

    fn gt_eq(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        impl_cmp_bool! {self, rhs, >= }
    }

    fn lt(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        impl_cmp_bool! {self, rhs, < }
    }

    fn lt_eq(&self, rhs: &DFBooleanArray) -> Result<DFBooleanArray> {
        impl_cmp_bool! {self, rhs, <= }
    }
}

impl DFUtf8Array {
    fn comparison(
        &self,
        rhs: &DFUtf8Array,
        operator: impl Fn(
            &LargeStringArray,
            &LargeStringArray,
        ) -> common_arrow::arrow::error::Result<BooleanArray>,
    ) -> Result<DFBooleanArray> {
        let arr = operator(self.downcast_ref(), rhs.downcast_ref())?;
        Ok(DFBooleanArray::new(Arc::new(arr) as ArrayRef))
    }
}

impl ArrayCompare<&DFUtf8Array> for DFUtf8Array {
    fn eq_missing(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        Ok(impl_eq_missing!(self, rhs))
    }

    fn eq(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, eq, eq_utf8,  ==}
    }

    fn neq(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, neq, neq_utf8,  ==}
    }

    fn gt(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, gt, gt_utf8,  ==}
    }

    fn gt_eq(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, gt_eq, gt_eq_utf8,  ==}
    }

    fn lt(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, lt, lt_utf8,  ==}
    }

    fn lt_eq(&self, rhs: &DFUtf8Array) -> Result<DFBooleanArray> {
        impl_cmp_numeric_utf8! {self, rhs, lt_eq, lt_eq_utf8,  ==}
    }
}

impl ArrayCompare<&DFNullArray> for DFNullArray {}
impl ArrayCompare<&DFBinaryArray> for DFBinaryArray {}
impl ArrayCompare<&DFStructArray> for DFStructArray {}

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

impl<T, Rhs> ArrayCompare<Rhs> for DataArray<T>
where
    T: DFNumericType,
    T::Native: NumCast,
    Rhs: NumComp + ToPrimitive,
{
    fn eq_missing(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        self.eq(rhs)
    }

    fn eq(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        let rhs = NumCast::from(rhs);
        match rhs {
            Some(v) => {
                let arr = eq_scalar(self.downcast_ref(), v)?;
                Ok(DFBooleanArray::new(Arc::new(arr) as ArrayRef))
            }
            None => Ok(DFBooleanArray::full(false, self.len())),
        }
    }

    fn neq(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        let rhs = NumCast::from(rhs);
        match rhs {
            Some(v) => {
                let arr = neq_scalar(self.downcast_ref(), v)?;
                Ok(DFBooleanArray::new(Arc::new(arr) as ArrayRef))
            }
            None => Ok(DFBooleanArray::full(false, self.len())),
        }
    }

    fn gt(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        let rhs = NumCast::from(rhs);
        match rhs {
            Some(v) => {
                let arr = gt_scalar(self.downcast_ref(), v)?;
                Ok(DFBooleanArray::new(Arc::new(arr) as ArrayRef))
            }
            None => Ok(DFBooleanArray::full(false, self.len())),
        }
    }

    fn gt_eq(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        let rhs = NumCast::from(rhs);

        match rhs {
            Some(v) => {
                let arr = gt_eq_scalar(self.downcast_ref(), v)?;
                Ok(DFBooleanArray::new(Arc::new(arr) as ArrayRef))
            }
            None => Ok(DFBooleanArray::full(false, self.len())),
        }
    }

    fn lt(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        let rhs = NumCast::from(rhs);

        match rhs {
            Some(v) => {
                let arr = lt_scalar(self.downcast_ref(), v)?;
                Ok(DFBooleanArray::new(Arc::new(arr) as ArrayRef))
            }
            None => Ok(DFBooleanArray::full(false, self.len())),
        }
    }

    fn lt_eq(&self, rhs: Rhs) -> Result<DFBooleanArray> {
        let rhs = NumCast::from(rhs);

        match rhs {
            Some(v) => {
                let arr = lt_eq_scalar(self.downcast_ref(), v)?;
                Ok(DFBooleanArray::new(Arc::new(arr) as ArrayRef))
            }
            None => Ok(DFBooleanArray::full(false, self.len())),
        }
    }
}

impl ArrayCompare<&str> for DFUtf8Array {
    fn eq_missing(&self, rhs: &str) -> Result<DFBooleanArray> {
        self.eq(rhs)
    }

    fn eq(&self, rhs: &str) -> Result<DFBooleanArray> {
        let arr = eq_utf8_scalar(self.downcast_ref(), rhs)?;
        Ok(DFBooleanArray::new(Arc::new(arr) as ArrayRef))
    }

    fn neq(&self, rhs: &str) -> Result<DFBooleanArray> {
        let arr = neq_utf8_scalar(self.downcast_ref(), rhs)?;
        Ok(DFBooleanArray::new(Arc::new(arr) as ArrayRef))
    }

    fn gt(&self, rhs: &str) -> Result<DFBooleanArray> {
        let arr = gt_utf8_scalar(self.downcast_ref(), rhs)?;
        Ok(DFBooleanArray::new(Arc::new(arr) as ArrayRef))
    }

    fn gt_eq(&self, rhs: &str) -> Result<DFBooleanArray> {
        let arr = gt_eq_utf8_scalar(self.downcast_ref(), rhs)?;
        Ok(DFBooleanArray::new(Arc::new(arr) as ArrayRef))
    }

    fn lt(&self, rhs: &str) -> Result<DFBooleanArray> {
        let arr = lt_utf8_scalar(self.downcast_ref(), rhs)?;
        Ok(DFBooleanArray::new(Arc::new(arr) as ArrayRef))
    }

    fn lt_eq(&self, rhs: &str) -> Result<DFBooleanArray> {
        let arr = lt_eq_utf8_scalar(self.downcast_ref(), rhs)?;
        Ok(DFBooleanArray::new(Arc::new(arr) as ArrayRef))
    }
}

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
    fn eq_missing(&self, rhs: &DFListArray) -> Result<DFBooleanArray> {
        Ok(impl_cmp_numeric_utf8_list!(self, rhs, series_equal_missing))
    }

    fn eq(&self, rhs: &DFListArray) -> Result<DFBooleanArray> {
        Ok(impl_cmp_numeric_utf8_list!(self, rhs, series_equal))
    }

    fn neq(&self, rhs: &DFListArray) -> Result<DFBooleanArray> {
        Ok(self.eq(rhs)?.not())
    }

    // following are not implemented because gt, lt comparison of series don't make sense
    fn gt(&self, _rhs: &DFListArray) -> Result<DFBooleanArray> {
        unimplemented!()
    }

    fn gt_eq(&self, _rhs: &DFListArray) -> Result<DFBooleanArray> {
        unimplemented!()
    }

    fn lt(&self, _rhs: &DFListArray) -> Result<DFBooleanArray> {
        unimplemented!()
    }

    fn lt_eq(&self, _rhs: &DFListArray) -> Result<DFBooleanArray> {
        unimplemented!()
    }
}

impl Not for &DFBooleanArray {
    type Output = DFBooleanArray;

    fn not(self) -> Self::Output {
        let arr = compute::not(self.downcast_ref()).expect("should not fail");
        let arr = Arc::new(arr) as ArrayRef;
        DataArray::new(arr)
    }
}

impl Not for DFBooleanArray {
    type Output = DFBooleanArray;

    fn not(self) -> Self::Output {
        (&self).not()
    }
}

impl DFBooleanArray {
    /// Check if all values are true
    pub fn all_true(&self) -> bool {
        let mut values = self.downcast_iter();
        values.all(|f| f.unwrap_or(false))
    }
}

impl DFBooleanArray {
    /// Check if all values are false
    pub fn all_false(&self) -> bool {
        let mut values = self.downcast_iter();
        values.all(|f| !f.unwrap_or(true))
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
