// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
use std::ops::Add;
use std::ops::Div;
use std::ops::Mul;
use std::ops::Neg;
use std::ops::Rem;
use std::ops::Sub;

use common_exception::Result;

use crate::prelude::*;

macro_rules! apply_arithmetic {
    ($self: ident, $rhs: ident, $op: tt) => {{
        if $self.is_null() {
            return Ok($rhs.clone());
        }

        if $rhs.is_null() {
            return Ok($self.clone());
        }
        let lhs = $self.to_series_with_size(1)?;
        let rhs = $rhs.to_series_with_size(1)?;

        let result = (&lhs $op &rhs)?;
        result.try_get(0)
    }};
}

macro_rules! apply_comparsion {
    ($self: ident, $rhs: ident, $op: tt) => {{
        if $self.is_null() {
            return Ok($rhs.clone());
        }

        if $rhs.is_null() {
            return Ok($self.clone());
        }

        let lhs = $self.to_series_with_size(1)?;
        let rhs = $rhs.to_series_with_size(1)?;

        let result = lhs.$op(&rhs)?;
        unsafe { result.try_get(0) }
    }};
}

impl DataValue {
    /// if one of the datavalues is null, return the other one
    /// if both are no null, then turn them into one-size array and apply the operation
    pub fn arithmetic(
        &self,
        op: DataValueArithmeticOperator,
        rhs: &DataValue,
    ) -> Result<DataValue> {
        match op {
            Plus => self + rhs,
            Minus => self - rhs,
            Mul => self * rhs,
            Div => self / rhs,
            Modulo => self % rhs,
        }
    }

    // always return DataValue::Boolean
    pub fn compare(&self, op: DataValueComparisonOperator, rhs: &DataValue) -> Result<DataValue> {
        match op {
            Eq => apply_comparsion! {self, rhs, eq},
            Lt => apply_comparsion! {self, rhs, lt},
            LtEq => apply_comparsion! {self, rhs, lt_eq},
            Gt => apply_comparsion! {self, rhs, gt},
            GtEq => apply_comparsion! {self, rhs, gt_eq},
            NotEq => apply_comparsion! {self, rhs, neq},
            Like => apply_comparsion! {self, rhs, like},
            NotLike => apply_comparsion! {self, rhs, nlike},
        }
    }

    pub fn min(&self, rhs: &DataValue) -> Result<DataValue> {
        if self.is_null() {
            return Ok(rhs.clone());
        }

        if rhs.is_null() {
            return Ok(self.clone());
        }

        if let Ok(DataValue::Boolean(Some(true))) = self.compare(LtEq, rhs) {
            return Ok(self.clone());
        }
        return Ok(rhs.clone());
    }

    pub fn max(&self, rhs: &DataValue) -> Result<DataValue> {
        if self.is_null() {
            return Ok(rhs.clone());
        }

        if rhs.is_null() {
            return Ok(self.clone());
        }

        if let Ok(DataValue::Boolean(Some(true))) = self.compare(GtEq, rhs) {
            return Ok(self.clone());
        }
        return Ok(rhs.clone());
    }
}

impl Add for &DataValue {
    type Output = Result<DataValue>;

    fn add(self, rhs: Self) -> Self::Output {
        apply_arithmetic! {self, rhs, +}
    }
}

impl Sub for &DataValue {
    type Output = Result<DataValue>;

    fn sub(self, rhs: Self) -> Self::Output {
        apply_arithmetic! {self, rhs, -}
    }
}

impl Mul for &DataValue {
    type Output = Result<DataValue>;

    fn mul(self, rhs: Self) -> Self::Output {
        apply_arithmetic! {self, rhs, *}
    }
}

impl Div for &DataValue {
    type Output = Result<DataValue>;

    fn div(self, rhs: Self) -> Self::Output {
        apply_arithmetic! {self, rhs, /}
    }
}

impl Rem for &DataValue {
    type Output = Result<DataValue>;

    fn rem(self, rhs: Self) -> Self::Output {
        apply_arithmetic! {self, rhs, %}
    }
}

impl Neg for &DataValue {
    type Output = Result<DataValue>;

    fn neg(self) -> Self::Output {
        if self.is_null() {
            return Ok(self.clone());
        }
        let lhs = self.to_series_with_size(1)?;
        let result = Neg::neg(&lhs)?;
        result.try_get(0)
    }
}
