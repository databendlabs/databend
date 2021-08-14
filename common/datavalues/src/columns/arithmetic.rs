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

use std::ops::Add;
use std::ops::Div;
use std::ops::Mul;
use std::ops::Neg;
use std::ops::Rem;
use std::ops::Sub;

use common_exception::Result;

use crate::prelude::*;
use crate::DataValueArithmeticOperator;

macro_rules! apply_arithmetic {
    ($self: ident, $rhs: ident, $op: tt) => {{
        let lhs = $self.to_minimal_array()?;
        let rhs = $rhs.to_minimal_array()?;

        let result = (&lhs $op &rhs)?;
        let result: DataColumn = result.into();
        Ok(result.resize_constant($self.len()))
    }};
}

impl Add for &DataColumn {
    type Output = Result<DataColumn>;

    fn add(self, rhs: Self) -> Self::Output {
        apply_arithmetic! {self, rhs, +}
    }
}

impl Sub for &DataColumn {
    type Output = Result<DataColumn>;

    fn sub(self, rhs: Self) -> Self::Output {
        apply_arithmetic! {self, rhs, -}
    }
}

impl Mul for &DataColumn {
    type Output = Result<DataColumn>;

    fn mul(self, rhs: Self) -> Self::Output {
        apply_arithmetic! {self, rhs, *}
    }
}

impl Div for &DataColumn {
    type Output = Result<DataColumn>;

    fn div(self, rhs: Self) -> Self::Output {
        apply_arithmetic! {self, rhs, /}
    }
}

impl Rem for &DataColumn {
    type Output = Result<DataColumn>;

    fn rem(self, rhs: Self) -> Self::Output {
        apply_arithmetic! {self, rhs, %}
    }
}

impl Neg for &DataColumn {
    type Output = Result<DataColumn>;

    fn neg(self) -> Self::Output {
        let lhs = self.to_minimal_array()?;
        let lhs = Neg::neg(&lhs)?;
        let result: DataColumn = lhs.into();
        Ok(result.resize_constant(self.len()))
    }
}

impl DataColumn {
    pub fn arithmetic(
        &self,
        op: DataValueArithmeticOperator,
        rhs: &DataColumn,
    ) -> Result<DataColumn> {
        match op {
            DataValueArithmeticOperator::Plus => self + rhs,
            DataValueArithmeticOperator::Minus => self - rhs,
            DataValueArithmeticOperator::Mul => self * rhs,
            DataValueArithmeticOperator::Div => self / rhs,
            DataValueArithmeticOperator::Modulo => self % rhs,
        }
    }
}
