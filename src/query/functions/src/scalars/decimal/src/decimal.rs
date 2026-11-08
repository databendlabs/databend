// Copyright 2021 Datafuse Labs
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

//! Function-layer decimal storage. This is a closed set: i64, i128 and i256.
//! Expression's Decimal remains the value/type interface; native arithmetic,
//! wrapping and lossless narrowing are defined here once for function kernels.

use std::ops::Add;
use std::ops::Div;
use std::ops::DivAssign;
use std::ops::Mul;
use std::ops::Neg;
use std::ops::Rem;
use std::ops::Sub;

use databend_common_expression::ScalarFunction;
use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::decimal::DecimalDataKind;
use databend_common_expression::types::decimal::i256;

use crate::arithmetic::InputPlan;
use crate::arithmetic::Kernel;
use crate::arithmetic::Rounded64;
use crate::arithmetic::Rounded128;
use crate::arithmetic::Rounded256;
use crate::arithmetic::evaluator;

pub(crate) trait DecimalExt:
    Decimal
    + From<i8>
    + Add<Output = Self>
    + Sub<Output = Self>
    + Mul<Output = Self>
    + Div<Output = Self>
    + Rem<Output = Self>
    + Neg<Output = Self>
    + DivAssign
{
    const KIND: DecimalDataKind;
    const MAX_MULTIPLY_SHIFT: u32;
    type Rounded: std::fmt::Debug + Send + Sync;

    fn prepare_rounded(shift: u32, precision: u8) -> Self::Rounded;
    /// Binds a kernel computing in `Self` to its operand storage. Only the
    /// operand storage pairs that legal signatures produce for this
    /// computation type are instantiated.
    fn bind_kernel<K: Kernel<Compute = Self>>(
        kernel: K,
        inputs: [InputPlan; 2],
    ) -> Box<dyn ScalarFunction>;
    fn multiply_rounded(self, rhs: Self, kernel: &Self::Rounded) -> Option<Self>;
    fn wrapping_add(self, rhs: Self) -> Self;
    fn wrapping_sub(self, rhs: Self) -> Self;
    fn wrapping_neg(self) -> Self;
    fn wrapping_rem(self, rhs: Self) -> Self;
    fn checked_div_rem(self, rhs: Self) -> Option<(Self, Self)>;
    fn checked_to_i64(self) -> Option<i64>;
    fn checked_to_i128(self) -> Option<i128>;

    #[inline]
    fn saturating_to_i64(self) -> i64 {
        match self.checked_to_i64() {
            Some(value) => value,
            None if self < Self::zero() => i64::MIN,
            None => i64::MAX,
        }
    }
}

fn operand_storage(inputs: &[InputPlan; 2]) -> [DecimalDataKind; 2] {
    inputs.map(|input| input.storage())
}

fn illegal_operands<T: DecimalExt>(storage: [DecimalDataKind; 2]) -> ! {
    unreachable!(
        "internal operand storage {storage:?} cannot bind to {:?} computation",
        T::KIND
    )
}

impl DecimalExt for i64 {
    const MAX_MULTIPLY_SHIFT: u32 = 36;
    const KIND: DecimalDataKind = DecimalDataKind::Decimal64;
    type Rounded = Rounded64;

    fn prepare_rounded(shift: u32, precision: u8) -> Self::Rounded {
        Rounded64::new(shift, precision)
    }

    fn bind_kernel<K: Kernel<Compute = Self>>(
        kernel: K,
        inputs: [InputPlan; 2],
    ) -> Box<dyn ScalarFunction> {
        use DecimalDataKind::*;
        match operand_storage(&inputs) {
            [Decimal64, Decimal64] => evaluator::<K, i64, i64>(kernel, inputs),
            storage => illegal_operands::<Self>(storage),
        }
    }

    #[inline]
    fn multiply_rounded(self, rhs: Self, kernel: &Self::Rounded) -> Option<Self> {
        kernel.multiply(self, rhs)
    }

    #[inline]
    fn wrapping_add(self, rhs: Self) -> Self {
        i64::wrapping_add(self, rhs)
    }
    #[inline]
    fn wrapping_sub(self, rhs: Self) -> Self {
        i64::wrapping_sub(self, rhs)
    }
    #[inline]
    fn wrapping_neg(self) -> Self {
        i64::wrapping_neg(self)
    }
    #[inline]
    fn wrapping_rem(self, rhs: Self) -> Self {
        i64::wrapping_rem(self, rhs)
    }
    #[inline]
    fn checked_div_rem(self, rhs: Self) -> Option<(Self, Self)> {
        Some((i64::checked_div(self, rhs)?, i64::checked_rem(self, rhs)?))
    }
    #[inline]
    fn checked_to_i64(self) -> Option<i64> {
        Some(self)
    }
    #[inline]
    fn checked_to_i128(self) -> Option<i128> {
        Some(self as i128)
    }
}

impl DecimalExt for i128 {
    const MAX_MULTIPLY_SHIFT: u32 = 38;
    const KIND: DecimalDataKind = DecimalDataKind::Decimal128;
    type Rounded = Rounded128;

    fn prepare_rounded(shift: u32, precision: u8) -> Self::Rounded {
        Rounded128::new(shift, precision)
    }

    fn bind_kernel<K: Kernel<Compute = Self>>(
        kernel: K,
        inputs: [InputPlan; 2],
    ) -> Box<dyn ScalarFunction> {
        use DecimalDataKind::*;
        match operand_storage(&inputs) {
            [Decimal64, Decimal64] => evaluator::<K, i64, i64>(kernel, inputs),
            [Decimal64, Decimal128] => evaluator::<K, i64, i128>(kernel, inputs),
            [Decimal128, Decimal64] => evaluator::<K, i128, i64>(kernel, inputs),
            [Decimal128, Decimal128] => evaluator::<K, i128, i128>(kernel, inputs),
            storage => illegal_operands::<Self>(storage),
        }
    }

    #[inline]
    fn multiply_rounded(self, rhs: Self, kernel: &Self::Rounded) -> Option<Self> {
        kernel.multiply(self, rhs)
    }

    #[inline]
    fn wrapping_add(self, rhs: Self) -> Self {
        i128::wrapping_add(self, rhs)
    }
    #[inline]
    fn wrapping_sub(self, rhs: Self) -> Self {
        i128::wrapping_sub(self, rhs)
    }
    #[inline]
    fn wrapping_neg(self) -> Self {
        i128::wrapping_neg(self)
    }
    #[inline]
    fn wrapping_rem(self, rhs: Self) -> Self {
        i128::wrapping_rem(self, rhs)
    }
    #[inline]
    fn checked_div_rem(self, rhs: Self) -> Option<(Self, Self)> {
        Some((i128::checked_div(self, rhs)?, i128::checked_rem(self, rhs)?))
    }
    #[inline]
    fn checked_to_i64(self) -> Option<i64> {
        i64::try_from(self).ok()
    }
    #[inline]
    fn checked_to_i128(self) -> Option<i128> {
        Some(self)
    }
}

impl DecimalExt for i256 {
    const MAX_MULTIPLY_SHIFT: u32 = 76;
    const KIND: DecimalDataKind = DecimalDataKind::Decimal256;
    type Rounded = Rounded256;

    fn prepare_rounded(shift: u32, precision: u8) -> Self::Rounded {
        Rounded256::new(shift, precision)
    }

    fn bind_kernel<K: Kernel<Compute = Self>>(
        kernel: K,
        inputs: [InputPlan; 2],
    ) -> Box<dyn ScalarFunction> {
        use DecimalDataKind::*;
        // An i256 computation needs more than 38 digits. Either some original
        // operand has them, and is kept or rescaled into i256, or (modulo
        // only) aligning to the larger scale needs them, and the rescaled
        // operand is materialized in i256. So at least one operand is i256.
        match operand_storage(&inputs) {
            [Decimal64, Decimal256] => evaluator::<K, i64, i256>(kernel, inputs),
            [Decimal128, Decimal256] => evaluator::<K, i128, i256>(kernel, inputs),
            [Decimal256, Decimal64] => evaluator::<K, i256, i64>(kernel, inputs),
            [Decimal256, Decimal128] => evaluator::<K, i256, i128>(kernel, inputs),
            [Decimal256, Decimal256] => evaluator::<K, i256, i256>(kernel, inputs),
            storage => illegal_operands::<Self>(storage),
        }
    }

    #[inline]
    fn multiply_rounded(self, rhs: Self, kernel: &Self::Rounded) -> Option<Self> {
        kernel.multiply(self, rhs)
    }

    #[inline]
    fn wrapping_add(self, rhs: Self) -> Self {
        i256(self.0.wrapping_add(rhs.0))
    }
    #[inline]
    fn wrapping_sub(self, rhs: Self) -> Self {
        i256(self.0.wrapping_sub(rhs.0))
    }
    #[inline]
    fn wrapping_neg(self) -> Self {
        i256(self.0.wrapping_neg())
    }
    #[inline]
    fn wrapping_rem(self, rhs: Self) -> Self {
        i256(self.0.wrapping_rem(rhs.0))
    }
    #[inline]
    fn checked_div_rem(self, rhs: Self) -> Option<(Self, Self)> {
        let (quotient, remainder) = self.0.checked_div_rem(rhs.0)?;
        Some((i256(quotient), i256(remainder)))
    }
    #[inline]
    fn checked_to_i64(self) -> Option<i64> {
        let low = *self.0.low();
        let value = low as i64;
        let sign = if value < 0 { -1 } else { 0 };
        (low == value as i128 && *self.0.high() == sign).then_some(value)
    }
    #[inline]
    fn checked_to_i128(self) -> Option<i128> {
        let low = *self.0.low();
        let sign = if low < 0 { -1 } else { 0 };
        (*self.0.high() == sign).then_some(low)
    }
}
