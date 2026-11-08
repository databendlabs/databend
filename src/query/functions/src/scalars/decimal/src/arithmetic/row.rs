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

//! Row-level decimal arithmetic. Every constructor consumes bound metadata
//! once; row methods consume only operands and prepared constants. Native
//! paths stay inline, and wide fallbacks through i256 and BigInt stay out of
//! line.

use databend_common_expression::types::decimal::*;
use num_bigint::BigInt;

use crate::decimal::DecimalExt;

#[derive(Clone, Copy, Debug)]
pub(super) struct Bounds<T> {
    pub(super) min: T,
    pub(super) max: T,
}

impl<T: DecimalExt> Bounds<T> {
    pub(super) fn new(precision: u8) -> Self {
        assert!(
            (1..=T::MAX_PRECISION).contains(&precision),
            "internal {:?} precision {precision} outside 1..={}",
            T::KIND,
            T::MAX_PRECISION
        );
        Self {
            min: T::min_for_precision(precision),
            max: T::max_for_precision(precision),
        }
    }

    #[inline]
    pub(super) fn fit(&self, value: T) -> Option<T> {
        (self.min <= value && value <= self.max).then_some(value)
    }
}

pub(super) fn to_bigint<T: DecimalExt>(value: T) -> BigInt {
    BigInt::from_signed_bytes_le(&value.as_decimal::<i256>().to_le_bytes())
}

// Compare the remainder with its complement instead of adding half the divisor
// to the numerator. Scaling may fit while that rounding addition would overflow.
pub(super) fn round_quotient<T: DecimalExt>(numerator: T, divisor: T) -> Option<T> {
    let (quotient, remainder) = numerator.checked_div_rem(divisor)?;
    let zero = T::zero();
    let remainder = if remainder < zero {
        zero.checked_sub(remainder)?
    } else {
        remainder
    };
    let magnitude_divisor = if divisor < zero {
        zero.checked_sub(divisor)?
    } else {
        divisor
    };
    if remainder >= magnitude_divisor.checked_sub(remainder)? {
        quotient.checked_add(if (numerator < zero) == (divisor < zero) {
            T::one()
        } else {
            T::minus_one()
        })
    } else {
        Some(quotient)
    }
}

pub(super) fn round_bigint(numerator: BigInt, divisor: BigInt) -> BigInt {
    let half = &divisor / 2_u8;
    if (numerator < BigInt::from(0)) == (divisor < BigInt::from(0)) {
        (numerator + half) / divisor
    } else {
        (numerator - half) / divisor
    }
}

// Addition, subtraction and alignment.

/// Scales one operand (the left one if `LEFT`) up to the result scale; the
/// other operand already has it. The result scale is the larger operand
/// scale and never exceeds the computation precision, so the factor always
/// fits `T`; only a scaled value can overflow it.
#[derive(Debug)]
pub(super) struct Aligned<T, const LEFT: bool> {
    factor: T,
    big_factor: BigInt,
    bounds: Bounds<T>,
}

impl<T: DecimalExt, const LEFT: bool> Aligned<T, LEFT> {
    pub(super) fn new(shift: u32, precision: u8) -> Self {
        assert!(
            (1..=T::MAX_PRECISION as u32).contains(&shift),
            "internal {:?} alignment shift {shift} is invalid",
            T::KIND
        );
        Self {
            factor: T::e(shift as u8),
            big_factor: BigInt::from(10).pow(shift),
            bounds: Bounds::new(precision),
        }
    }

    #[inline(always)]
    fn native(&self, a: T, b: T) -> Option<(T, T)> {
        if LEFT {
            Some((a.checked_mul(self.factor)?, b))
        } else {
            Some((a, b.checked_mul(self.factor)?))
        }
    }

    fn wide(&self, a: T, b: T) -> (BigInt, BigInt) {
        let (a, b) = (to_bigint(a), to_bigint(b));
        if LEFT {
            (a * &self.big_factor, b)
        } else {
            (a, b * &self.big_factor)
        }
    }

    #[inline(always)]
    pub(super) fn add_subtract<const SUBTRACT: bool>(&self, a: T, b: T) -> Option<T> {
        if let Some((a, b)) = self.native(a, b) {
            let result = if SUBTRACT {
                a.checked_sub(b)
            } else {
                a.checked_add(b)
            };
            if let Some(value) = result {
                return self.bounds.fit(value);
            }
        }
        self.add_subtract_wide::<SUBTRACT>(a, b)
    }

    // A scaled operand can exceed T while cancellation still fits.
    #[inline(never)]
    fn add_subtract_wide<const SUBTRACT: bool>(&self, a: T, b: T) -> Option<T> {
        let (a, b) = self.wide(a, b);
        self.bounds
            .fit(T::from_bigint(if SUBTRACT { a - b } else { a + b })?)
    }

    /// The caller rejects a zero divisor; scaling never makes one zero.
    #[inline(always)]
    pub(super) fn remainder(&self, a: T, b: T) -> Option<T> {
        if let Some((a, b)) = self.native(a, b)
            && let Some(value) = a.checked_rem(b)
        {
            return self.bounds.fit(value);
        }
        self.remainder_wide(a, b)
    }

    #[inline(never)]
    fn remainder_wide(&self, a: T, b: T) -> Option<T> {
        let (a, b) = self.wide(a, b);
        self.bounds.fit(T::from_bigint(a % b)?)
    }
}

// Visible operands fit by the binding proof. Wrapping also makes this cheap
// path safe for arbitrary payloads in rows excluded by validity.
#[inline]
pub(super) fn native_add_subtract<T: DecimalExt, const SUBTRACT: bool>(a: T, b: T) -> T {
    if SUBTRACT {
        a.wrapping_sub(b)
    } else {
        a.wrapping_add(b)
    }
}

#[inline]
pub(super) fn checked_add_subtract<T: DecimalExt, const SUBTRACT: bool>(
    a: T,
    b: T,
    bounds: &Bounds<T>,
) -> Option<T> {
    // For each decimal storage type, 2^bits > 3*(10^MAX_PRECISION-1).
    // Visible decimal operands have magnitude <= M: a machine overflow wraps
    // outside [-M,M], so the decimal bound check still rejects it. Inactive
    // arbitrary payloads can wrap without panicking and remain unobservable.
    bounds.fit(native_add_subtract::<T, SUBTRACT>(a, b))
}

// Multiplication.

#[inline]
pub(super) fn checked_integral<T: DecimalExt>(a: T, b: T, bounds: &Bounds<T>) -> Option<T> {
    a.checked_mul(b).and_then(|value| bounds.fit(value))
}

#[derive(Clone, Copy, Debug)]
pub(super) struct NativeRound<T> {
    pub(super) divisor: T,
    pub(super) half: T,
}

impl<T: DecimalExt> NativeRound<T> {
    pub(super) fn new(shift: u32) -> Self {
        assert!(
            (1..=T::MAX_PRECISION as u32).contains(&shift),
            "internal {:?} native rounding shift {shift} is invalid",
            T::KIND
        );
        let divisor = T::e(shift as u8);
        Self {
            divisor,
            half: divisor / T::from_i64(2),
        }
    }
}

// Binding proves the native product plus rounding offset fits storage. The
// vectorizer skips invisible payloads before this unchecked native kernel.
#[inline]
pub(super) fn native_rounded<T: DecimalExt>(a: T, b: T, scale: &NativeRound<T>) -> T {
    if (a < T::zero()) == (b < T::zero()) {
        (a * b + scale.half) / scale.divisor
    } else {
        (a * b - scale.half) / scale.divisor
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Rounded64 {
    divisor: i128,
    half: i128,
    bounds: Bounds<i128>,
}

impl Rounded64 {
    pub(crate) fn new(shift: u32, precision: u8) -> Self {
        assert!(
            (1..=i64::MAX_MULTIPLY_SHIFT).contains(&shift),
            "internal i64 multiplication shift {shift} is invalid"
        );
        assert!(
            (1..=i64::MAX_PRECISION).contains(&precision),
            "internal i64 multiplication precision {precision} is invalid"
        );
        let divisor = i128::e(shift as u8);
        Rounded64 {
            divisor,
            half: divisor / 2,
            bounds: Bounds::new(precision),
        }
    }

    #[inline]
    pub(crate) fn multiply(&self, a: i64, b: i64) -> Option<i64> {
        // For all i64 inputs |product| <= 2^126, so adding <= 10^36/2 is safe.
        let product = (a as i128) * (b as i128);
        let rounded = (product.abs() + self.half) / self.divisor;
        self.bounds
            .fit(rounded)
            .map(|value| (if product < 0 { -value } else { value }) as i64)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Rounded128 {
    native: NativeRound<i128>,
    budget: i128,
    bounds: Bounds<i128>,
    wide: NativeRound<i256>,
    wide_limit: i256,
}

impl Rounded128 {
    pub(crate) fn new(shift: u32, precision: u8) -> Self {
        assert!(
            (1..=i128::MAX_MULTIPLY_SHIFT).contains(&shift),
            "internal i128 multiplication shift {shift} is invalid"
        );
        assert!(
            (1..=i128::MAX_PRECISION).contains(&precision),
            "internal i128 multiplication precision {precision} is invalid"
        );
        let bounds = Bounds::<i128>::new(precision);
        let divisor = i128::e(shift as u8);
        let half = divisor / 2;
        Rounded128 {
            native: NativeRound { divisor, half },
            budget: i128::MAX - half,
            wide: NativeRound {
                divisor: i256::from(divisor),
                half: i256::from(half),
            },
            wide_limit: i256::from(bounds.max),
            bounds,
        }
    }

    #[inline]
    pub(crate) fn multiply(&self, a: i128, b: i128) -> Option<i128> {
        if can_mul128_with_budget(a, b, self.budget) {
            mul128_rounded(a, b, self.native, self.bounds.max)
        } else {
            // Two i128 inputs have product magnitude <= 2^254. Adding half
            // of a divisor <= 10^38 fits i256. Check before narrowing.
            let product = i256::from(a) * i256::from(b);
            let magnitude = if product < i256::ZERO {
                -product
            } else {
                product
            };
            let rounded = (magnitude + self.wide.half) / self.wide.divisor;
            (rounded <= self.wide_limit).then(|| {
                (if product < i256::ZERO {
                    -rounded
                } else {
                    rounded
                })
                .as_i128()
            })
        }
    }
}

#[inline]
fn can_mul128_with_budget(a: i128, b: i128, budget: i128) -> bool {
    if a == 0 || b == 0 {
        return true;
    }
    if a == i128::MIN || b == i128::MIN {
        return false;
    }
    let magnitude_a = if a < 0 { -a } else { a };
    let magnitude_b = if b < 0 { -b } else { b };
    magnitude_a <= budget / magnitude_b
}

// Caller establishes |a*b| + half <= MAX before using native multiplication.
#[inline]
fn mul128_rounded(a: i128, b: i128, scale: NativeRound<i128>, limit: i128) -> Option<i128> {
    let product = a * b;
    let magnitude = if product < 0 { -product } else { product };
    let rounded = (magnitude + scale.half) / scale.divisor;
    (rounded <= limit).then_some(if product < 0 { -rounded } else { rounded })
}

#[derive(Clone, Debug)]
pub(crate) struct Rounded256 {
    native: NativeRound<i256>,
    bounds: Bounds<i256>,
    big_divisor: BigInt,
    big_half: BigInt,
}

impl Rounded256 {
    pub(crate) fn new(shift: u32, precision: u8) -> Self {
        assert!(
            (1..=i256::MAX_MULTIPLY_SHIFT).contains(&shift),
            "internal i256 multiplication shift {shift} is invalid"
        );
        assert!(
            (1..=i256::MAX_PRECISION).contains(&precision),
            "internal i256 multiplication precision {precision} is invalid"
        );
        let bounds = Bounds::new(precision);
        let divisor = i256::e(shift as u8);
        let half = divisor / i256::from(2);
        Rounded256 {
            native: NativeRound { divisor, half },
            bounds,
            big_divisor: to_bigint(divisor),
            big_half: to_bigint(half),
        }
    }

    #[inline]
    pub(crate) fn multiply(&self, a: i256, b: i256) -> Option<i256> {
        if let Some(product) = a.checked_mul(b) {
            // The prepared divisor is positive and even (10^shift, shift > 0).
            // No per-row divisor sign/zero/width dispatch or half calculation.
            let (quotient, remainder) = product.0.checked_div_rem(self.native.divisor.0)?;
            let remainder = i256(remainder);
            // |remainder| < divisor <= 10^76, so this magnitude is representable.
            let magnitude = if remainder < i256::ZERO {
                -remainder
            } else {
                remainder
            };
            let rounded = if magnitude >= self.native.half {
                i256(quotient).checked_add(if product < i256::ZERO {
                    i256::MINUS_ONE
                } else {
                    i256::ONE
                })?
            } else {
                i256(quotient)
            };
            return self.bounds.fit(rounded);
        }
        // Precision bounds only the rescaled result, not the unscaled product.
        // Both BigInt constants are bound once and borrowed by all fallback rows.
        let product = to_bigint(a) * to_bigint(b);
        let numerator = if product < BigInt::from(0) {
            product - &self.big_half
        } else {
            product + &self.big_half
        };
        self.bounds
            .fit(i256::from_bigint(numerator / &self.big_divisor)?)
    }
}

// Division.

/// How the numerator is scaled by `10^shift` in each intermediate width. One
/// shift selects exactly one implementation at binding; a factor too wide for
/// an intermediate still scales zero.
pub(super) trait DivideScaling: Copy + Send + Sync + 'static {
    fn scale128(&self, value: i128) -> Option<i128>;
    fn scale256(&self, value: i256) -> Option<i256>;
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Unscaled;

#[derive(Clone, Copy, Debug)]
pub(super) struct Scale128 {
    factor: i128,
    wide_factor: i256,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Scale256(i256);

#[derive(Clone, Copy, Debug)]
pub(super) struct ScaleWide;

impl Scale128 {
    pub(super) fn new(shift: u32) -> Self {
        assert!((1..=i128::MAX_PRECISION as u32).contains(&shift));
        let factor = i128::e(shift as u8);
        Self {
            factor,
            wide_factor: i256::from(factor),
        }
    }
}

impl Scale256 {
    pub(super) fn new(shift: u32) -> Self {
        assert!((i128::MAX_PRECISION as u32 + 1..=i256::MAX_PRECISION as u32).contains(&shift));
        Self(i256::e(shift as u8))
    }
}

fn zero_only<T: DecimalExt>(value: T) -> Option<T> {
    (value == T::zero()).then_some(value)
}

impl DivideScaling for Unscaled {
    #[inline(always)]
    fn scale128(&self, value: i128) -> Option<i128> {
        Some(value)
    }
    #[inline(always)]
    fn scale256(&self, value: i256) -> Option<i256> {
        Some(value)
    }
}

impl DivideScaling for Scale128 {
    #[inline(always)]
    fn scale128(&self, value: i128) -> Option<i128> {
        value.checked_mul(self.factor)
    }
    #[inline(always)]
    fn scale256(&self, value: i256) -> Option<i256> {
        value.checked_mul(self.wide_factor)
    }
}

impl DivideScaling for Scale256 {
    #[inline(always)]
    fn scale128(&self, value: i128) -> Option<i128> {
        zero_only(value)
    }
    #[inline(always)]
    fn scale256(&self, value: i256) -> Option<i256> {
        value.checked_mul(self.0)
    }
}

impl DivideScaling for ScaleWide {
    #[inline(always)]
    fn scale128(&self, value: i128) -> Option<i128> {
        zero_only(value)
    }
    #[inline(always)]
    fn scale256(&self, value: i256) -> Option<i256> {
        zero_only(value)
    }
}

/// Protected division computing in `T` and producing `U`.
#[derive(Debug)]
pub(super) struct CheckedDivide<T, U, S> {
    scaling: S,
    big_factor: BigInt,
    bounds: Bounds<T>,
    // Consulted only for a narrow output: a wide output has precision > 38,
    // so every i128 quotient fits it.
    bounds128: Bounds<i128>,
    bounds256: Bounds<i256>,
    output: std::marker::PhantomData<U>,
}

impl<T: DecimalExt, U: DecimalExt, S: DivideScaling> CheckedDivide<T, U, S> {
    const NARROW: bool = U::MAX_PRECISION <= i128::MAX_PRECISION;

    pub(super) fn new(scaling: S, shift: u32, precision: u8) -> Self {
        assert!(
            shift <= 152,
            "internal division shift {shift} exceeds bound decimal dimensions"
        );
        assert_eq!(
            DecimalSize::new(precision, 0).unwrap().data_kind(),
            U::KIND,
            "internal division output storage"
        );
        Self {
            scaling,
            big_factor: BigInt::from(10).pow(shift),
            bounds: Bounds::new(precision),
            bounds128: Bounds::new(precision.min(i128::MAX_PRECISION)),
            bounds256: Bounds::new(precision),
            output: std::marker::PhantomData,
        }
    }

    // Keep the primitive i128 route in the vectorized caller. A large out-of-
    // line Option<i256> return costs more than this path's actual arithmetic.
    // The caller rejects a zero divisor before this kernel.
    #[inline(always)]
    pub(super) fn divide(&self, a: T, b: T) -> Option<U> {
        debug_assert!(b != T::zero());
        if let Some(native_a) = a.checked_to_i128()
            && let Some(numerator) = self.scaling.scale128(native_a)
            && let Some(native_b) = b.checked_to_i128()
            && let Some(value) = round_quotient(numerator, native_b)
        {
            return if Self::NARROW {
                self.bounds128
                    .fit(value)
                    .map(|value| value.as_decimal::<U>())
            } else {
                Some(value.as_decimal::<U>())
            };
        }
        self.divide_wide(a, b).map(|value| value.as_decimal::<U>())
    }

    // Do not duplicate software division/BigInt allocation into every reader.
    #[inline(never)]
    fn divide_wide(&self, a: T, b: T) -> Option<T> {
        if let Some(value) = self
            .scaling
            .scale256(a.as_decimal::<i256>())
            .and_then(|numerator| round_quotient(numerator, b.as_decimal::<i256>()))
        {
            return self
                .bounds256
                .fit(value)
                .map(|value| value.as_decimal::<T>());
        }
        let numerator = to_bigint(a) * &self.big_factor;
        let value = round_bigint(numerator, to_bigint(b));
        self.bounds.fit(T::from_bigint(value)?)
    }
}

#[inline]
pub(super) fn native64(a: i64, divisor: i64, multiplier: i64) -> i64 {
    // NativeDivisor already normalized each wide divisor without changing the
    // quotient. The caller handles zero before entering this numerical kernel.
    let numerator = a.wrapping_mul(multiplier);
    let adjusted = if (numerator < 0) == (divisor < 0) {
        numerator.wrapping_add(divisor / 2)
    } else {
        numerator.wrapping_sub(divisor / 2)
    };
    adjusted.wrapping_div(divisor)
}

#[inline]
pub(super) fn native_scaled<T: DecimalExt>(a: T, b: T, multiplier: T) -> T {
    let half = b / T::from_i64(2);
    if (a < T::zero()) == (b < T::zero()) {
        (a * multiplier + half) / b
    } else {
        (a * multiplier - half) / b
    }
}
