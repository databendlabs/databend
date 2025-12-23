// Copyright 2015 Jonathan Reem
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

//! Wrappers for total order on Floats.  See the [`OrderedFloat`] and [`NotNan`] docs for details.

extern crate std;

use core::borrow::Borrow;
use core::cmp::Ordering;
use core::convert::TryFrom;
use core::fmt;
use core::hash::Hash;
use core::hash::Hasher;
use core::iter::Product;
use core::iter::Sum;
use core::num::FpCategory;
use core::ops::Add;
use core::ops::AddAssign;
use core::ops::Deref;
use core::ops::DerefMut;
use core::ops::Div;
use core::ops::DivAssign;
use core::ops::Mul;
use core::ops::MulAssign;
use core::ops::Neg;
use core::ops::Rem;
use core::ops::RemAssign;
use core::ops::Sub;
use core::ops::SubAssign;
use core::str::FromStr;
use std::error::Error;

use bytemuck::Pod;
use bytemuck::Zeroable;
use micromarshal::Unmarshal;
use num_traits::AsPrimitive;
use num_traits::Bounded;
use num_traits::Float;
use num_traits::FloatConst;
use num_traits::FromPrimitive;
use num_traits::Num;
use num_traits::NumCast;
use num_traits::One;
use num_traits::Pow;
use num_traits::Signed;
use num_traits::ToPrimitive;
use num_traits::Zero;
use num_traits::float::FloatCore;

// masks for the parts of the IEEE 754 float
const SIGN_MASK: u64 = 0x8000000000000000u64;
const EXP_MASK: u64 = 0x7ff0000000000000u64;
const MAN_MASK: u64 = 0x000fffffffffffffu64;

// canonical raw bit patterns (for hashing)
const CANONICAL_NAN_BITS: u64 = 0x7ff8000000000000u64;

#[inline(always)]
fn canonicalize_signed_zero<T: FloatCore>(x: T) -> T {
    // -0.0 + 0.0 == +0.0 under IEEE754 roundTiesToEven rounding mode,
    // which Rust guarantees. Thus by adding a positive zero we
    // canonicalize signed zero without any branches in one instruction.
    x + T::zero()
}

/// A wrapper around floats providing implementations of `Eq`, `Ord`, and `Hash`.
///
/// NaN is sorted as *greater* than all other values and *equal*
/// to itself, in contradiction with the IEEE standard.
///
/// ```
/// use std::f32::NAN;
///
/// use ordered_float::OrderedFloat;
///
/// let mut v = [OrderedFloat(NAN), OrderedFloat(2.0), OrderedFloat(1.0)];
/// v.sort();
/// assert_eq!(v, [OrderedFloat(1.0), OrderedFloat(2.0), OrderedFloat(NAN)]);
/// ```
///
/// Because `OrderedFloat` implements `Ord` and `Eq`, it can be used as a key in a `HashSet`,
/// `HashMap`, `BTreeMap`, or `BTreeSet` (unlike the primitive `f32` or `f64` types):
///
/// ```
/// # use ordered_float::OrderedFloat;
/// # use std::collections::HashSet;
/// # use std::f32::NAN;
///
/// let mut s: HashSet<OrderedFloat<f32>> = HashSet::new();
/// s.insert(OrderedFloat(NAN));
/// assert!(s.contains(&OrderedFloat(NAN)));
/// ```
#[derive(Default, Clone, Copy)]
#[repr(transparent)]
pub struct OrderedFloat<T>(pub T);

impl<T: FloatCore> OrderedFloat<T> {
    /// Get the value out.
    #[inline]
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T: FloatCore> AsRef<T> for OrderedFloat<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T: FloatCore> AsMut<T> for OrderedFloat<T> {
    #[inline]
    fn as_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<'a, T: FloatCore> From<&'a T> for &'a OrderedFloat<T> {
    #[inline]
    fn from(t: &'a T) -> &'a OrderedFloat<T> {
        // Safety: OrderedFloat is #[repr(transparent)] and has no invalid values.
        unsafe { &*(t as *const T as *const OrderedFloat<T>) }
    }
}

impl<'a, T: FloatCore> From<&'a mut T> for &'a mut OrderedFloat<T> {
    #[inline]
    fn from(t: &'a mut T) -> &'a mut OrderedFloat<T> {
        // Safety: OrderedFloat is #[repr(transparent)] and has no invalid values.
        unsafe { &mut *(t as *mut T as *mut OrderedFloat<T>) }
    }
}

impl<T: FloatCore> PartialOrd for OrderedFloat<T> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }

    #[inline]
    fn lt(&self, other: &Self) -> bool {
        !self.ge(other)
    }

    #[inline]
    fn le(&self, other: &Self) -> bool {
        other.ge(self)
    }

    #[inline]
    fn gt(&self, other: &Self) -> bool {
        !other.ge(self)
    }

    #[inline]
    fn ge(&self, other: &Self) -> bool {
        // We consider all NaNs equal, and NaN is the largest possible
        // value. Thus if self is NaN we always return true. Otherwise
        // self >= other is correct. If other is also not NaN it is trivially
        // correct, and if it is we note that nothing can be greater or
        // equal to NaN except NaN itself, which we already handled earlier.
        self.0.is_nan() | (self.0 >= other.0)
    }
}

impl<T: FloatCore> Ord for OrderedFloat<T> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        #[allow(clippy::comparison_chain)]
        if self < other {
            Ordering::Less
        } else if self > other {
            Ordering::Greater
        } else {
            Ordering::Equal
        }
    }
}

impl<T: FloatCore> PartialEq for OrderedFloat<T> {
    #[inline]
    fn eq(&self, other: &OrderedFloat<T>) -> bool {
        if self.0.is_nan() {
            other.0.is_nan()
        } else {
            self.0 == other.0
        }
    }
}

impl<T: FloatCore> PartialEq<T> for OrderedFloat<T> {
    #[inline]
    fn eq(&self, other: &T) -> bool {
        self.0 == *other
    }
}

impl<T: FloatCore> Hash for OrderedFloat<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let bits = if self.is_nan() {
            CANONICAL_NAN_BITS
        } else {
            raw_double_bits(&canonicalize_signed_zero(self.0))
        };

        bits.hash(state)
    }
}

impl<T: fmt::Debug> fmt::Debug for OrderedFloat<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: FloatCore + fmt::Display> fmt::Display for OrderedFloat<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: FloatCore + fmt::LowerExp> fmt::LowerExp for OrderedFloat<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: FloatCore + fmt::UpperExp> fmt::UpperExp for OrderedFloat<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<OrderedFloat<f32>> for f32 {
    #[inline]
    fn from(f: OrderedFloat<f32>) -> f32 {
        f.0
    }
}

impl From<OrderedFloat<f64>> for f64 {
    #[inline]
    fn from(f: OrderedFloat<f64>) -> f64 {
        f.0
    }
}

impl<T: FloatCore> From<T> for OrderedFloat<T> {
    #[inline]
    fn from(val: T) -> Self {
        OrderedFloat(val)
    }
}

impl From<bool> for OrderedFloat<f32> {
    fn from(val: bool) -> Self {
        OrderedFloat(val as u8 as f32)
    }
}

impl From<bool> for OrderedFloat<f64> {
    fn from(val: bool) -> Self {
        OrderedFloat(val as u8 as f64)
    }
}

macro_rules! impl_ordered_float_from {
    ($dst:ty, $src:ty) => {
        impl From<$src> for OrderedFloat<$dst> {
            fn from(val: $src) -> Self {
                OrderedFloat(val.into())
            }
        }
    };
}
impl_ordered_float_from! {f64, i8}
impl_ordered_float_from! {f64, i16}
impl_ordered_float_from! {f64, i32}
impl_ordered_float_from! {f64, u8}
impl_ordered_float_from! {f64, u16}
impl_ordered_float_from! {f64, u32}
impl_ordered_float_from! {f32, i8}
impl_ordered_float_from! {f32, i16}
impl_ordered_float_from! {f32, u8}
impl_ordered_float_from! {f32, u16}

impl<T: FloatCore> Deref for OrderedFloat<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: FloatCore> DerefMut for OrderedFloat<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: FloatCore> Eq for OrderedFloat<T> {}

macro_rules! impl_ordered_float_binop {
    ($imp:ident, $method:ident, $assign_imp:ident, $assign_method:ident) => {
        impl<T: $imp> $imp for OrderedFloat<T> {
            type Output = OrderedFloat<T::Output>;

            #[inline]
            fn $method(self, other: Self) -> Self::Output {
                OrderedFloat((self.0).$method(other.0))
            }
        }

        impl<T: $imp> $imp<T> for OrderedFloat<T> {
            type Output = OrderedFloat<T::Output>;

            #[inline]
            fn $method(self, other: T) -> Self::Output {
                OrderedFloat((self.0).$method(other))
            }
        }

        impl<'a, T> $imp<&'a T> for OrderedFloat<T>
        where T: $imp<&'a T>
        {
            type Output = OrderedFloat<<T as $imp<&'a T>>::Output>;

            #[inline]
            fn $method(self, other: &'a T) -> Self::Output {
                OrderedFloat((self.0).$method(other))
            }
        }

        impl<'a, T> $imp<&'a Self> for OrderedFloat<T>
        where T: $imp<&'a T>
        {
            type Output = OrderedFloat<<T as $imp<&'a T>>::Output>;

            #[inline]
            fn $method(self, other: &'a Self) -> Self::Output {
                OrderedFloat((self.0).$method(&other.0))
            }
        }

        impl<'a, T> $imp<OrderedFloat<T>> for &'a OrderedFloat<T>
        where &'a T: $imp<T>
        {
            type Output = OrderedFloat<<&'a T as $imp<T>>::Output>;

            #[inline]
            fn $method(self, other: OrderedFloat<T>) -> Self::Output {
                OrderedFloat((self.0).$method(other.0))
            }
        }

        impl<'a, T> $imp<T> for &'a OrderedFloat<T>
        where &'a T: $imp<T>
        {
            type Output = OrderedFloat<<&'a T as $imp<T>>::Output>;

            #[inline]
            fn $method(self, other: T) -> Self::Output {
                OrderedFloat((self.0).$method(other))
            }
        }

        impl<'a, T> $imp<&'a T> for &'a OrderedFloat<T>
        where &'a T: $imp
        {
            type Output = OrderedFloat<<&'a T as $imp>::Output>;

            #[inline]
            fn $method(self, other: &'a T) -> Self::Output {
                OrderedFloat((self.0).$method(other))
            }
        }

        impl<T: $assign_imp> $assign_imp<T> for OrderedFloat<T> {
            #[inline]
            fn $assign_method(&mut self, other: T) {
                (self.0).$assign_method(other);
            }
        }

        impl<'a, T: $assign_imp<&'a T>> $assign_imp<&'a T> for OrderedFloat<T> {
            #[inline]
            fn $assign_method(&mut self, other: &'a T) {
                (self.0).$assign_method(other);
            }
        }

        impl<T: $assign_imp> $assign_imp for OrderedFloat<T> {
            #[inline]
            fn $assign_method(&mut self, other: Self) {
                (self.0).$assign_method(other.0);
            }
        }

        impl<'a, T: $assign_imp<&'a T>> $assign_imp<&'a Self> for OrderedFloat<T> {
            #[inline]
            fn $assign_method(&mut self, other: &'a Self) {
                (self.0).$assign_method(&other.0);
            }
        }
    };
}

impl_ordered_float_binop! {Add, add, AddAssign, add_assign}
impl_ordered_float_binop! {Sub, sub, SubAssign, sub_assign}
impl_ordered_float_binop! {Mul, mul, MulAssign, mul_assign}
impl_ordered_float_binop! {Div, div, DivAssign, div_assign}
impl_ordered_float_binop! {Rem, rem, RemAssign, rem_assign}

macro_rules! impl_ordered_float_pow {
    ($inner:ty, $rhs:ty) => {
        impl Pow<$rhs> for OrderedFloat<$inner> {
            type Output = OrderedFloat<$inner>;
            #[inline]
            fn pow(self, rhs: $rhs) -> OrderedFloat<$inner> {
                OrderedFloat(<$inner>::pow(self.0, rhs))
            }
        }

        impl<'a> Pow<&'a $rhs> for OrderedFloat<$inner> {
            type Output = OrderedFloat<$inner>;
            #[inline]
            fn pow(self, rhs: &'a $rhs) -> OrderedFloat<$inner> {
                OrderedFloat(<$inner>::pow(self.0, *rhs))
            }
        }

        impl<'a> Pow<$rhs> for &'a OrderedFloat<$inner> {
            type Output = OrderedFloat<$inner>;
            #[inline]
            fn pow(self, rhs: $rhs) -> OrderedFloat<$inner> {
                OrderedFloat(<$inner>::pow(self.0, rhs))
            }
        }

        impl<'a, 'b> Pow<&'a $rhs> for &'b OrderedFloat<$inner> {
            type Output = OrderedFloat<$inner>;
            #[inline]
            fn pow(self, rhs: &'a $rhs) -> OrderedFloat<$inner> {
                OrderedFloat(<$inner>::pow(self.0, *rhs))
            }
        }
    };
}

impl_ordered_float_pow! {f32, i8}
impl_ordered_float_pow! {f32, i16}
impl_ordered_float_pow! {f32, u8}
impl_ordered_float_pow! {f32, u16}
impl_ordered_float_pow! {f32, i32}
impl_ordered_float_pow! {f64, i8}
impl_ordered_float_pow! {f64, i16}
impl_ordered_float_pow! {f64, u8}
impl_ordered_float_pow! {f64, u16}
impl_ordered_float_pow! {f64, i32}
impl_ordered_float_pow! {f32, f32}
impl_ordered_float_pow! {f64, f32}
impl_ordered_float_pow! {f64, f64}

macro_rules! impl_ordered_float_self_pow {
    ($base:ty, $exp:ty) => {
        impl Pow<OrderedFloat<$exp>> for OrderedFloat<$base> {
            type Output = OrderedFloat<$base>;
            #[inline]
            fn pow(self, rhs: OrderedFloat<$exp>) -> OrderedFloat<$base> {
                OrderedFloat(<$base>::pow(self.0, rhs.0))
            }
        }

        impl<'a> Pow<&'a OrderedFloat<$exp>> for OrderedFloat<$base> {
            type Output = OrderedFloat<$base>;
            #[inline]
            fn pow(self, rhs: &'a OrderedFloat<$exp>) -> OrderedFloat<$base> {
                OrderedFloat(<$base>::pow(self.0, rhs.0))
            }
        }

        impl<'a> Pow<OrderedFloat<$exp>> for &'a OrderedFloat<$base> {
            type Output = OrderedFloat<$base>;
            #[inline]
            fn pow(self, rhs: OrderedFloat<$exp>) -> OrderedFloat<$base> {
                OrderedFloat(<$base>::pow(self.0, rhs.0))
            }
        }

        impl<'a, 'b> Pow<&'a OrderedFloat<$exp>> for &'b OrderedFloat<$base> {
            type Output = OrderedFloat<$base>;
            #[inline]
            fn pow(self, rhs: &'a OrderedFloat<$exp>) -> OrderedFloat<$base> {
                OrderedFloat(<$base>::pow(self.0, rhs.0))
            }
        }
    };
}

impl_ordered_float_self_pow! {f32, f32}
impl_ordered_float_self_pow! {f64, f32}
impl_ordered_float_self_pow! {f64, f64}

/// Adds a float directly.
impl<T: FloatCore + Sum> Sum for OrderedFloat<T> {
    fn sum<I: Iterator<Item = OrderedFloat<T>>>(iter: I) -> Self {
        OrderedFloat(iter.map(|v| v.0).sum())
    }
}

impl<'a, T: FloatCore + Sum + 'a> Sum<&'a OrderedFloat<T>> for OrderedFloat<T> {
    #[inline]
    fn sum<I: Iterator<Item = &'a OrderedFloat<T>>>(iter: I) -> Self {
        iter.cloned().sum()
    }
}

impl<T: FloatCore + Product> Product for OrderedFloat<T> {
    fn product<I: Iterator<Item = OrderedFloat<T>>>(iter: I) -> Self {
        OrderedFloat(iter.map(|v| v.0).product())
    }
}

impl<'a, T: FloatCore + Product + 'a> Product<&'a OrderedFloat<T>> for OrderedFloat<T> {
    #[inline]
    fn product<I: Iterator<Item = &'a OrderedFloat<T>>>(iter: I) -> Self {
        iter.cloned().product()
    }
}

impl<T: FloatCore + Signed> Signed for OrderedFloat<T> {
    #[inline]
    fn abs(&self) -> Self {
        OrderedFloat(self.0.abs())
    }

    fn abs_sub(&self, other: &Self) -> Self {
        OrderedFloat(Signed::abs_sub(&self.0, &other.0))
    }

    #[inline]
    fn signum(&self) -> Self {
        OrderedFloat(self.0.signum())
    }
    #[inline]
    fn is_positive(&self) -> bool {
        #[expect(clippy::disallowed_methods)]
        self.0.is_positive()
    }
    #[inline]
    fn is_negative(&self) -> bool {
        #[expect(clippy::disallowed_methods)]
        self.0.is_negative()
    }
}

impl<T: Bounded> Bounded for OrderedFloat<T> {
    #[inline]
    fn min_value() -> Self {
        OrderedFloat(T::min_value())
    }

    #[inline]
    fn max_value() -> Self {
        OrderedFloat(T::max_value())
    }
}

unsafe impl Zeroable for OrderedFloat<f32> {}
unsafe impl Pod for OrderedFloat<f32> {}
unsafe impl Zeroable for OrderedFloat<f64> {}
unsafe impl Pod for OrderedFloat<f64> {}

impl<T: FromStr> FromStr for OrderedFloat<T> {
    type Err = T::Err;

    /// Convert a &str to `OrderedFloat`. Returns an error if the string fails to parse.
    ///
    /// ```
    /// use ordered_float::OrderedFloat;
    ///
    /// assert!("-10".parse::<OrderedFloat<f32>>().is_ok());
    /// assert!("abc".parse::<OrderedFloat<f32>>().is_err());
    /// assert!("NaN".parse::<OrderedFloat<f32>>().is_ok());
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        T::from_str(s).map(OrderedFloat)
    }
}

impl<T: Neg> Neg for OrderedFloat<T> {
    type Output = OrderedFloat<T::Output>;

    #[inline]
    fn neg(self) -> Self::Output {
        OrderedFloat(-self.0)
    }
}

impl<'a, T> Neg for &'a OrderedFloat<T>
where &'a T: Neg
{
    type Output = OrderedFloat<<&'a T as Neg>::Output>;

    #[inline]
    fn neg(self) -> Self::Output {
        OrderedFloat(-(&self.0))
    }
}

impl<T: Zero> Zero for OrderedFloat<T> {
    #[inline]
    fn zero() -> Self {
        OrderedFloat(T::zero())
    }

    #[inline]
    fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

impl<T: One> One for OrderedFloat<T> {
    #[inline]
    fn one() -> Self {
        OrderedFloat(T::one())
    }
}

impl<T: NumCast> NumCast for OrderedFloat<T> {
    #[inline]
    fn from<F: ToPrimitive>(n: F) -> Option<Self> {
        T::from(n).map(OrderedFloat)
    }
}

macro_rules! impl_as_primitive {
    (@ (NotNan<$T: ty>) => $(#[$cfg:meta])* impl (NotNan<$U: ty>) ) => {
        $(#[$cfg])*
        impl AsPrimitive<NotNan<$U>> for NotNan<$T> {
            #[inline] fn as_(self) -> NotNan<$U> {
                // Safety: `NotNan` guarantees that the value is not NaN.
                unsafe {NotNan::new_unchecked(self.0 as $U) }
            }
        }
    };
    (@ ($T: ty) => $(#[$cfg:meta])* impl (NotNan<$U: ty>) ) => {
        $(#[$cfg])*
        impl AsPrimitive<NotNan<$U>> for $T {
            #[inline] fn as_(self) -> NotNan<$U> { NotNan(self as $U) }
        }
    };
    (@ (NotNan<$T: ty>) => $(#[$cfg:meta])* impl ($U: ty) ) => {
        $(#[$cfg])*
        impl AsPrimitive<$U> for NotNan<$T> {
            #[inline] fn as_(self) -> $U { self.0 as $U }
        }
    };
    (@ (OrderedFloat<$T: ty>) => $(#[$cfg:meta])* impl (OrderedFloat<$U: ty>) ) => {
        $(#[$cfg])*
        impl AsPrimitive<OrderedFloat<$U>> for OrderedFloat<$T> {
            #[inline] fn as_(self) -> OrderedFloat<$U> { OrderedFloat(self.0 as $U) }
        }
    };
    (@ ($T: ty) => $(#[$cfg:meta])* impl (OrderedFloat<$U: ty>) ) => {
        $(#[$cfg])*
        impl AsPrimitive<OrderedFloat<$U>> for $T {
            #[inline] fn as_(self) -> OrderedFloat<$U> { OrderedFloat(self as $U) }
        }
    };
    (@ (OrderedFloat<$T: ty>) => $(#[$cfg:meta])* impl ($U: ty) ) => {
        $(#[$cfg])*
        impl AsPrimitive<$U> for OrderedFloat<$T> {
            #[inline] fn as_(self) -> $U { self.0 as $U }
        }
    };
    ($T: tt => { $( $U: tt ),* } ) => {$(
        impl_as_primitive!(@ $T => impl $U);
    )*};
}

impl_as_primitive!((OrderedFloat<f32>) => { (OrderedFloat<f32>), (OrderedFloat<f64>) });
impl_as_primitive!((OrderedFloat<f64>) => { (OrderedFloat<f32>), (OrderedFloat<f64>) });

impl_as_primitive!((NotNan<f32>) => { (NotNan<f32>), (NotNan<f64>) });
impl_as_primitive!((NotNan<f64>) => { (NotNan<f32>), (NotNan<f64>) });

impl_as_primitive!((u8) => { (OrderedFloat<f32>), (OrderedFloat<f64>) });
impl_as_primitive!((i8) => { (OrderedFloat<f32>), (OrderedFloat<f64>) });
impl_as_primitive!((u16) => { (OrderedFloat<f32>), (OrderedFloat<f64>) });
impl_as_primitive!((i16) => { (OrderedFloat<f32>), (OrderedFloat<f64>) });
impl_as_primitive!((u32) => { (OrderedFloat<f32>), (OrderedFloat<f64>) });
impl_as_primitive!((i32) => { (OrderedFloat<f32>), (OrderedFloat<f64>) });
impl_as_primitive!((u64) => { (OrderedFloat<f32>), (OrderedFloat<f64>) });
impl_as_primitive!((i64) => { (OrderedFloat<f32>), (OrderedFloat<f64>) });
impl_as_primitive!((usize) => { (OrderedFloat<f32>), (OrderedFloat<f64>) });
impl_as_primitive!((isize) => { (OrderedFloat<f32>), (OrderedFloat<f64>) });
impl_as_primitive!((f32) => { (OrderedFloat<f32>), (OrderedFloat<f64>) });
impl_as_primitive!((f64) => { (OrderedFloat<f32>), (OrderedFloat<f64>) });

impl_as_primitive!((u8) => { (NotNan<f32>), (NotNan<f64>) });
impl_as_primitive!((i8) => { (NotNan<f32>), (NotNan<f64>) });
impl_as_primitive!((u16) => { (NotNan<f32>), (NotNan<f64>) });
impl_as_primitive!((i16) => { (NotNan<f32>), (NotNan<f64>) });
impl_as_primitive!((u32) => { (NotNan<f32>), (NotNan<f64>) });
impl_as_primitive!((i32) => { (NotNan<f32>), (NotNan<f64>) });
impl_as_primitive!((u64) => { (NotNan<f32>), (NotNan<f64>) });
impl_as_primitive!((i64) => { (NotNan<f32>), (NotNan<f64>) });
impl_as_primitive!((usize) => { (NotNan<f32>), (NotNan<f64>) });
impl_as_primitive!((isize) => { (NotNan<f32>), (NotNan<f64>) });

impl_as_primitive!((OrderedFloat<f32>) => { (u8), (u16), (u32), (u64), (usize), (i8), (i16), (i32), (i64), (isize), (f32), (f64) });
impl_as_primitive!((OrderedFloat<f64>) => { (u8), (u16), (u32), (u64), (usize), (i8), (i16), (i32), (i64), (isize), (f32), (f64) });

impl_as_primitive!((NotNan<f32>) => { (u8), (u16), (u32), (u64), (usize), (i8), (i16), (i32), (i64), (isize), (f32), (f64) });
impl_as_primitive!((NotNan<f64>) => { (u8), (u16), (u32), (u64), (usize), (i8), (i16), (i32), (i64), (isize), (f32), (f64) });

impl<T: FromPrimitive> FromPrimitive for OrderedFloat<T> {
    fn from_i64(n: i64) -> Option<Self> {
        T::from_i64(n).map(OrderedFloat)
    }
    fn from_u64(n: u64) -> Option<Self> {
        T::from_u64(n).map(OrderedFloat)
    }
    fn from_isize(n: isize) -> Option<Self> {
        T::from_isize(n).map(OrderedFloat)
    }
    fn from_i8(n: i8) -> Option<Self> {
        T::from_i8(n).map(OrderedFloat)
    }
    fn from_i16(n: i16) -> Option<Self> {
        T::from_i16(n).map(OrderedFloat)
    }
    fn from_i32(n: i32) -> Option<Self> {
        T::from_i32(n).map(OrderedFloat)
    }
    fn from_usize(n: usize) -> Option<Self> {
        T::from_usize(n).map(OrderedFloat)
    }
    fn from_u8(n: u8) -> Option<Self> {
        T::from_u8(n).map(OrderedFloat)
    }
    fn from_u16(n: u16) -> Option<Self> {
        T::from_u16(n).map(OrderedFloat)
    }
    fn from_u32(n: u32) -> Option<Self> {
        T::from_u32(n).map(OrderedFloat)
    }
    fn from_f32(n: f32) -> Option<Self> {
        T::from_f32(n).map(OrderedFloat)
    }
    fn from_f64(n: f64) -> Option<Self> {
        T::from_f64(n).map(OrderedFloat)
    }
}

impl<T: ToPrimitive> ToPrimitive for OrderedFloat<T> {
    fn to_i64(&self) -> Option<i64> {
        self.0.to_i64()
    }
    fn to_u64(&self) -> Option<u64> {
        self.0.to_u64()
    }
    fn to_isize(&self) -> Option<isize> {
        self.0.to_isize()
    }
    fn to_i8(&self) -> Option<i8> {
        self.0.to_i8()
    }
    fn to_i16(&self) -> Option<i16> {
        self.0.to_i16()
    }
    fn to_i32(&self) -> Option<i32> {
        self.0.to_i32()
    }
    fn to_usize(&self) -> Option<usize> {
        self.0.to_usize()
    }
    fn to_u8(&self) -> Option<u8> {
        self.0.to_u8()
    }
    fn to_u16(&self) -> Option<u16> {
        self.0.to_u16()
    }
    fn to_u32(&self) -> Option<u32> {
        self.0.to_u32()
    }
    fn to_f32(&self) -> Option<f32> {
        self.0.to_f32()
    }
    fn to_f64(&self) -> Option<f64> {
        self.0.to_f64()
    }
}

impl<T: FloatCore> FloatCore for OrderedFloat<T> {
    fn nan() -> Self {
        OrderedFloat(T::nan())
    }
    fn infinity() -> Self {
        OrderedFloat(T::infinity())
    }
    fn neg_infinity() -> Self {
        OrderedFloat(T::neg_infinity())
    }
    fn neg_zero() -> Self {
        OrderedFloat(T::neg_zero())
    }
    fn min_value() -> Self {
        OrderedFloat(T::min_value())
    }
    fn min_positive_value() -> Self {
        OrderedFloat(T::min_positive_value())
    }
    fn max_value() -> Self {
        OrderedFloat(T::max_value())
    }
    fn is_nan(self) -> bool {
        self.0.is_nan()
    }
    fn is_infinite(self) -> bool {
        self.0.is_infinite()
    }
    fn is_finite(self) -> bool {
        self.0.is_finite()
    }
    fn is_normal(self) -> bool {
        self.0.is_normal()
    }
    fn classify(self) -> FpCategory {
        self.0.classify()
    }
    fn floor(self) -> Self {
        OrderedFloat(self.0.floor())
    }
    fn ceil(self) -> Self {
        OrderedFloat(self.0.ceil())
    }
    fn round(self) -> Self {
        OrderedFloat(self.0.round())
    }
    fn trunc(self) -> Self {
        OrderedFloat(self.0.trunc())
    }
    fn fract(self) -> Self {
        OrderedFloat(self.0.fract())
    }
    fn abs(self) -> Self {
        OrderedFloat(self.0.abs())
    }
    fn signum(self) -> Self {
        OrderedFloat(self.0.signum())
    }
    fn is_sign_positive(self) -> bool {
        self.0.is_sign_positive()
    }
    fn is_sign_negative(self) -> bool {
        self.0.is_sign_negative()
    }
    fn recip(self) -> Self {
        OrderedFloat(self.0.recip())
    }
    fn powi(self, n: i32) -> Self {
        OrderedFloat(self.0.powi(n))
    }
    fn integer_decode(self) -> (u64, i16, i8) {
        self.0.integer_decode()
    }
    fn epsilon() -> Self {
        OrderedFloat(T::epsilon())
    }
    fn to_degrees(self) -> Self {
        OrderedFloat(self.0.to_degrees())
    }
    fn to_radians(self) -> Self {
        OrderedFloat(self.0.to_radians())
    }
}

impl<T: Float + FloatCore> Float for OrderedFloat<T> {
    fn nan() -> Self {
        OrderedFloat(<T as Float>::nan())
    }
    fn infinity() -> Self {
        OrderedFloat(<T as Float>::infinity())
    }
    fn neg_infinity() -> Self {
        OrderedFloat(<T as Float>::neg_infinity())
    }
    fn neg_zero() -> Self {
        OrderedFloat(<T as Float>::neg_zero())
    }
    fn min_value() -> Self {
        OrderedFloat(<T as Float>::min_value())
    }
    fn min_positive_value() -> Self {
        OrderedFloat(<T as Float>::min_positive_value())
    }
    fn max_value() -> Self {
        OrderedFloat(<T as Float>::max_value())
    }
    fn is_nan(self) -> bool {
        Float::is_nan(self.0)
    }
    fn is_infinite(self) -> bool {
        Float::is_infinite(self.0)
    }
    fn is_finite(self) -> bool {
        Float::is_finite(self.0)
    }
    fn is_normal(self) -> bool {
        Float::is_normal(self.0)
    }
    fn classify(self) -> FpCategory {
        Float::classify(self.0)
    }
    fn floor(self) -> Self {
        OrderedFloat(Float::floor(self.0))
    }
    fn ceil(self) -> Self {
        OrderedFloat(Float::ceil(self.0))
    }
    fn round(self) -> Self {
        OrderedFloat(Float::round(self.0))
    }
    fn trunc(self) -> Self {
        OrderedFloat(Float::trunc(self.0))
    }
    fn fract(self) -> Self {
        OrderedFloat(Float::fract(self.0))
    }
    fn abs(self) -> Self {
        OrderedFloat(Float::abs(self.0))
    }
    fn signum(self) -> Self {
        OrderedFloat(Float::signum(self.0))
    }
    fn is_sign_positive(self) -> bool {
        Float::is_sign_positive(self.0)
    }
    fn is_sign_negative(self) -> bool {
        Float::is_sign_negative(self.0)
    }
    fn mul_add(self, a: Self, b: Self) -> Self {
        OrderedFloat(self.0.mul_add(a.0, b.0))
    }
    fn recip(self) -> Self {
        OrderedFloat(Float::recip(self.0))
    }
    fn powi(self, n: i32) -> Self {
        OrderedFloat(Float::powi(self.0, n))
    }
    fn powf(self, n: Self) -> Self {
        OrderedFloat(self.0.powf(n.0))
    }
    fn sqrt(self) -> Self {
        OrderedFloat(self.0.sqrt())
    }
    fn exp(self) -> Self {
        OrderedFloat(self.0.exp())
    }
    fn exp2(self) -> Self {
        OrderedFloat(self.0.exp2())
    }
    fn ln(self) -> Self {
        OrderedFloat(self.0.ln())
    }
    fn log(self, base: Self) -> Self {
        OrderedFloat(self.0.log(base.0))
    }
    fn log2(self) -> Self {
        OrderedFloat(self.0.log2())
    }
    fn log10(self) -> Self {
        OrderedFloat(self.0.log10())
    }
    fn max(self, other: Self) -> Self {
        OrderedFloat(Float::max(self.0, other.0))
    }
    fn min(self, other: Self) -> Self {
        OrderedFloat(Float::min(self.0, other.0))
    }
    fn abs_sub(self, other: Self) -> Self {
        OrderedFloat(self.0.abs_sub(other.0))
    }
    fn cbrt(self) -> Self {
        OrderedFloat(self.0.cbrt())
    }
    fn hypot(self, other: Self) -> Self {
        OrderedFloat(self.0.hypot(other.0))
    }
    fn sin(self) -> Self {
        OrderedFloat(self.0.sin())
    }
    fn cos(self) -> Self {
        OrderedFloat(self.0.cos())
    }
    fn tan(self) -> Self {
        OrderedFloat(self.0.tan())
    }
    fn asin(self) -> Self {
        OrderedFloat(self.0.asin())
    }
    fn acos(self) -> Self {
        OrderedFloat(self.0.acos())
    }
    fn atan(self) -> Self {
        OrderedFloat(self.0.atan())
    }
    fn atan2(self, other: Self) -> Self {
        OrderedFloat(self.0.atan2(other.0))
    }
    fn sin_cos(self) -> (Self, Self) {
        let (a, b) = self.0.sin_cos();
        (OrderedFloat(a), OrderedFloat(b))
    }
    fn exp_m1(self) -> Self {
        OrderedFloat(self.0.exp_m1())
    }
    fn ln_1p(self) -> Self {
        OrderedFloat(self.0.ln_1p())
    }
    fn sinh(self) -> Self {
        OrderedFloat(self.0.sinh())
    }
    fn cosh(self) -> Self {
        OrderedFloat(self.0.cosh())
    }
    fn tanh(self) -> Self {
        OrderedFloat(self.0.tanh())
    }
    fn asinh(self) -> Self {
        OrderedFloat(self.0.asinh())
    }
    fn acosh(self) -> Self {
        OrderedFloat(self.0.acosh())
    }
    fn atanh(self) -> Self {
        OrderedFloat(self.0.atanh())
    }
    fn integer_decode(self) -> (u64, i16, i8) {
        Float::integer_decode(self.0)
    }
    fn epsilon() -> Self {
        OrderedFloat(<T as Float>::epsilon())
    }
    fn to_degrees(self) -> Self {
        OrderedFloat(Float::to_degrees(self.0))
    }
    fn to_radians(self) -> Self {
        OrderedFloat(Float::to_radians(self.0))
    }
}

impl<T: FloatCore + Num> Num for OrderedFloat<T> {
    type FromStrRadixErr = T::FromStrRadixErr;
    fn from_str_radix(str: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
        T::from_str_radix(str, radix).map(OrderedFloat)
    }
}

/// A wrapper around floats providing an implementation of `Eq`, `Ord` and `Hash`.
///
/// A NaN value cannot be stored in this type.
///
/// ```
/// use ordered_float::NotNan;
///
/// let mut v = [NotNan::new(2.0).unwrap(), NotNan::new(1.0).unwrap()];
/// v.sort();
/// assert_eq!(v, [1.0, 2.0]);
/// ```
///
/// Because `NotNan` implements `Ord` and `Eq`, it can be used as a key in a `HashSet`,
/// `HashMap`, `BTreeMap`, or `BTreeSet` (unlike the primitive `f32` or `f64` types):
///
/// ```
/// # use ordered_float::NotNan;
/// # use std::collections::HashSet;
///
/// let mut s: HashSet<NotNan<f32>> = HashSet::new();
/// let key = NotNan::new(1.0).unwrap();
/// s.insert(key);
/// assert!(s.contains(&key));
/// ```
///
/// Arithmetic on NotNan values will panic if it produces a NaN value:
///
/// ```should_panic
/// # use ordered_float::NotNan;
/// let a = NotNan::new(std::f32::INFINITY).unwrap();
/// let b = NotNan::new(std::f32::NEG_INFINITY).unwrap();
///
/// // This will panic:
/// let c = a + b;
/// ```
#[allow(clippy::derive_ord_xor_partial_ord)]
#[derive(PartialOrd, PartialEq, Default, Clone, Copy)]
#[repr(transparent)]
pub struct NotNan<T>(T);

impl<T: FloatCore> NotNan<T> {
    /// Create a `NotNan` value.
    ///
    /// Returns `Err` if `val` is NaN
    pub fn new(val: T) -> Result<Self, FloatIsNan> {
        match val {
            ref val if val.is_nan() => Err(FloatIsNan),
            val => Ok(NotNan(val)),
        }
    }
}

impl<T> NotNan<T> {
    /// Get the value out.
    #[inline]
    pub fn into_inner(self) -> T {
        self.0
    }

    /// Create a `NotNan` value from a value that is guaranteed to not be NaN
    ///
    /// # Safety
    ///
    /// Behaviour is undefined if `val` is NaN
    #[inline]
    pub const unsafe fn new_unchecked(val: T) -> Self {
        NotNan(val)
    }

    /// Create a `NotNan` value from a value that is guaranteed to not be NaN
    ///
    /// # Safety
    ///
    /// Behaviour is undefined if `val` is NaN
    #[deprecated(
        since = "2.5.0",
        note = "Please use the new_unchecked function instead."
    )]
    #[inline]
    pub const unsafe fn unchecked_new(val: T) -> Self {
        unsafe { Self::new_unchecked(val) }
    }
}

impl<T: FloatCore> AsRef<T> for NotNan<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl Borrow<f32> for NotNan<f32> {
    #[inline]
    fn borrow(&self) -> &f32 {
        &self.0
    }
}

impl Borrow<f64> for NotNan<f64> {
    #[inline]
    fn borrow(&self) -> &f64 {
        &self.0
    }
}

#[allow(clippy::derive_ord_xor_partial_ord)]
impl<T: FloatCore> Ord for NotNan<T> {
    fn cmp(&self, other: &NotNan<T>) -> Ordering {
        // Can't use unreachable_unchecked because unsafe code can't depend on FloatCore impl.
        // https://github.com/reem/rust-ordered-float/issues/150
        self.partial_cmp(other)
            .expect("partial_cmp failed for non-NaN value")
    }
}

impl<T: FloatCore> Hash for NotNan<T> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        let bits = raw_double_bits(&canonicalize_signed_zero(self.0));
        bits.hash(state)
    }
}

impl<T: fmt::Debug> fmt::Debug for NotNan<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: FloatCore + fmt::Display> fmt::Display for NotNan<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl NotNan<f64> {
    /// Converts this [`NotNan`]`<`[`f64`]`>` to a [`NotNan`]`<`[`f32`]`>` while giving up on
    /// precision, [using `roundTiesToEven` as rounding mode, yielding `Infinity` on
    /// overflow](https://doc.rust-lang.org/reference/expressions/operator-expr.html#semantics).
    ///
    /// Note: For the reverse conversion (from `NotNan<f32>` to `NotNan<f64>`), you can use
    /// `.into()`.
    pub fn as_f32(self) -> NotNan<f32> {
        // This is not destroying invariants, as it is a pure rounding operation. The only two special
        // cases are where f32 would be overflowing, then the operation yields Infinity, or where
        // the input is already NaN, in which case the invariant is already broken elsewhere.
        NotNan(self.0 as f32)
    }
}

impl From<NotNan<f32>> for f32 {
    #[inline]
    fn from(value: NotNan<f32>) -> Self {
        value.0
    }
}

impl From<NotNan<f64>> for f64 {
    #[inline]
    fn from(value: NotNan<f64>) -> Self {
        value.0
    }
}

impl TryFrom<f32> for NotNan<f32> {
    type Error = FloatIsNan;
    #[inline]
    fn try_from(v: f32) -> Result<Self, Self::Error> {
        NotNan::new(v)
    }
}

impl TryFrom<f64> for NotNan<f64> {
    type Error = FloatIsNan;
    #[inline]
    fn try_from(v: f64) -> Result<Self, Self::Error> {
        NotNan::new(v)
    }
}

macro_rules! impl_from_int_primitive {
    ($primitive:ty, $inner:ty) => {
        impl From<$primitive> for NotNan<$inner> {
            fn from(source: $primitive) -> Self {
                // the primitives with which this macro will be called cannot hold a value that
                // f64::from would convert to NaN, so this does not hurt invariants
                NotNan(<$inner as From<$primitive>>::from(source))
            }
        }
    };
}

impl_from_int_primitive!(i8, f64);
impl_from_int_primitive!(i16, f64);
impl_from_int_primitive!(i32, f64);
impl_from_int_primitive!(u8, f64);
impl_from_int_primitive!(u16, f64);
impl_from_int_primitive!(u32, f64);

impl_from_int_primitive!(i8, f32);
impl_from_int_primitive!(i16, f32);
impl_from_int_primitive!(u8, f32);
impl_from_int_primitive!(u16, f32);

impl From<NotNan<f32>> for NotNan<f64> {
    #[inline]
    fn from(v: NotNan<f32>) -> NotNan<f64> {
        unsafe { NotNan::new_unchecked(v.0 as f64) }
    }
}

impl<T: FloatCore> Deref for NotNan<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: FloatCore + PartialEq> Eq for NotNan<T> {}

impl<T: FloatCore> PartialEq<T> for NotNan<T> {
    #[inline]
    fn eq(&self, other: &T) -> bool {
        self.0 == *other
    }
}

/// Adds a float directly.
///
/// Panics if the provided value is NaN or the computation results in NaN
impl<T: FloatCore> Add<T> for NotNan<T> {
    type Output = Self;

    #[inline]
    fn add(self, other: T) -> Self {
        NotNan::new(self.0 + other).expect("Addition resulted in NaN")
    }
}

/// Adds a float directly.
///
/// Panics if the provided value is NaN.
impl<T: FloatCore + Sum> Sum for NotNan<T> {
    fn sum<I: Iterator<Item = NotNan<T>>>(iter: I) -> Self {
        NotNan::new(iter.map(|v| v.0).sum()).expect("Sum resulted in NaN")
    }
}

impl<'a, T: FloatCore + Sum + 'a> Sum<&'a NotNan<T>> for NotNan<T> {
    #[inline]
    fn sum<I: Iterator<Item = &'a NotNan<T>>>(iter: I) -> Self {
        iter.cloned().sum()
    }
}

/// Subtracts a float directly.
///
/// Panics if the provided value is NaN or the computation results in NaN
impl<T: FloatCore> Sub<T> for NotNan<T> {
    type Output = Self;

    #[inline]
    fn sub(self, other: T) -> Self {
        NotNan::new(self.0 - other).expect("Subtraction resulted in NaN")
    }
}

/// Multiplies a float directly.
///
/// Panics if the provided value is NaN or the computation results in NaN
impl<T: FloatCore> Mul<T> for NotNan<T> {
    type Output = Self;

    #[inline]
    fn mul(self, other: T) -> Self {
        NotNan::new(self.0 * other).expect("Multiplication resulted in NaN")
    }
}

impl<T: FloatCore + Product> Product for NotNan<T> {
    fn product<I: Iterator<Item = NotNan<T>>>(iter: I) -> Self {
        NotNan::new(iter.map(|v| v.0).product()).expect("Product resulted in NaN")
    }
}

impl<'a, T: FloatCore + Product + 'a> Product<&'a NotNan<T>> for NotNan<T> {
    #[inline]
    fn product<I: Iterator<Item = &'a NotNan<T>>>(iter: I) -> Self {
        iter.cloned().product()
    }
}

/// Divides a float directly.
///
/// Panics if the provided value is NaN or the computation results in NaN
impl<T: FloatCore> Div<T> for NotNan<T> {
    type Output = Self;

    #[inline]
    fn div(self, other: T) -> Self {
        NotNan::new(self.0 / other).expect("Division resulted in NaN")
    }
}

/// Calculates `%` with a float directly.
///
/// Panics if the provided value is NaN or the computation results in NaN
impl<T: FloatCore> Rem<T> for NotNan<T> {
    type Output = Self;

    #[inline]
    fn rem(self, other: T) -> Self {
        NotNan::new(self.0 % other).expect("Rem resulted in NaN")
    }
}

macro_rules! impl_not_nan_binop {
    ($imp:ident, $method:ident, $assign_imp:ident, $assign_method:ident) => {
        impl<T: FloatCore> $imp for NotNan<T> {
            type Output = Self;

            #[inline]
            fn $method(self, other: Self) -> Self {
                self.$method(other.0)
            }
        }

        impl<T: FloatCore> $imp<&T> for NotNan<T> {
            type Output = NotNan<T>;

            #[inline]
            fn $method(self, other: &T) -> Self::Output {
                self.$method(*other)
            }
        }

        impl<T: FloatCore> $imp<&Self> for NotNan<T> {
            type Output = NotNan<T>;

            #[inline]
            fn $method(self, other: &Self) -> Self::Output {
                self.$method(other.0)
            }
        }

        impl<T: FloatCore> $imp for &NotNan<T> {
            type Output = NotNan<T>;

            #[inline]
            fn $method(self, other: Self) -> Self::Output {
                (*self).$method(other.0)
            }
        }

        impl<T: FloatCore> $imp<NotNan<T>> for &NotNan<T> {
            type Output = NotNan<T>;

            #[inline]
            fn $method(self, other: NotNan<T>) -> Self::Output {
                (*self).$method(other.0)
            }
        }

        impl<T: FloatCore> $imp<T> for &NotNan<T> {
            type Output = NotNan<T>;

            #[inline]
            fn $method(self, other: T) -> Self::Output {
                (*self).$method(other)
            }
        }

        impl<T: FloatCore> $imp<&T> for &NotNan<T> {
            type Output = NotNan<T>;

            #[inline]
            fn $method(self, other: &T) -> Self::Output {
                (*self).$method(*other)
            }
        }

        impl<T: FloatCore + $assign_imp> $assign_imp<T> for NotNan<T> {
            #[inline]
            fn $assign_method(&mut self, other: T) {
                *self = (*self).$method(other);
            }
        }

        impl<T: FloatCore + $assign_imp> $assign_imp<&T> for NotNan<T> {
            #[inline]
            fn $assign_method(&mut self, other: &T) {
                *self = (*self).$method(*other);
            }
        }

        impl<T: FloatCore + $assign_imp> $assign_imp for NotNan<T> {
            #[inline]
            fn $assign_method(&mut self, other: Self) {
                (*self).$assign_method(other.0);
            }
        }

        impl<T: FloatCore + $assign_imp> $assign_imp<&Self> for NotNan<T> {
            #[inline]
            fn $assign_method(&mut self, other: &Self) {
                (*self).$assign_method(other.0);
            }
        }
    };
}

impl_not_nan_binop! {Add, add, AddAssign, add_assign}
impl_not_nan_binop! {Sub, sub, SubAssign, sub_assign}
impl_not_nan_binop! {Mul, mul, MulAssign, mul_assign}
impl_not_nan_binop! {Div, div, DivAssign, div_assign}
impl_not_nan_binop! {Rem, rem, RemAssign, rem_assign}

// Will panic if NaN value is return from the operation
macro_rules! impl_not_nan_pow {
    ($inner:ty, $rhs:ty) => {
        impl Pow<$rhs> for NotNan<$inner> {
            type Output = NotNan<$inner>;
            #[inline]
            fn pow(self, rhs: $rhs) -> NotNan<$inner> {
                NotNan::new(<$inner>::pow(self.0, rhs)).expect("Pow resulted in NaN")
            }
        }

        impl<'a> Pow<&'a $rhs> for NotNan<$inner> {
            type Output = NotNan<$inner>;
            #[inline]
            fn pow(self, rhs: &'a $rhs) -> NotNan<$inner> {
                NotNan::new(<$inner>::pow(self.0, *rhs)).expect("Pow resulted in NaN")
            }
        }

        impl<'a> Pow<$rhs> for &'a NotNan<$inner> {
            type Output = NotNan<$inner>;
            #[inline]
            fn pow(self, rhs: $rhs) -> NotNan<$inner> {
                NotNan::new(<$inner>::pow(self.0, rhs)).expect("Pow resulted in NaN")
            }
        }

        impl<'a, 'b> Pow<&'a $rhs> for &'b NotNan<$inner> {
            type Output = NotNan<$inner>;
            #[inline]
            fn pow(self, rhs: &'a $rhs) -> NotNan<$inner> {
                NotNan::new(<$inner>::pow(self.0, *rhs)).expect("Pow resulted in NaN")
            }
        }
    };
}

impl_not_nan_pow! {f32, i8}
impl_not_nan_pow! {f32, i16}
impl_not_nan_pow! {f32, u8}
impl_not_nan_pow! {f32, u16}
impl_not_nan_pow! {f32, i32}
impl_not_nan_pow! {f64, i8}
impl_not_nan_pow! {f64, i16}
impl_not_nan_pow! {f64, u8}
impl_not_nan_pow! {f64, u16}
impl_not_nan_pow! {f64, i32}
impl_not_nan_pow! {f32, f32}
impl_not_nan_pow! {f64, f32}
impl_not_nan_pow! {f64, f64}

// This also should panic on NaN
macro_rules! impl_not_nan_self_pow {
    ($base:ty, $exp:ty) => {
        impl Pow<NotNan<$exp>> for NotNan<$base> {
            type Output = NotNan<$base>;
            #[inline]
            fn pow(self, rhs: NotNan<$exp>) -> NotNan<$base> {
                NotNan::new(self.0.pow(rhs.0)).expect("Pow resulted in NaN")
            }
        }

        impl<'a> Pow<&'a NotNan<$exp>> for NotNan<$base> {
            type Output = NotNan<$base>;
            #[inline]
            fn pow(self, rhs: &'a NotNan<$exp>) -> NotNan<$base> {
                NotNan::new(self.0.pow(rhs.0)).expect("Pow resulted in NaN")
            }
        }

        impl<'a> Pow<NotNan<$exp>> for &'a NotNan<$base> {
            type Output = NotNan<$base>;
            #[inline]
            fn pow(self, rhs: NotNan<$exp>) -> NotNan<$base> {
                NotNan::new(self.0.pow(rhs.0)).expect("Pow resulted in NaN")
            }
        }

        impl<'a, 'b> Pow<&'a NotNan<$exp>> for &'b NotNan<$base> {
            type Output = NotNan<$base>;
            #[inline]
            fn pow(self, rhs: &'a NotNan<$exp>) -> NotNan<$base> {
                NotNan::new(self.0.pow(rhs.0)).expect("Pow resulted in NaN")
            }
        }
    };
}

impl_not_nan_self_pow! {f32, f32}
impl_not_nan_self_pow! {f64, f32}
impl_not_nan_self_pow! {f64, f64}

impl<T: FloatCore> Neg for NotNan<T> {
    type Output = Self;

    #[inline]
    fn neg(self) -> Self {
        NotNan(-self.0)
    }
}

impl<T: FloatCore> Neg for &NotNan<T> {
    type Output = NotNan<T>;

    #[inline]
    fn neg(self) -> Self::Output {
        NotNan(-self.0)
    }
}

/// An error indicating an attempt to construct NotNan from a NaN
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct FloatIsNan;

impl Error for FloatIsNan {
    fn description(&self) -> &str {
        "NotNan constructed with NaN"
    }
}

impl fmt::Display for FloatIsNan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "NotNan constructed with NaN")
    }
}

impl From<FloatIsNan> for std::io::Error {
    #[inline]
    fn from(e: FloatIsNan) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, e)
    }
}

#[inline]
/// Used for hashing. Input must not be zero or NaN.
fn raw_double_bits<F: FloatCore>(f: &F) -> u64 {
    let (man, exp, sign) = f.integer_decode();
    let exp_u64 = exp as u16 as u64;
    let sign_u64 = (sign > 0) as u64;
    (man & MAN_MASK) | ((exp_u64 << 52) & EXP_MASK) | ((sign_u64 << 63) & SIGN_MASK)
}

impl<T: FloatCore> Zero for NotNan<T> {
    #[inline]
    fn zero() -> Self {
        NotNan(T::zero())
    }

    #[inline]
    fn is_zero(&self) -> bool {
        self.0.is_zero()
    }
}

impl<T: FloatCore> One for NotNan<T> {
    #[inline]
    fn one() -> Self {
        NotNan(T::one())
    }
}

impl<T: FloatCore> Bounded for NotNan<T> {
    #[inline]
    fn min_value() -> Self {
        NotNan(T::min_value())
    }

    #[inline]
    fn max_value() -> Self {
        NotNan(T::max_value())
    }
}

impl<T: FloatCore + FromStr> FromStr for NotNan<T> {
    type Err = ParseNotNanError<T::Err>;

    /// Convert a &str to `NotNan`. Returns an error if the string fails to parse,
    /// or if the resulting value is NaN
    ///
    /// ```
    /// use ordered_float::NotNan;
    ///
    /// assert!("-10".parse::<NotNan<f32>>().is_ok());
    /// assert!("abc".parse::<NotNan<f32>>().is_err());
    /// assert!("NaN".parse::<NotNan<f32>>().is_err());
    /// ```
    fn from_str(src: &str) -> Result<Self, Self::Err> {
        src.parse()
            .map_err(ParseNotNanError::ParseFloatError)
            .and_then(|f| NotNan::new(f).map_err(|_| ParseNotNanError::IsNaN))
    }
}

impl<T: FloatCore + FromPrimitive> FromPrimitive for NotNan<T> {
    fn from_i64(n: i64) -> Option<Self> {
        T::from_i64(n).and_then(|n| NotNan::new(n).ok())
    }
    fn from_u64(n: u64) -> Option<Self> {
        T::from_u64(n).and_then(|n| NotNan::new(n).ok())
    }

    fn from_isize(n: isize) -> Option<Self> {
        T::from_isize(n).and_then(|n| NotNan::new(n).ok())
    }
    fn from_i8(n: i8) -> Option<Self> {
        T::from_i8(n).and_then(|n| NotNan::new(n).ok())
    }
    fn from_i16(n: i16) -> Option<Self> {
        T::from_i16(n).and_then(|n| NotNan::new(n).ok())
    }
    fn from_i32(n: i32) -> Option<Self> {
        T::from_i32(n).and_then(|n| NotNan::new(n).ok())
    }
    fn from_usize(n: usize) -> Option<Self> {
        T::from_usize(n).and_then(|n| NotNan::new(n).ok())
    }
    fn from_u8(n: u8) -> Option<Self> {
        T::from_u8(n).and_then(|n| NotNan::new(n).ok())
    }
    fn from_u16(n: u16) -> Option<Self> {
        T::from_u16(n).and_then(|n| NotNan::new(n).ok())
    }
    fn from_u32(n: u32) -> Option<Self> {
        T::from_u32(n).and_then(|n| NotNan::new(n).ok())
    }
    fn from_f32(n: f32) -> Option<Self> {
        T::from_f32(n).and_then(|n| NotNan::new(n).ok())
    }
    fn from_f64(n: f64) -> Option<Self> {
        T::from_f64(n).and_then(|n| NotNan::new(n).ok())
    }
}

impl<T: FloatCore> ToPrimitive for NotNan<T> {
    fn to_i64(&self) -> Option<i64> {
        self.0.to_i64()
    }
    fn to_u64(&self) -> Option<u64> {
        self.0.to_u64()
    }

    fn to_isize(&self) -> Option<isize> {
        self.0.to_isize()
    }
    fn to_i8(&self) -> Option<i8> {
        self.0.to_i8()
    }
    fn to_i16(&self) -> Option<i16> {
        self.0.to_i16()
    }
    fn to_i32(&self) -> Option<i32> {
        self.0.to_i32()
    }
    fn to_usize(&self) -> Option<usize> {
        self.0.to_usize()
    }
    fn to_u8(&self) -> Option<u8> {
        self.0.to_u8()
    }
    fn to_u16(&self) -> Option<u16> {
        self.0.to_u16()
    }
    fn to_u32(&self) -> Option<u32> {
        self.0.to_u32()
    }
    fn to_f32(&self) -> Option<f32> {
        self.0.to_f32()
    }
    fn to_f64(&self) -> Option<f64> {
        self.0.to_f64()
    }
}

/// An error indicating a parse error from a string for `NotNan`.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum ParseNotNanError<E> {
    /// A plain parse error from the underlying float type.
    ParseFloatError(E),
    /// The parsed float value resulted in a NaN.
    IsNaN,
}

impl<E: fmt::Debug + Error + 'static> Error for ParseNotNanError<E> {
    fn description(&self) -> &str {
        "Error parsing a not-NaN floating point value"
    }

    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ParseNotNanError::ParseFloatError(e) => Some(e),
            ParseNotNanError::IsNaN => None,
        }
    }
}

impl<E: fmt::Display> fmt::Display for ParseNotNanError<E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ParseNotNanError::ParseFloatError(e) => write!(f, "Parse error: {}", e),
            ParseNotNanError::IsNaN => write!(f, "NotNan parser encounter a NaN"),
        }
    }
}

impl<T: FloatCore> Num for NotNan<T> {
    type FromStrRadixErr = ParseNotNanError<T::FromStrRadixErr>;

    fn from_str_radix(src: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
        T::from_str_radix(src, radix)
            .map_err(ParseNotNanError::ParseFloatError)
            .and_then(|n| NotNan::new(n).map_err(|_| ParseNotNanError::IsNaN))
    }
}

impl<T: FloatCore + Signed> Signed for NotNan<T> {
    #[inline]
    fn abs(&self) -> Self {
        NotNan(self.0.abs())
    }

    fn abs_sub(&self, other: &Self) -> Self {
        NotNan::new(Signed::abs_sub(&self.0, &other.0)).expect("Subtraction resulted in NaN")
    }

    #[inline]
    fn signum(&self) -> Self {
        NotNan(self.0.signum())
    }
    #[inline]
    fn is_positive(&self) -> bool {
        #[expect(clippy::disallowed_methods)]
        self.0.is_positive()
    }
    #[inline]
    fn is_negative(&self) -> bool {
        #[expect(clippy::disallowed_methods)]
        self.0.is_negative()
    }
}

impl<T: FloatCore> NumCast for NotNan<T> {
    fn from<F: ToPrimitive>(n: F) -> Option<Self> {
        T::from(n).and_then(|n| NotNan::new(n).ok())
    }
}

macro_rules! impl_float_const_method {
    ($wrapper:expr_2021, $method:ident) => {
        #[allow(non_snake_case)]
        #[allow(clippy::redundant_closure_call)]
        fn $method() -> Self {
            $wrapper(T::$method())
        }
    };
}

macro_rules! impl_float_const {
    ($type:ident, $wrapper:expr_2021) => {
        impl<T: FloatConst> FloatConst for $type<T> {
            impl_float_const_method!($wrapper, E);
            impl_float_const_method!($wrapper, FRAC_1_PI);
            impl_float_const_method!($wrapper, FRAC_1_SQRT_2);
            impl_float_const_method!($wrapper, FRAC_2_PI);
            impl_float_const_method!($wrapper, FRAC_2_SQRT_PI);
            impl_float_const_method!($wrapper, FRAC_PI_2);
            impl_float_const_method!($wrapper, FRAC_PI_3);
            impl_float_const_method!($wrapper, FRAC_PI_4);
            impl_float_const_method!($wrapper, FRAC_PI_6);
            impl_float_const_method!($wrapper, FRAC_PI_8);
            impl_float_const_method!($wrapper, LN_10);
            impl_float_const_method!($wrapper, LN_2);
            impl_float_const_method!($wrapper, LOG10_E);
            impl_float_const_method!($wrapper, LOG2_E);
            impl_float_const_method!($wrapper, PI);
            impl_float_const_method!($wrapper, SQRT_2);
        }
    };
}

impl_float_const!(OrderedFloat, OrderedFloat);
// Float constants are not NaN.
impl_float_const!(NotNan, |x| unsafe { NotNan::new_unchecked(x) });

mod impl_serde {
    extern crate serde;
    use core::f64;

    use num_traits::float::FloatCore;
    use serde::de::IntoDeserializer;

    use self::serde::Deserialize;
    use self::serde::Deserializer;
    use self::serde::Serialize;
    use self::serde::Serializer;
    use self::serde::de::Error;
    use self::serde::de::Unexpected;
    use super::NotNan;
    use super::OrderedFloat;

    #[cfg(test)]
    extern crate serde_test;
    #[cfg(test)]
    use self::serde_test::Configure;
    #[cfg(test)]
    use self::serde_test::Token;
    #[cfg(test)]
    use self::serde_test::assert_de_tokens_error;
    #[cfg(test)]
    use self::serde_test::assert_tokens;

    impl<T: FloatCore + Serialize> Serialize for OrderedFloat<T> {
        #[inline]
        fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            if s.is_human_readable() {
                if self.0.is_infinite() {
                    if self.0.is_sign_positive() {
                        s.serialize_str("Infinity")
                    } else {
                        s.serialize_str("-Infinity")
                    }
                } else if self.0.is_nan() {
                    s.serialize_str("Nan")
                } else {
                    self.0.serialize(s)
                }
            } else {
                self.0.serialize(s)
            }
        }
    }

    impl<'de, T: FloatCore + Deserialize<'de>> Deserialize<'de> for OrderedFloat<T> {
        #[inline]
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            if d.is_human_readable() {
                let value = serde_json::Value::deserialize(d)?;
                match value {
                    serde_json::Value::String(s) if s == "Infinity" => Ok(Self::infinity()),
                    serde_json::Value::String(s) if s == "-Infinity" => Ok(Self::neg_infinity()),
                    serde_json::Value::String(s) if s == "Nan" => Ok(Self::nan()),
                    _ => T::deserialize(serde_json::Value::into_deserializer(value))
                        .map(OrderedFloat)
                        .map_err(Error::custom),
                }
            } else {
                T::deserialize(d).map(OrderedFloat)
            }
        }
    }

    impl<T: FloatCore + Serialize> Serialize for NotNan<T> {
        #[inline]
        fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            self.0.serialize(s)
        }
    }

    impl<'de, T: FloatCore + Deserialize<'de>> Deserialize<'de> for NotNan<T> {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            let float = T::deserialize(d)?;
            NotNan::new(float).map_err(|_| {
                Error::invalid_value(Unexpected::Float(f64::NAN), &"float (but not NaN)")
            })
        }
    }

    #[test]
    fn test_ordered_float() {
        let float = OrderedFloat(1.0f64);
        assert_tokens(&float.readable(), &[Token::F64(1.0)]);
    }

    #[test]
    fn test_ordered_float_with_nan() {
        let float = OrderedFloat(f64::nan());
        assert_tokens(&float.readable(), &[Token::Str("Nan")]);
    }

    #[test]
    fn test_ordered_float_with_inf() {
        let float = OrderedFloat(f64::infinity());
        assert_tokens(&float.readable(), &[Token::Str("Infinity")]);
        let float = OrderedFloat(f64::neg_infinity());
        assert_tokens(&float.readable(), &[Token::Str("-Infinity")]);
    }

    #[test]
    fn test_non_readable_ordered_float() {
        let float = OrderedFloat(1.0f64);
        assert_tokens(&float.compact(), &[Token::F64(1.0)]);

        let float = OrderedFloat(f64::infinity());
        assert_tokens(&float.compact(), &[Token::F64(f64::infinity())]);

        let float = OrderedFloat(f64::neg_infinity());
        assert_tokens(&float.compact(), &[Token::F64(f64::neg_infinity())]);
    }

    #[test]
    fn test_not_nan() {
        let float = NotNan(1.0f64);
        assert_tokens(&float, &[Token::F64(1.0)]);
    }

    #[test]
    fn test_fail_on_nan() {
        assert_de_tokens_error::<NotNan<f64>>(
            &[Token::F64(f64::NAN)],
            "invalid value: floating point `NaN`, expected float (but not NaN)",
        );
    }
}

mod impl_borsh {
    extern crate borsh;

    use std::io::Read;

    use num_traits::float::FloatCore;

    use super::NotNan;
    use super::OrderedFloat;

    impl borsh::BorshSerialize for OrderedFloat<f32> {
        #[inline]
        fn serialize<W: borsh::io::Write>(&self, writer: &mut W) -> borsh::io::Result<()> {
            writer.write_all(&self.0.to_bits().to_le_bytes())?;
            Ok(())
        }
    }

    impl borsh::BorshSerialize for OrderedFloat<f64> {
        #[inline]
        fn serialize<W: borsh::io::Write>(&self, writer: &mut W) -> borsh::io::Result<()> {
            writer.write_all(&self.0.to_bits().to_le_bytes())?;
            Ok(())
        }
    }

    impl borsh::BorshDeserialize for OrderedFloat<f32> {
        fn deserialize_reader<R: Read>(reader: &mut R) -> std::io::Result<Self> {
            let mut buf = [0u8; size_of::<OrderedFloat<f32>>()];
            reader.read_exact(&mut buf)?;
            let res = OrderedFloat::from(f32::from_bits(u32::from_le_bytes(buf)));
            Ok(res)
        }
    }

    impl borsh::BorshDeserialize for OrderedFloat<f64> {
        fn deserialize_reader<R: Read>(reader: &mut R) -> std::io::Result<Self> {
            let mut buf = [0u8; size_of::<OrderedFloat<f64>>()];
            reader.read_exact(&mut buf)?;
            let res = OrderedFloat::from(f64::from_bits(u64::from_le_bytes(buf)));
            Ok(res)
        }
    }

    impl<T> borsh::BorshSerialize for NotNan<T>
    where T: borsh::BorshSerialize
    {
        #[inline]
        fn serialize<W: borsh::io::Write>(&self, writer: &mut W) -> borsh::io::Result<()> {
            <T as borsh::BorshSerialize>::serialize(&self.0, writer)
        }
    }

    impl<T> borsh::BorshDeserialize for NotNan<T>
    where T: FloatCore + borsh::BorshDeserialize
    {
        #[inline]
        fn deserialize_reader<R: borsh::io::Read>(reader: &mut R) -> borsh::io::Result<Self> {
            let float = <T as borsh::BorshDeserialize>::deserialize_reader(reader)?;
            NotNan::new(float).map_err(|_| {
                borsh::io::Error::new(
                    borsh::io::ErrorKind::InvalidData,
                    "expected a non-NaN float",
                )
            })
        }
    }

    #[test]
    fn test_ordered_float() {
        let float = OrderedFloat(1.0f64);
        let buffer = borsh::to_vec(&float).expect("failed to serialize value");
        let deser_float: OrderedFloat<f64> =
            borsh::from_slice(&buffer).expect("failed to deserialize value");
        assert_eq!(deser_float, float);
    }

    #[test]
    fn test_not_nan() {
        let float = NotNan(1.0f64);
        let buffer = borsh::to_vec(&float).expect("failed to serialize value");
        let deser_float: NotNan<f64> =
            borsh::from_slice(&buffer).expect("failed to deserialize value");
        assert_eq!(deser_float, float);
    }
}

mod impl_rand {
    use rand::Rng;
    use rand::distributions::Distribution;
    use rand::distributions::Open01;
    use rand::distributions::OpenClosed01;
    use rand::distributions::Standard;
    use rand::distributions::uniform::*;
    use serde::Deserialize;
    use serde::Serialize;

    use super::NotNan;
    use super::OrderedFloat;

    macro_rules! impl_distribution {
        ($dist:ident, $($f:ty),+) => {
            $(
            impl Distribution<NotNan<$f>> for $dist {
                fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> NotNan<$f> {
                    // 'rand' never generates NaN values in the Standard, Open01, or
                    // OpenClosed01 distributions. Using 'new_unchecked' is therefore
                    // safe.
                    unsafe { NotNan::new_unchecked(self.sample(rng)) }
                }
            }

            impl Distribution<OrderedFloat<$f>> for $dist {
                fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> OrderedFloat<$f> {
                    OrderedFloat(self.sample(rng))
                }
            }
            )*
        }
    }

    impl_distribution! { Standard, f32, f64 }
    impl_distribution! { Open01, f32, f64 }
    impl_distribution! { OpenClosed01, f32, f64 }

    /// A sampler for a uniform distribution
    #[derive(Clone, Copy, Debug, Serialize, Deserialize)]
    pub struct UniformNotNan<T>(UniformFloat<T>);
    impl SampleUniform for NotNan<f32> {
        type Sampler = UniformNotNan<f32>;
    }
    impl SampleUniform for NotNan<f64> {
        type Sampler = UniformNotNan<f64>;
    }
    impl<T> PartialEq for UniformNotNan<T>
    where UniformFloat<T>: PartialEq
    {
        fn eq(&self, other: &Self) -> bool {
            self.0 == other.0
        }
    }

    /// A sampler for a uniform distribution
    #[derive(Clone, Copy, Debug, Serialize, Deserialize)]
    pub struct UniformOrdered<T>(UniformFloat<T>);
    impl SampleUniform for OrderedFloat<f32> {
        type Sampler = UniformOrdered<f32>;
    }
    impl SampleUniform for OrderedFloat<f64> {
        type Sampler = UniformOrdered<f64>;
    }
    impl<T> PartialEq for UniformOrdered<T>
    where UniformFloat<T>: PartialEq
    {
        fn eq(&self, other: &Self) -> bool {
            self.0 == other.0
        }
    }

    macro_rules! impl_uniform_sampler {
        ($f:ty) => {
            impl UniformSampler for UniformNotNan<$f> {
                type X = NotNan<$f>;
                fn new<B1, B2>(low: B1, high: B2) -> Self
                where
                    B1: SampleBorrow<Self::X> + Sized,
                    B2: SampleBorrow<Self::X> + Sized,
                {
                    UniformNotNan(UniformFloat::<$f>::new(low.borrow().0, high.borrow().0))
                }
                fn new_inclusive<B1, B2>(low: B1, high: B2) -> Self
                where
                    B1: SampleBorrow<Self::X> + Sized,
                    B2: SampleBorrow<Self::X> + Sized,
                {
                    UniformSampler::new(low, high)
                }
                fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Self::X {
                    // UniformFloat.sample() will never return NaN.
                    unsafe { NotNan::new_unchecked(self.0.sample(rng)) }
                }
            }

            impl UniformSampler for UniformOrdered<$f> {
                type X = OrderedFloat<$f>;
                fn new<B1, B2>(low: B1, high: B2) -> Self
                where
                    B1: SampleBorrow<Self::X> + Sized,
                    B2: SampleBorrow<Self::X> + Sized,
                {
                    UniformOrdered(UniformFloat::<$f>::new(low.borrow().0, high.borrow().0))
                }
                fn new_inclusive<B1, B2>(low: B1, high: B2) -> Self
                where
                    B1: SampleBorrow<Self::X> + Sized,
                    B2: SampleBorrow<Self::X> + Sized,
                {
                    UniformSampler::new(low, high)
                }
                fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Self::X {
                    OrderedFloat(self.0.sample(rng))
                }
            }
        };
    }

    impl_uniform_sampler! { f32 }
    impl_uniform_sampler! { f64 }
}

impl Unmarshal<OrderedFloat<f32>> for OrderedFloat<f32> {
    fn unmarshal(scratch: &[u8]) -> Self {
        let bits = u32::from_le_bytes(scratch.try_into().unwrap());
        f32::from_bits(bits).into()
    }
}

impl Unmarshal<OrderedFloat<f64>> for OrderedFloat<f64> {
    fn unmarshal(scratch: &[u8]) -> Self {
        let bits = u64::from_le_bytes(scratch.try_into().unwrap());
        f64::from_bits(bits).into()
    }
}
