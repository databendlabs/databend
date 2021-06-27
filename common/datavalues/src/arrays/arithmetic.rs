//! Implementations of arithmetic operations on DataArray's.
use std::ops::Add;
use std::ops::Div;
use std::ops::Mul;
use std::ops::Rem;
use std::ops::Sub;
use std::sync::Arc;

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::compute;
use common_arrow::arrow::compute::divide_scalar;
use common_arrow::arrow::error::ArrowError;
use common_exception::Result;
use num::Num;
use num::NumCast;
use num::One;
use num::ToPrimitive;
use num::Zero;

use crate::arrays::ops::*;
use crate::arrays::DataArray;
use crate::DFBooleanArray;
use crate::DFFloat32Array;
use crate::DFFloat64Array;
use crate::DFListArray;
use crate::DFNumericType;
use crate::DFStringArray;
use crate::Float32Type;
use crate::Float64Type;

macro_rules! apply_operand_on_array_by_iter {

    ($self:ident, $rhs:ident, $operand:tt) => {
            {
                match ($self.null_count(), $rhs.null_count()) {
                    (0, 0) => {
                        let a: NoNull<DataArray<_>> = $self
                        .into_no_null_iter()
                        .zip($rhs.into_no_null_iter())
                        .map(|(left, right)| left $operand right)
                        .collect();
                        a.into_inner()
                    },
                    (0, _) => {
                        $self
                        .into_no_null_iter()
                        .zip($rhs.downcast_iter())
                        .map(|(left, opt_right)| opt_right.map(|right| left $operand right))
                        .collect()
                    },
                    (_, 0) => {
                        $self
                        .downcast_iter()
                        .zip($rhs.into_no_null_iter())
                        .map(|(opt_left, right)| opt_left.map(|left| left $operand right))
                        .collect()
                    },
                    (_, _) => {
                    $self.downcast_iter()
                        .zip($rhs.downcast_iter())
                        .map(|(opt_left, opt_right)| match (opt_left, opt_right) {
                            (None, None) => None,
                            (None, Some(_)) => None,
                            (Some(_), None) => None,
                            (Some(left), Some(right)) => Some(left $operand right),
                        })
                        .collect()

                    }
                }
            }
    }
}

fn arithmetic_helper<T, Kernel, F>(
    lhs: &DataArray<T>,
    rhs: &DataArray<T>,
    kernel: Kernel,
    operation: F,
) -> DataArray<T>
where
    T: DFNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + num::Zero,
    Kernel: Fn(
        &PrimitiveArray<T>,
        &PrimitiveArray<T>,
    ) -> std::result::Result<PrimitiveArray<T>, ArrowError>,
    F: Fn(T::Native, T::Native) -> T::Native,
{
    let ca = match (lhs.len(), rhs.len()) {
        (a, b) if a == b => {
            let array = Arc::new(kernel(lhs.downcast_ref(), rhs.downcast_ref()).expect("output"))
                as ArrayRef;

            array.into()
        }
        // broadcast right path
        (_, 1) => {
            let opt_rhs = rhs.get(0);
            match opt_rhs {
                None => DataArray::full_null(lhs.len()),
                Some(rhs) => lhs.apply(|lhs| operation(lhs, rhs)),
            }
        }
        (1, _) => {
            let opt_lhs = lhs.get(0);
            match opt_lhs {
                None => DataArray::full_null(rhs.len()),
                Some(lhs) => rhs.apply(|rhs| operation(lhs, rhs)),
            }
        }
        _ => unreachable!(),
    };
    ca
}

// Operands on DataArray & DataArray

impl<T> Add for &DataArray<T>
where
    T: DFNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + num::Zero,
{
    type Output = DataArray<T>;

    fn add(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, compute::add, |lhs, rhs| lhs + rhs)
    }
}

impl<T> Div for &DataArray<T>
where
    T: DFNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Rem<Output = T::Native>
        + num::Zero
        + num::One,
{
    type Output = DataArray<T>;

    fn div(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, compute::divide, |lhs, rhs| lhs / rhs)
    }
}

impl<T> Mul for &DataArray<T>
where
    T: DFNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Rem<Output = T::Native>
        + num::Zero,
{
    type Output = DataArray<T>;

    fn mul(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, compute::multiply, |lhs, rhs| lhs * rhs)
    }
}

impl<T> Rem for &DataArray<T>
where
    T: DFNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Rem<Output = T::Native>
        + num::Zero
        + num::One,
{
    type Output = DataArray<T>;

    fn rem(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, compute::modulus, |lhs, rhs| lhs % rhs)
    }
}

impl<T> Sub for &DataArray<T>
where
    T: DFNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Rem<Output = T::Native>
        + num::Zero,
{
    type Output = DataArray<T>;

    fn sub(self, rhs: Self) -> Self::Output {
        arithmetic_helper(self, rhs, compute::subtract, |lhs, rhs| lhs - rhs)
    }
}

impl<T> Add for DataArray<T>
where
    T: DFNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Rem<Output = T::Native>
        + num::Zero,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        (&self).add(&rhs)
    }
}

impl<T> Div for DataArray<T>
where
    T: DFNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Rem<Output = T::Native>
        + num::Zero
        + num::One,
{
    type Output = Self;

    fn div(self, rhs: Self) -> Self::Output {
        (&self).div(&rhs)
    }
}

impl<T> Mul for DataArray<T>
where
    T: DFNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Rem<Output = T::Native>
        + num::Zero,
{
    type Output = Self;

    fn mul(self, rhs: Self) -> Self::Output {
        (&self).mul(&rhs)
    }
}

impl<T> Sub for DataArray<T>
where
    T: DFNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Rem<Output = T::Native>
        + num::Zero,
{
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        (&self).sub(&rhs)
    }
}

impl<T> Rem for DataArray<T>
where
    T: DFNumericType,
    T::Native: Add<Output = T::Native>
        + Sub<Output = T::Native>
        + Mul<Output = T::Native>
        + Div<Output = T::Native>
        + Rem<Output = T::Native>
        + num::Zero
        + num::One,
{
    type Output = DataArray<T>;

    fn rem(self, rhs: Self) -> Self::Output {
        (&self).rem(&rhs)
    }
}

// Operands on DataArray & Num

impl<T, N> Add<N> for &DataArray<T>
where
    T: DFNumericType,
    T::Native: NumCast,
    N: Num + ToPrimitive,
    T::Native: Add<Output = T::Native>,
{
    type Output = DataArray<T>;

    fn add(self, rhs: N) -> Self::Output {
        let adder: T::Native = NumCast::from(rhs).unwrap();
        self.apply(|val| val + adder)
    }
}

impl<T, N> Sub<N> for &DataArray<T>
where
    T: DFNumericType,
    T::Native: NumCast,
    N: Num + ToPrimitive,
    T::Native: Sub<Output = T::Native>,
{
    type Output = DataArray<T>;

    fn sub(self, rhs: N) -> Self::Output {
        let subber: T::Native = NumCast::from(rhs).unwrap();
        self.apply(|val| val - subber)
    }
}

impl<T, N> Div<N> for &DataArray<T>
where
    T: DFNumericType,
    T::Native: NumCast
        + Div<Output = T::Native>
        + One
        + Zero
        + Rem<Output = T::Native>
        + Sub<Output = T::Native>,
    N: Num + ToPrimitive,
{
    type Output = DataArray<T>;

    fn div(self, rhs: N) -> Self::Output {
        let rhs: T::Native = NumCast::from(rhs).expect("could not cast");
        self.apply_kernel(|arr| Arc::new(divide_scalar(arr, rhs).unwrap()))
    }
}

impl<T, N> Mul<N> for &DataArray<T>
where
    T: DFNumericType,
    T::Native: NumCast,
    N: Num + ToPrimitive,
    T::Native: Mul<Output = T::Native>,
{
    type Output = DataArray<T>;

    fn mul(self, rhs: N) -> Self::Output {
        let multiplier: T::Native = NumCast::from(rhs).unwrap();
        self.apply(|val| val * multiplier)
    }
}

impl<T, N> Rem<N> for &DataArray<T>
where
    T: DFNumericType,
    T::Native: NumCast,
    N: Num + ToPrimitive,
    T::Native:
        Div<Output = T::Native> + One + Zero + Rem<Output = T::Native> + Sub<Output = T::Native>,
{
    type Output = DataArray<T>;

    fn rem(self, rhs: N) -> Self::Output {
        let rhs: T::Native = NumCast::from(rhs).expect("could not cast");
        self.apply_kernel(|arr| Arc::new(compute::modulus_scalar(arr, rhs).unwrap()))
    }
}

impl<T, N> Add<N> for DataArray<T>
where
    T: DFNumericType,
    T::Native: NumCast,
    N: Num + ToPrimitive,
    T::Native: Add<Output = T::Native>,
{
    type Output = DataArray<T>;

    fn add(self, rhs: N) -> Self::Output {
        (&self).add(rhs)
    }
}

impl<T, N> Sub<N> for DataArray<T>
where
    T: DFNumericType,
    T::Native: NumCast,
    N: Num + ToPrimitive,
    T::Native: Sub<Output = T::Native>,
{
    type Output = DataArray<T>;

    fn sub(self, rhs: N) -> Self::Output {
        (&self).sub(rhs)
    }
}

impl<T, N> Div<N> for DataArray<T>
where
    T: DFNumericType,
    T::Native: NumCast
        + Div<Output = T::Native>
        + One
        + Zero
        + Sub<Output = T::Native>
        + Rem<Output = T::Native>,
    N: Num + ToPrimitive,
{
    type Output = DataArray<T>;

    fn div(self, rhs: N) -> Self::Output {
        (&self).div(rhs)
    }
}

impl<T, N> Mul<N> for DataArray<T>
where
    T: DFNumericType,
    T::Native: NumCast,
    N: Num + ToPrimitive,
    T::Native: Mul<Output = T::Native>,
{
    type Output = DataArray<T>;

    fn mul(self, rhs: N) -> Self::Output {
        (&self).mul(rhs)
    }
}

impl<T, N> Rem<N> for DataArray<T>
where
    T: DFNumericType,
    T::Native: NumCast,
    N: Num + ToPrimitive,
    T::Native:
        Div<Output = T::Native> + One + Zero + Rem<Output = T::Native> + Sub<Output = T::Native>,
{
    type Output = DataArray<T>;

    fn rem(self, rhs: N) -> Self::Output {
        (&self).rem(rhs)
    }
}

fn concat_strings(l: &str, r: &str) -> String {
    // fastest way to concat strings according to https://github.com/hoodie/concatenation_benchmarks-rs
    let mut s = String::with_capacity(l.len() + r.len());
    s.push_str(l);
    s.push_str(r);
    s
}

impl Add for &DFStringArray {
    type Output = DFStringArray;

    fn add(self, rhs: Self) -> Self::Output {
        // broadcasting path
        if rhs.len() == 1 {
            let rhs = rhs.get(0);
            return match rhs {
                Some(rhs) => self.add(rhs),
                None => DFStringArray::full_null(self.len()),
            };
        }

        // todo! add no_null variants. Need 4 paths.
        self.into_iter()
            .zip(rhs.into_iter())
            .map(|(opt_l, opt_r)| match (opt_l, opt_r) {
                (Some(l), Some(r)) => Some(concat_strings(l, r)),
                _ => None,
            })
            .collect()
    }
}

impl Add for DFStringArray {
    type Output = DFStringArray;

    fn add(self, rhs: Self) -> Self::Output {
        (&self).add(&rhs)
    }
}

impl Add<&str> for &DFStringArray {
    type Output = DFStringArray;

    fn add(self, rhs: &str) -> Self::Output {
        match self.null_count() {
            0 => self
                .into_no_null_iter()
                .map(|l| concat_strings(l, rhs))
                .collect(),
            _ => self
                .into_iter()
                .map(|opt_l| opt_l.map(|l| concat_strings(l, rhs)))
                .collect(),
        }
    }
}

pub trait Pow {
    fn pow_f32(&self, _exp: f32) -> DFFloat32Array {
        unimplemented!()
    }
    fn pow_f64(&self, _exp: f64) -> DFFloat64Array {
        unimplemented!()
    }
}

impl<T> Pow for DataArray<T>
where
    T: DFNumericType,
    DataArray<T>: ArrayCast,
{
    fn pow_f32(&self, exp: f32) -> DFFloat32Array {
        self.cast::<Float32Type>()
            .expect("f32 array")
            .apply_kernel(|arr| Arc::new(compute::powf_scalar(arr, exp).unwrap()))
    }

    fn pow_f64(&self, exp: f64) -> DFFloat64Array {
        self.cast::<Float64Type>()
            .expect("f64 array")
            .apply_kernel(|arr| Arc::new(compute::powf_scalar(arr, exp).unwrap()))
    }
}

impl Pow for DFBooleanArray {}
impl Pow for DFStringArray {}
impl Pow for DFListArray {}
