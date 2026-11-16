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

//! Concrete batch kernels. A plan selects exactly one kernel at binding; the
//! kernel fixes its storage types, row operation, batch driver and partial
//! evaluation policy. Nothing is chosen again per batch.
//!
//! Drivers: `SKIP` drivers skip rows excluded by validity, the others evaluate
//! every row.

use std::marker::PhantomData;

use databend_common_expression::Column;
use databend_common_expression::EvalContext;
use databend_common_expression::PartialEvalPolicy;
use databend_common_expression::ScalarFunction;
use databend_common_expression::Value;
use databend_common_expression::types::compute_view::Compute;
use databend_common_expression::types::compute_view::ComputeView;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::vectorize_2_arg_with_policy;
use databend_common_expression::vectorize_with_builder_2_arg;

use super::plan::InputPlan;
use super::row;
use super::row::Aligned;
use super::row::Bounds;
use super::row::CheckedDivide;
use super::row::DivideScaling;
use super::row::NativeRound;
use crate::decimal::DecimalExt;

pub(crate) trait Kernel: Sized + Send + Sync + 'static {
    /// Storage both operands are widened into.
    type Compute: DecimalExt;

    fn eval<A: DecimalExt, B: DecimalExt>(
        &self,
        args: DecimalArgs<'_, A, B>,
        ctx: &mut EvalContext,
    ) -> Value<AnyType>;

    fn bind(self, inputs: [InputPlan; 2]) -> Box<dyn ScalarFunction> {
        Self::Compute::bind_kernel(self, inputs)
    }
}

struct Evaluator<K, A, B> {
    inputs: [InputPlan; 2],
    kernel: K,
    storage: PhantomData<fn() -> (A, B)>,
}

impl<K: Kernel, A: DecimalExt, B: DecimalExt> ScalarFunction for Evaluator<K, A, B> {
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        self.kernel.eval(
            DecimalArgs::<A, B>::new([&args[0], &args[1]], &self.inputs),
            ctx,
        )
    }
}

pub(crate) fn evaluator<K: Kernel, A: DecimalExt, B: DecimalExt>(
    kernel: K,
    inputs: [InputPlan; 2],
) -> Box<dyn ScalarFunction> {
    Box::new(Evaluator::<K, A, B> {
        inputs,
        kernel,
        storage: PhantomData,
    })
}

// Borrowed call arguments whose converted storage types `A` and `B` were fixed
// at binding; conversion stays deferred until the plan has selected its batch
// kernel.
pub(crate) struct DecimalArgs<'a, A, B> {
    pub(super) values: [&'a Value<AnyType>; 2],
    plans: &'a [InputPlan; 2],
    storage: PhantomData<fn() -> (A, B)>,
}

impl<'a, A: DecimalExt, B: DecimalExt> DecimalArgs<'a, A, B> {
    pub(super) fn new(values: [&'a Value<AnyType>; 2], plans: &'a [InputPlan; 2]) -> Self {
        debug_assert_eq!(plans.map(|plan| plan.storage()), [A::KIND, B::KIND]);
        Self {
            values,
            plans,
            storage: PhantomData,
        }
    }

    pub(super) fn convert(self, ctx: &mut EvalContext) -> [Value<AnyType>; 2] {
        [
            self.plans[0].convert(self.values[0], ctx),
            self.plans[1].convert(self.values[1], ctx),
        ]
    }
}

// Batch drivers.

fn inactive_batch<A, B, U: DecimalExt>(
    args: &DecimalArgs<'_, A, B>,
    ctx: &EvalContext,
    size: DecimalSize,
) -> Option<Value<AnyType>> {
    if ctx.num_rows != 0
        && !ctx
            .validity
            .as_ref()
            .is_some_and(|v| v.null_count() == v.len())
    {
        return None;
    }
    // Only shape/validity metadata is inspected. No input conversion or kernel
    // executes when the whole batch is invisible.
    Some(
        if args.values.iter().all(|value| value.as_scalar().is_some()) {
            Value::Scalar(U::upcast_scalar(U::zero(), size))
        } else {
            Value::Column(Column::Decimal(U::to_column(
                vec![U::zero(); ctx.num_rows],
                size,
            )))
        },
    )
}

// Operand storage only widens into the computation type; the binder never
// instantiates a wider operand.
fn widen<A: DecimalExt, T: DecimalExt>(value: Value<AnyType>) -> Value<DecimalView<A, T>> {
    const { assert!(A::MAX_PRECISION <= T::MAX_PRECISION) };
    value.try_downcast().unwrap()
}

/// Infallible row kernel.
pub(super) fn map<
    const SKIP: bool,
    A: DecimalExt,
    B: DecimalExt,
    T: DecimalExt,
    U: DecimalExt,
    F,
>(
    args: DecimalArgs<'_, A, B>,
    ctx: &mut EvalContext,
    size: DecimalSize,
    f: F,
) -> Value<AnyType>
where
    F: Fn(T, T) -> U + Sync,
{
    if SKIP && let Some(value) = inactive_batch::<A, B, U>(&args, ctx, size) {
        return value;
    }
    let [a, b] = args.convert(ctx);
    let (a, b) = (widen::<A, T>(a), widen::<B, T>(b));
    let value = if SKIP {
        vectorize_2_arg_with_policy::<DecimalView<A, T>, DecimalView<B, T>, DecimalType<U>>(
            PartialEvalPolicy::SkipInactiveRows,
            |a, b, _| f(a, b),
        )(a, b, ctx)
    } else {
        vectorize_2_arg::<DecimalView<A, T>, DecimalView<B, T>, DecimalType<U>>(|a, b, _| f(a, b))(
            a, b, ctx,
        )
    };
    value.upcast_with_type(&DataType::Decimal(size))
}

/// Fallible row kernel; failed rows report `Err` and hold `fallback`.
pub(super) fn checked<
    const SKIP: bool,
    A: DecimalExt,
    B: DecimalExt,
    T: DecimalExt,
    U: DecimalExt,
    F,
>(
    args: DecimalArgs<'_, A, B>,
    ctx: &mut EvalContext,
    size: DecimalSize,
    f: F,
    fallback: U,
) -> Value<AnyType>
where
    F: Fn(T, T) -> Result<U, &'static str> + Sync,
{
    if SKIP && let Some(value) = inactive_batch::<A, B, U>(&args, ctx, size) {
        return value;
    }
    let [a, b] = args.convert(ctx);
    checked_views::<SKIP, _, _, T, U, F>(widen::<A, T>(a), widen::<B, T>(b), ctx, size, f, fallback)
}

// A Native64 numerator has fewer than 19 digits, hence i64 storage. The
// divisor keeps its own storage and is read through the saturating view.
pub(super) fn divide64<A: DecimalExt, B: DecimalExt>(
    args: DecimalArgs<'_, A, B>,
    ctx: &mut EvalContext,
    size: DecimalSize,
    multiplier: i64,
) -> Value<AnyType> {
    debug_assert_eq!(A::KIND, DecimalDataKind::Decimal64);
    if let Some(value) = inactive_batch::<A, B, i64>(&args, ctx, size) {
        return value;
    }
    let [a, b] = args.convert(ctx);
    let a: Value<DecimalView<A, i64>> = a.try_downcast().unwrap();
    let b: Value<ComputeView<NativeDivisor<B>, CoreDecimal<B>, CoreDecimal<i64>>> =
        b.try_downcast().unwrap();
    checked_views::<true, _, _, i64, i64, _>(
        a,
        b,
        ctx,
        size,
        |a, b| {
            if b == 0 {
                Err("divided by zero")
            } else {
                Ok(row::native64(a, b, multiplier))
            }
        },
        1,
    )
}

fn checked_views<const SKIP: bool, L, R, T: DecimalExt, U: DecimalExt, F>(
    a: Value<L>,
    b: Value<R>,
    ctx: &mut EvalContext,
    size: DecimalSize,
    f: F,
    fallback: U,
) -> Value<AnyType>
where
    L: for<'a> AccessType<ScalarRef<'a> = T>,
    R: for<'a> AccessType<ScalarRef<'a> = T>,
    F: Fn(T, T) -> Result<U, &'static str> + Sync,
{
    if let (Value::Scalar(a), Value::Scalar(b)) = (&a, &b) {
        // Broadcast scalar work executes once. Report errors at active rows,
        // not just row zero (which may be excluded by partial evaluation).
        let value = match f(L::to_scalar_ref(a), R::to_scalar_ref(b)) {
            Ok(value) => value,
            Err(message) => {
                for row in 0..ctx.num_rows {
                    ctx.set_error(row, message);
                }
                fallback
            }
        };
        return Value::Scalar(U::upcast_scalar(value, size));
    }
    if SKIP && let Some(active) = PartialEvalPolicy::SkipInactiveRows.active_rows(ctx) {
        return vectorize_with_builder_2_arg::<L, R, DecimalType<U>>(
            |a, b, out: &mut Vec<U>, ctx| {
                if !active.get_bit(out.len()) {
                    out.push(U::zero());
                    return;
                }
                match f(a, b) {
                    Ok(value) => out.push(value),
                    Err(message) => {
                        ctx.set_error(out.len(), message);
                        out.push(fallback);
                    }
                }
            },
        )(a, b, ctx)
        .upcast_with_type(&DataType::Decimal(size));
    }
    // No mask: keep the original builder loop without a row-level mask branch.
    vectorize_with_builder_2_arg::<L, R, DecimalType<U>>(|a, b, out: &mut Vec<U>, ctx| {
        match f(a, b) {
            Ok(value) => out.push(value),
            Err(message) => {
                ctx.set_error(out.len(), message);
                out.push(fallback);
            }
        }
    })(a, b, ctx)
    .upcast_with_type(&DataType::Decimal(size))
}

// Only used by a Native64 plan, whose scaled numerator has magnitude <10^18.
// Any divisor outside i64 rounds that quotient to zero. Saturating the divisor
// preserves zero/sign and the same quotient, without selecting a batch route
// from data. Scalars convert once; columns convert lazily in their single pass.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct NativeDivisor<F>(PhantomData<F>);

impl<F: DecimalExt> Compute<CoreDecimal<F>, CoreDecimal<i64>> for NativeDivisor<F> {
    #[inline]
    fn compute<'a>(
        value: <CoreDecimal<F> as AccessType>::ScalarRef<'a>,
    ) -> <CoreDecimal<i64> as AccessType>::ScalarRef<'a> {
        value.saturating_to_i64()
    }
    fn compute_domain(domain: &SimpleDomain<F>) -> SimpleDomain<i64> {
        SimpleDomain {
            min: Self::compute(domain.min),
            max: Self::compute(domain.max),
        }
    }
}

// Kernels.

fn assert_storage<T: DecimalExt>(size: DecimalSize, role: &str) {
    assert_eq!(
        size.data_kind(),
        T::KIND,
        "internal {role} storage does not match bound {size:?}"
    );
}

// Addition and subtraction.

/// The binding proves the result fits: wrapping is exact for visible rows and
/// harmless for hidden ones, so every row is evaluated without a mask branch.
pub(super) struct AddSubtractNative<const SUBTRACT: bool, T> {
    size: DecimalSize,
    compute: PhantomData<T>,
}

impl<const SUBTRACT: bool, T: DecimalExt> AddSubtractNative<SUBTRACT, T> {
    pub(super) fn new(size: DecimalSize) -> Self {
        assert_storage::<T>(size, "result");
        Self {
            size,
            compute: PhantomData,
        }
    }
}

impl<const SUBTRACT: bool, T: DecimalExt> Kernel for AddSubtractNative<SUBTRACT, T> {
    type Compute = T;

    fn eval<A: DecimalExt, B: DecimalExt>(
        &self,
        args: DecimalArgs<'_, A, B>,
        ctx: &mut EvalContext,
    ) -> Value<AnyType> {
        map::<false, A, B, T, T, _>(
            args,
            ctx,
            self.size,
            row::native_add_subtract::<T, SUBTRACT>,
        )
    }
}

/// Same-scale operands whose sum may exceed the result precision. Wrapping
/// plus a bound check is still exact, so every row is evaluated.
pub(super) struct AddSubtractChecked<const SUBTRACT: bool, T> {
    size: DecimalSize,
    bounds: Bounds<T>,
}

impl<const SUBTRACT: bool, T: DecimalExt> AddSubtractChecked<SUBTRACT, T> {
    pub(super) fn new(size: DecimalSize) -> Self {
        assert_storage::<T>(size, "result");
        Self {
            size,
            bounds: Bounds::new(size.precision()),
        }
    }
}

impl<const SUBTRACT: bool, T: DecimalExt> Kernel for AddSubtractChecked<SUBTRACT, T> {
    type Compute = T;

    fn eval<A: DecimalExt, B: DecimalExt>(
        &self,
        args: DecimalArgs<'_, A, B>,
        ctx: &mut EvalContext,
    ) -> Value<AnyType> {
        checked::<false, A, B, T, T, _>(
            args,
            ctx,
            self.size,
            |a, b| {
                row::checked_add_subtract::<T, SUBTRACT>(a, b, &self.bounds)
                    .ok_or("Decimal overflow")
            },
            T::one(),
        )
    }
}

/// One operand is scaled to the result scale, possibly through BigInt; only
/// visible rows are evaluated.
pub(super) struct AddSubtractAligned<const SUBTRACT: bool, const LEFT: bool, T> {
    size: DecimalSize,
    aligned: Aligned<T, LEFT>,
}

impl<const SUBTRACT: bool, const LEFT: bool, T: DecimalExt> AddSubtractAligned<SUBTRACT, LEFT, T> {
    pub(super) fn new(size: DecimalSize, shift: u32) -> Self {
        assert_storage::<T>(size, "result");
        Self {
            size,
            aligned: Aligned::new(shift, size.precision()),
        }
    }
}

impl<const SUBTRACT: bool, const LEFT: bool, T: DecimalExt> Kernel
    for AddSubtractAligned<SUBTRACT, LEFT, T>
{
    type Compute = T;

    fn eval<A: DecimalExt, B: DecimalExt>(
        &self,
        args: DecimalArgs<'_, A, B>,
        ctx: &mut EvalContext,
    ) -> Value<AnyType> {
        checked::<true, A, B, T, T, _>(
            args,
            ctx,
            self.size,
            |a, b| {
                self.aligned
                    .add_subtract::<SUBTRACT>(a, b)
                    .ok_or("Decimal overflow")
            },
            T::one(),
        )
    }
}

// Multiplication. Computation and result share storage `T`.

pub(super) struct MultiplyIntegral<T> {
    size: DecimalSize,
    compute: PhantomData<T>,
}

impl<T: DecimalExt> MultiplyIntegral<T> {
    pub(super) fn new(size: DecimalSize) -> Self {
        assert_storage::<T>(size, "result");
        Self {
            size,
            compute: PhantomData,
        }
    }
}

impl<T: DecimalExt> Kernel for MultiplyIntegral<T> {
    type Compute = T;

    fn eval<A: DecimalExt, B: DecimalExt>(
        &self,
        args: DecimalArgs<'_, A, B>,
        ctx: &mut EvalContext,
    ) -> Value<AnyType> {
        map::<true, A, B, T, T, _>(args, ctx, self.size, |a, b| a * b)
    }
}

pub(super) struct MultiplyRounded<T> {
    size: DecimalSize,
    round: NativeRound<T>,
}

impl<T: DecimalExt> MultiplyRounded<T> {
    pub(super) fn new(size: DecimalSize, shift: u32) -> Self {
        assert_storage::<T>(size, "result");
        Self {
            size,
            round: NativeRound::new(shift),
        }
    }
}

impl<T: DecimalExt> Kernel for MultiplyRounded<T> {
    type Compute = T;

    fn eval<A: DecimalExt, B: DecimalExt>(
        &self,
        args: DecimalArgs<'_, A, B>,
        ctx: &mut EvalContext,
    ) -> Value<AnyType> {
        map::<true, A, B, T, T, _>(args, ctx, self.size, |a, b| {
            row::native_rounded(a, b, &self.round)
        })
    }
}

pub(super) struct MultiplyCheckedIntegral<T> {
    size: DecimalSize,
    bounds: Bounds<T>,
}

impl<T: DecimalExt> MultiplyCheckedIntegral<T> {
    pub(super) fn new(size: DecimalSize) -> Self {
        assert_storage::<T>(size, "result");
        Self {
            size,
            bounds: Bounds::new(size.precision()),
        }
    }
}

impl<T: DecimalExt> Kernel for MultiplyCheckedIntegral<T> {
    type Compute = T;

    fn eval<A: DecimalExt, B: DecimalExt>(
        &self,
        args: DecimalArgs<'_, A, B>,
        ctx: &mut EvalContext,
    ) -> Value<AnyType> {
        checked::<true, A, B, T, T, _>(
            args,
            ctx,
            self.size,
            |a, b| row::checked_integral(a, b, &self.bounds).ok_or("Decimal multiply overflow"),
            T::one(),
        )
    }
}

pub(super) struct MultiplyCheckedRounded<T: DecimalExt> {
    size: DecimalSize,
    rounded: T::Rounded,
}

impl<T: DecimalExt> MultiplyCheckedRounded<T> {
    pub(super) fn new(size: DecimalSize, shift: u32) -> Self {
        assert_storage::<T>(size, "result");
        Self {
            size,
            rounded: T::prepare_rounded(shift, size.precision()),
        }
    }
}

impl<T: DecimalExt> Kernel for MultiplyCheckedRounded<T> {
    type Compute = T;

    fn eval<A: DecimalExt, B: DecimalExt>(
        &self,
        args: DecimalArgs<'_, A, B>,
        ctx: &mut EvalContext,
    ) -> Value<AnyType> {
        checked::<true, A, B, T, T, _>(
            args,
            ctx,
            self.size,
            |a, b| {
                a.multiply_rounded(b, &self.rounded)
                    .ok_or("Decimal multiply overflow")
            },
            T::one(),
        )
    }
}

// Division.

/// Quotient below 10^18: an i64 numerator with any divisor storage, read
/// through the saturating divisor view.
pub(super) struct DivideNative64 {
    size: DecimalSize,
    multiplier: i64,
}

impl DivideNative64 {
    pub(super) fn new(size: DecimalSize, shift: u32) -> Self {
        assert_storage::<i64>(size, "result");
        Self {
            size,
            multiplier: i64::e(shift as u8),
        }
    }
}

impl Kernel for DivideNative64 {
    type Compute = i64;

    fn eval<A: DecimalExt, B: DecimalExt>(
        &self,
        args: DecimalArgs<'_, A, B>,
        ctx: &mut EvalContext,
    ) -> Value<AnyType> {
        divide64::<A, B>(args, ctx, self.size, self.multiplier)
    }

    // The divisor is never widened, so it may be stored wider than i64.
    fn bind(self, inputs: [InputPlan; 2]) -> Box<dyn ScalarFunction> {
        use DecimalDataKind::*;
        match inputs.map(|input| input.storage()) {
            [Decimal64, Decimal64] => evaluator::<Self, i64, i64>(self, inputs),
            [Decimal64, Decimal128] => evaluator::<Self, i64, i128>(self, inputs),
            [Decimal64, Decimal256] => evaluator::<Self, i64, i256>(self, inputs),
            storage => unreachable!("internal Native64 numerator storage {storage:?}"),
        }
    }
}

/// Unclamped i128 quotient: the scaled numerator plus rounding offset fits.
pub(super) struct DivideNative128 {
    size: DecimalSize,
    multiplier: i128,
}

impl DivideNative128 {
    pub(super) fn new(size: DecimalSize, shift: u32) -> Self {
        assert_storage::<i128>(size, "result");
        Self {
            size,
            multiplier: i128::e(shift as u8),
        }
    }
}

impl Kernel for DivideNative128 {
    type Compute = i128;

    fn eval<A: DecimalExt, B: DecimalExt>(
        &self,
        args: DecimalArgs<'_, A, B>,
        ctx: &mut EvalContext,
    ) -> Value<AnyType> {
        checked::<true, A, B, i128, i128, _>(
            args,
            ctx,
            self.size,
            |a, b| {
                if b == 0 {
                    Err("divided by zero")
                } else {
                    Ok(row::native_scaled(a, b, self.multiplier))
                }
            },
            1,
        )
    }
}

pub(super) struct DivideChecked<T, U, S> {
    size: DecimalSize,
    divide: CheckedDivide<T, U, S>,
}

impl<T: DecimalExt, U: DecimalExt, S: DivideScaling> DivideChecked<T, U, S> {
    pub(super) fn new(size: DecimalSize, scaling: S, shift: u32) -> Self {
        Self {
            size,
            divide: CheckedDivide::new(scaling, shift, size.precision()),
        }
    }
}

impl<T: DecimalExt, U: DecimalExt, S: DivideScaling> Kernel for DivideChecked<T, U, S> {
    type Compute = T;

    fn eval<A: DecimalExt, B: DecimalExt>(
        &self,
        args: DecimalArgs<'_, A, B>,
        ctx: &mut EvalContext,
    ) -> Value<AnyType> {
        checked::<true, A, B, T, U, _>(
            args,
            ctx,
            self.size,
            |a, b| {
                if b == T::zero() {
                    Err("divided by zero")
                } else {
                    self.divide.divide(a, b).ok_or("Decimal div overflow")
                }
            },
            U::one(),
        )
    }
}

// Remainder. Computation `T` may be wider than result `U`.

/// Both operands already have the result scale.
pub(super) struct ModuloSame<T, U> {
    size: DecimalSize,
    storage: PhantomData<(T, U)>,
}

impl<T: DecimalExt, U: DecimalExt> ModuloSame<T, U> {
    pub(super) fn new(size: DecimalSize) -> Self {
        assert_storage::<U>(size, "result");
        Self {
            size,
            storage: PhantomData,
        }
    }
}

impl<T: DecimalExt, U: DecimalExt> Kernel for ModuloSame<T, U> {
    type Compute = T;

    fn eval<A: DecimalExt, B: DecimalExt>(
        &self,
        args: DecimalArgs<'_, A, B>,
        ctx: &mut EvalContext,
    ) -> Value<AnyType> {
        checked::<true, A, B, T, U, _>(
            args,
            ctx,
            self.size,
            |a, b| {
                if b == T::zero() {
                    Err("divided by zero")
                } else {
                    Ok(a.wrapping_rem(b).as_decimal::<U>())
                }
            },
            U::zero(),
        )
    }
}

pub(super) struct ModuloAligned<const LEFT: bool, T, U> {
    size: DecimalSize,
    aligned: Aligned<T, LEFT>,
    output: PhantomData<U>,
}

impl<const LEFT: bool, T: DecimalExt, U: DecimalExt> ModuloAligned<LEFT, T, U> {
    pub(super) fn new(size: DecimalSize, shift: u32) -> Self {
        assert_storage::<U>(size, "result");
        Self {
            size,
            aligned: Aligned::new(shift, size.precision()),
            output: PhantomData,
        }
    }
}

impl<const LEFT: bool, T: DecimalExt, U: DecimalExt> Kernel for ModuloAligned<LEFT, T, U> {
    type Compute = T;

    fn eval<A: DecimalExt, B: DecimalExt>(
        &self,
        args: DecimalArgs<'_, A, B>,
        ctx: &mut EvalContext,
    ) -> Value<AnyType> {
        checked::<true, A, B, T, U, _>(
            args,
            ctx,
            self.size,
            |a, b| {
                if b == T::zero() {
                    Err("divided by zero")
                } else {
                    self.aligned
                        .remainder(a, b)
                        .map(|value| value.as_decimal::<U>())
                        .ok_or("Decimal modulo overflow")
                }
            },
            U::zero(),
        )
    }
}
