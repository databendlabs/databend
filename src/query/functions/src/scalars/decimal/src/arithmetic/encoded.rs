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

//! Plus, minus and multiply over frame of reference encoded operands.
//!
//! The arithmetic plan attaches this backend only to same-scale addition and
//! subtraction or integral multiplication. An encoded column must already have
//! its bound operand scale; scalar conversion remains deferred until runtime. At
//! run time the bounds of the operands must prove that every result, hidden
//! rows included, fits both `i64` and the result precision. The result is then
//! computed in `i64` and returned frame of reference encoded, whatever storage
//! the result size implies. Otherwise every encoded operand is decoded and the
//! bound kernel runs as usual.
//!
//! TODO: extend to the other frame of reference encodable types (integers,
//! `Date`, `Timestamp`). Their arithmetic is registered through the generic
//! function builder, whose `accepts_encoded_args` is `false`, so encoded
//! operands are decoded before the call. The bounds and chunked kernel here are
//! not decimal specific; only the scale and precision checks are.

use databend_common_expression::Column;
use databend_common_expression::EvalContext;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarFunction;
use databend_common_expression::Value;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::*;
use databend_common_expression::with_narrow_buffer;

use super::InputPlan;
use super::plan::BinaryBinding;

#[derive(Clone, Copy, Debug)]
pub(super) enum Op {
    Plus,
    Minus,
    Multiply,
}

/// The bound function with an encoded fast path in front of it.
pub(super) struct EncodedFastPath {
    op: Op,
    inputs: [InputPlan; 2],
    // Both bound operand sizes; an encoded column must have this scale.
    operands: [DecimalSize; 2],
    result: DecimalSize,
    bound: Box<dyn ScalarFunction>,
}

impl EncodedFastPath {
    /// Attaches the raw-coefficient backend selected by the arithmetic plan.
    /// These invariants must hold independently of the runtime range proof.
    pub(super) fn bind(
        op: Op,
        binding: &BinaryBinding,
        inputs: [InputPlan; 2],
        bound: Box<dyn ScalarFunction>,
    ) -> Box<dyn ScalarFunction> {
        match op {
            Op::Plus | Op::Minus => {
                assert_eq!(binding.left.scale(), binding.result.scale());
                assert_eq!(binding.right.scale(), binding.result.scale());
            }
            Op::Multiply => assert_eq!(
                binding.left.scale() + binding.right.scale(),
                binding.result.scale()
            ),
        }
        Box::new(Self {
            op,
            inputs,
            operands: [binding.left, binding.right],
            result: binding.result,
            bound,
        })
    }

    fn eval_encoded(&self, args: &[Value<AnyType>], ctx: &EvalContext) -> Option<Value<AnyType>> {
        let lhs = Operand::new(&args[0], &self.inputs[0], self.operands[0], ctx)?;
        let rhs = Operand::new(&args[1], &self.inputs[1], self.operands[1], ctx)?;
        let len = lhs.len().or(rhs.len())?;

        let ((a_min, a_max), (b_min, b_max)) = (lhs.bounds(), rhs.bounds());
        let (min, max) = match self.op {
            Op::Plus => (a_min + b_min, a_max + b_max),
            Op::Minus => (a_min - b_max, a_max - b_min),
            Op::Multiply => {
                let corners = [a_min * b_min, a_min * b_max, a_max * b_min, a_max * b_max];
                (*corners.iter().min()?, *corners.iter().max()?)
            }
        };
        let limit = 10i128
            .checked_pow(self.result.precision() as u32)
            .map_or(i128::MAX, |p| p - 1);
        if min < (-limit).max(i64::MIN as i128) || max > limit.min(i64::MAX as i128) {
            return None;
        }
        let max_delta = (max - min) as u64;
        let deltas = compute_deltas(self.op, &lhs, &rhs, len, min as i64, max_delta);
        let column = ForColumn::new(DataType::Decimal(self.result), min, max_delta, deltas);
        Some(Value::Column(Column::Encoded(Box::new(
            EncodedColumn::For(column),
        ))))
    }
}

impl ScalarFunction for EncodedFastPath {
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        if !args.iter().any(Value::is_encoded) {
            return self.bound.eval(args, ctx);
        }
        if let Some(value) = self.eval_encoded(args, ctx) {
            return value;
        }
        let args = args
            .iter()
            .map(|arg| arg.decoded().into_owned())
            .collect::<Vec<_>>();
        self.bound.eval(&args, ctx)
    }

    fn accepts_encoded_args(&self) -> bool {
        true
    }
}

/// Raw `i64` values of one operand at its bound scale.
enum Operand<'a> {
    Column {
        base: i64,
        max_delta: u64,
        deltas: &'a NarrowBuffer,
    },
    Scalar(i64),
}

impl<'a> Operand<'a> {
    fn new(
        value: &'a Value<AnyType>,
        input: &InputPlan,
        size: DecimalSize,
        ctx: &EvalContext,
    ) -> Option<Self> {
        match value {
            Value::Column(Column::Encoded(encoded)) => {
                let column = encoded.as_for()?;
                let DataType::Decimal(column_size) = column.data_type() else {
                    return None;
                };
                if column_size.scale() != size.scale() || !column.fits::<i64>() {
                    return None;
                }
                Some(Operand::Column {
                    base: column.base() as i64,
                    max_delta: column.max_delta(),
                    deltas: column.deltas(),
                })
            }
            Value::Scalar(_) => {
                // A speculative conversion must not change the caller's error
                // state: on rejection the ordinary kernel converts again.
                let mut convert_ctx = EvalContext {
                    generics: ctx.generics,
                    num_rows: ctx.num_rows,
                    func_ctx: ctx.func_ctx,
                    validity: ctx.validity.clone(),
                    errors: None,
                    suppress_error: ctx.suppress_error,
                    strict_eval: ctx.strict_eval,
                };
                let Value::Scalar(Scalar::Decimal(scalar)) = input.convert(value, &mut convert_ctx)
                else {
                    return None;
                };
                if convert_ctx.errors.is_some() {
                    return None;
                }
                let raw = match scalar {
                    DecimalScalar::Decimal64(v, _) => v as i128,
                    DecimalScalar::Decimal128(v, _) => v,
                    DecimalScalar::Decimal256(v, _) => {
                        let raw = v.as_i128();
                        (i256::from(raw) == v).then_some(raw)?
                    }
                };
                i64::try_from(raw).ok().map(Operand::Scalar)
            }
            Value::Column(_) => None,
        }
    }

    fn len(&self) -> Option<usize> {
        match self {
            Operand::Column { deltas, .. } => Some(deltas.len()),
            Operand::Scalar(_) => None,
        }
    }

    fn bounds(&self) -> (i128, i128) {
        match self {
            Operand::Column {
                base, max_delta, ..
            } => (*base as i128, *base as i128 + *max_delta as i128),
            Operand::Scalar(v) => (*v as i128, *v as i128),
        }
    }

    /// Writes the values of rows `start..start + out.len()` into `out`.
    fn fill(&self, start: usize, out: &mut [i64]) {
        match self {
            Operand::Scalar(v) => out.fill(*v),
            Operand::Column { base, deltas, .. } => with_narrow_buffer!(*deltas, |d| {
                let deltas = &d.as_slice()[start..start + out.len()];
                for (value, delta) in out.iter_mut().zip(deltas) {
                    *value = base.wrapping_add(*delta as i64);
                }
            }),
        }
    }
}

/// Computes `lhs <op> rhs` chunk by chunk and stores the results as deltas from `min`.
fn compute_deltas(
    op: Op,
    lhs: &Operand,
    rhs: &Operand,
    len: usize,
    min: i64,
    max_delta: u64,
) -> NarrowBuffer {
    const CHUNK: usize = 1024;
    let mut lhs_values = [0i64; CHUNK];
    let mut rhs_values = [0i64; CHUNK];

    macro_rules! compute {
        ($t:ty, $variant:ident) => {{
            let mut out = Vec::<$t>::with_capacity(len);
            for start in (0..len).step_by(CHUNK) {
                let n = (len - start).min(CHUNK);
                let (a, b) = (&mut lhs_values[..n], &mut rhs_values[..n]);
                lhs.fill(start, a);
                rhs.fill(start, b);
                // Wrapping arithmetic is exact: the bounds prove every result fits `i64`.
                let pairs = a.iter_mut().zip(b.iter());
                match op {
                    Op::Plus => pairs.for_each(|(x, y)| *x = x.wrapping_add(*y)),
                    Op::Minus => pairs.for_each(|(x, y)| *x = x.wrapping_sub(*y)),
                    Op::Multiply => pairs.for_each(|(x, y)| *x = x.wrapping_mul(*y)),
                }
                out.extend(a.iter().map(|v| v.wrapping_sub(min) as $t));
            }
            NarrowBuffer::$variant(out.into())
        }};
    }

    match NarrowBuffer::width_for(max_delta) {
        1 => compute!(u8, U8),
        2 => compute!(u16, U16),
        4 => compute!(u32, U32),
        _ => compute!(u64, U64),
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FunctionContext;
    use num_bigint::BigInt;

    use super::*;
    use crate::testing::*;

    #[test]
    fn speculative_scalar_conversion_preserves_errors() {
        let func_ctx = FunctionContext::default();
        let mut ctx = context(&func_ctx, 2, None);
        ctx.set_error(1, "existing error");
        let original = ctx.clone();
        let from = DecimalSize::new(38, 0).unwrap();
        let to = DecimalSize::new(38, 1).unwrap();
        let input = InputPlan::prepare(&DataType::Decimal(from), to);
        let scalar = |raw: BigInt| Value::Scalar(scalar_value(&DataType::Decimal(from), &raw));
        // Conversion succeeds, but the result is too wide for the FOR backend.
        assert!(Operand::new(&scalar(pow10(20)), &input, to, &ctx).is_none());
        assert_eq!(ctx, original);
        // A failed conversion's fallback coefficient must not be accepted as
        // an actual operand, and its error must not leak into the caller.
        assert!(Operand::new(&scalar(pow10(37)), &input, to, &ctx).is_none());
        assert_eq!(ctx, original);
        assert!(Operand::new(&scalar(BigInt::from(1)), &input, to, &ctx).is_some());
        assert_eq!(ctx, original);
    }
}
