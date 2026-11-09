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

use databend_common_expression::EvalContext;
use databend_common_expression::ScalarFunction;
use databend_common_expression::Value;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::*;

use super::ArithmeticOp;
use super::decision;
use super::decision::AddSubtractPlan;
use super::decision::Alignment;
pub(super) use super::decision::BinaryBinding;
use super::decision::DividePlan;
use super::decision::MultiplyPlan;
use super::encoded::EncodedFastPath;
use super::encoded::Op as EncodedOp;
use super::kernel::*;
use super::row::Scale128;
use super::row::Scale256;
use super::row::ScaleWide;
use super::row::Unscaled;
use crate::decimal::DecimalExt;
use crate::decimal_to_decimal;
use crate::other_to_decimal;

impl BinaryBinding {
    pub(super) fn new(op: ArithmeticOp, original: &[DecimalSize; 2]) -> Self {
        match op {
            ArithmeticOp::Plus | ArithmeticOp::Minus => decision::add_subtract_size(original),
            ArithmeticOp::Multiply => decision::multiply_size(original),
            ArithmeticOp::Divide => decision::divide_size(original),
            ArithmeticOp::Modulo => decision::modulo_size(original),
        }
    }
}

/// How one operand reaches its bound size. A decimal value always uses the
/// canonical storage of its declared precision, so the recipe also fixes the
/// storage type that the kernel reads.
#[derive(Clone, Copy, Debug)]
pub(crate) enum InputPlan {
    /// The operand already has the bound scale and fits the bound precision.
    /// Its buffer is read as-is; narrower storage widens lazily per row.
    Keep(DecimalSize),
    Rescale {
        from: DecimalSize,
        to: DecimalSize,
    },
    Integer {
        ty: NumberDataType,
        size: DecimalSize,
    },
}

impl InputPlan {
    // The factory has already accepted the numeric signature. There is no
    // overload search here; unsupported types are an internal caller error.
    pub(super) fn prepare(data_type: &DataType, bound: DecimalSize) -> Self {
        match data_type {
            DataType::Decimal(from) if from.scale() != bound.scale() => Self::Rescale {
                from: *from,
                to: bound,
            },
            DataType::Decimal(from) => {
                assert!(
                    from.precision() <= bound.precision(),
                    "internal arithmetic binding narrows {from:?} to {bound:?}"
                );
                Self::Keep(*from)
            }
            DataType::Number(ty) => {
                let from = data_type.get_decimal_properties().unwrap_or_else(|| {
                    panic!("internal arithmetic input preparation received unsupported {data_type}")
                });
                // Every integer fits its own decimal size exactly, so a
                // zero-scale binding keeps that narrower size.
                let size = if bound.scale() == 0 && from.precision() <= bound.precision() {
                    from
                } else {
                    bound
                };
                Self::Integer { ty: *ty, size }
            }
            _ => panic!("internal arithmetic input preparation received unsupported {data_type}"),
        }
    }

    /// Storage of the converted operand, fixed at binding.
    pub(crate) fn storage(&self) -> DecimalDataKind {
        match self {
            Self::Keep(size) | Self::Rescale { to: size, .. } | Self::Integer { size, .. } => {
                size.data_kind()
            }
        }
    }

    pub(super) fn convert(&self, value: &Value<AnyType>, ctx: &mut EvalContext) -> Value<AnyType> {
        match *self {
            Self::Keep(size) => {
                debug_assert_storage(value, size);
                value.clone()
            }
            Self::Rescale { from, to } => {
                debug_assert_storage(value, from);
                decimal_to_decimal(value, ctx, from.into(), to.into())
            }
            Self::Integer { ty, size } => {
                other_to_decimal(value, None, ctx, &DataType::Number(ty), size.into())
            }
        }
    }
}

fn debug_assert_storage(value: &Value<AnyType>, size: DecimalSize) {
    debug_assert_eq!(
        DecimalDataType::from_value(value).map(|(ty, _)| ty.data_kind()),
        Some(size.data_kind()),
        "decimal operand storage does not match its declared {size:?}"
    );
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ModuloPlan {
    Same,
    Aligned(Alignment),
}

impl ModuloPlan {
    pub(super) fn new(binding: &BinaryBinding) -> Self {
        match Alignment::new(binding) {
            None => Self::Same,
            Some(alignment) => Self::Aligned(alignment),
        }
    }
}

pub(super) fn bind(
    op: ArithmeticOp,
    binding: BinaryBinding,
    inputs: [InputPlan; 2],
) -> Box<dyn ScalarFunction> {
    match op {
        ArithmeticOp::Plus => bind_add_subtract::<false>(binding, inputs),
        ArithmeticOp::Minus => bind_add_subtract::<true>(binding, inputs),
        ArithmeticOp::Multiply => bind_multiply(binding, inputs),
        ArithmeticOp::Divide => bind_divide(binding, inputs),
        ArithmeticOp::Modulo => bind_modulo(binding, inputs),
    }
}

fn unreachable_plan(plan: impl std::fmt::Debug, binding: &BinaryBinding, why: &str) -> ! {
    unreachable!("internal decimal plan {plan:?} for {binding:?}: {why}")
}

macro_rules! with_storage {
    ($kind:expr, |$T:ident| $body:expr) => {
        match $kind {
            DecimalDataKind::Decimal64 => {
                type $T = i64;
                $body
            }
            DecimalDataKind::Decimal128 => {
                type $T = i128;
                $body
            }
            DecimalDataKind::Decimal256 => {
                type $T = i256;
                $body
            }
        }
    };
}

// For plans that only a clamped (38 or 76 digit) precision can produce.
macro_rules! with_wide_storage {
    ($kind:expr, |$T:ident| $body:expr, narrow => $narrow:expr) => {
        match $kind {
            DecimalDataKind::Decimal64 => $narrow,
            DecimalDataKind::Decimal128 => {
                type $T = i128;
                $body
            }
            DecimalDataKind::Decimal256 => {
                type $T = i256;
                $body
            }
        }
    };
}

fn bind_add_subtract<const SUBTRACT: bool>(
    binding: BinaryBinding,
    inputs: [InputPlan; 2],
) -> Box<dyn ScalarFunction> {
    let size = binding.result;
    let kind = size.data_kind();
    let plan = AddSubtractPlan::new(&binding);
    let narrow = || -> ! {
        unreachable_plan(
            plan,
            &binding,
            "an i64 result precision is unclamped, so the carry always fits",
        )
    };
    let bound = match plan {
        AddSubtractPlan::Native => with_storage!(kind, |T| {
            AddSubtractNative::<SUBTRACT, T>::new(size).bind(inputs)
        }),
        AddSubtractPlan::Checked => with_wide_storage!(
            kind,
            |T| AddSubtractChecked::<SUBTRACT, T>::new(size).bind(inputs),
            narrow => narrow()
        ),
        AddSubtractPlan::Aligned(alignment) => with_wide_storage!(
            kind,
            |T| match alignment {
                Alignment::Left(shift) => {
                    AddSubtractAligned::<SUBTRACT, true, T>::new(size, shift).bind(inputs)
                }
                Alignment::Right(shift) => {
                    AddSubtractAligned::<SUBTRACT, false, T>::new(size, shift).bind(inputs)
                }
            },
            narrow => narrow()
        ),
    };
    match plan {
        AddSubtractPlan::Native | AddSubtractPlan::Checked => EncodedFastPath::bind(
            if SUBTRACT {
                EncodedOp::Minus
            } else {
                EncodedOp::Plus
            },
            &binding,
            inputs,
            bound,
        ),
        AddSubtractPlan::Aligned(_) => bound,
    }
}

fn bind_multiply(binding: BinaryBinding, inputs: [InputPlan; 2]) -> Box<dyn ScalarFunction> {
    let size = binding.result;
    let kind = size.data_kind();
    let plan = MultiplyPlan::new(&binding);
    let bound = match plan {
        MultiplyPlan::Integral => {
            with_storage!(kind, |T| MultiplyIntegral::<T>::new(size).bind(inputs))
        }
        MultiplyPlan::Rounded(shift) => with_storage!(kind, |T| {
            MultiplyRounded::<T>::new(size, shift).bind(inputs)
        }),
        MultiplyPlan::CheckedIntegral => with_wide_storage!(
            kind,
            |T| MultiplyCheckedIntegral::<T>::new(size).bind(inputs),
            narrow => unreachable_plan(
                plan,
                &binding,
                "an unscaled i64 product has exactly the unclamped result precision",
            )
        ),
        MultiplyPlan::CheckedRounded(shift) => with_storage!(kind, |T| {
            MultiplyCheckedRounded::<T>::new(size, shift).bind(inputs)
        }),
    };
    match plan {
        MultiplyPlan::Integral | MultiplyPlan::CheckedIntegral => {
            EncodedFastPath::bind(EncodedOp::Multiply, &binding, inputs, bound)
        }
        MultiplyPlan::Rounded(_) | MultiplyPlan::CheckedRounded(_) => bound,
    }
}

fn bind_divide(binding: BinaryBinding, inputs: [InputPlan; 2]) -> Box<dyn ScalarFunction> {
    use DecimalDataKind::*;
    let size = binding.result;
    let plan = DividePlan::new(&binding);
    match plan {
        DividePlan::Native64 { shift } => DivideNative64::new(size, shift).bind(inputs),
        DividePlan::Native128 { shift } => DivideNative128::new(size, shift).bind(inputs),
        DividePlan::Checked { shift } => match (binding.compute().data_kind(), size.data_kind()) {
            (Decimal128, Decimal128) => bind_checked_divide::<i128, i128>(size, shift, inputs),
            (Decimal256, Decimal128) => bind_checked_divide::<i256, i128>(size, shift, inputs),
            (Decimal256, Decimal256) => bind_checked_divide::<i256, i256>(size, shift, inputs),
            _ => unreachable_plan(plan, &binding, "an i64 quotient is unclamped, hence native"),
        },
    }
}

fn bind_checked_divide<T: DecimalExt, U: DecimalExt>(
    size: DecimalSize,
    shift: u32,
    inputs: [InputPlan; 2],
) -> Box<dyn ScalarFunction> {
    match shift {
        0 => DivideChecked::<T, U, _>::new(size, Unscaled, shift).bind(inputs),
        1..=38 => DivideChecked::<T, U, _>::new(size, Scale128::new(shift), shift).bind(inputs),
        39..=76 => DivideChecked::<T, U, _>::new(size, Scale256::new(shift), shift).bind(inputs),
        _ => DivideChecked::<T, U, _>::new(size, ScaleWide, shift).bind(inputs),
    }
}

fn bind_modulo(binding: BinaryBinding, inputs: [InputPlan; 2]) -> Box<dyn ScalarFunction> {
    use DecimalDataKind::*;
    let size = binding.result;
    let plan = ModuloPlan::new(&binding);
    match (plan, binding.compute().data_kind(), size.data_kind()) {
        (ModuloPlan::Same, Decimal64, Decimal64) => ModuloSame::<i64, i64>::new(size).bind(inputs),
        (ModuloPlan::Same, Decimal128, Decimal64) => {
            ModuloSame::<i128, i64>::new(size).bind(inputs)
        }
        (ModuloPlan::Same, Decimal128, Decimal128) => {
            ModuloSame::<i128, i128>::new(size).bind(inputs)
        }
        (ModuloPlan::Same, Decimal256, kind) => {
            with_storage!(kind, |U| ModuloSame::<i256, U>::new(size).bind(inputs))
        }
        (ModuloPlan::Aligned(alignment), Decimal256, kind) => with_storage!(kind, |U| {
            match alignment {
                Alignment::Left(shift) => {
                    ModuloAligned::<true, i256, U>::new(size, shift).bind(inputs)
                }
                Alignment::Right(shift) => {
                    ModuloAligned::<false, i256, U>::new(size, shift).bind(inputs)
                }
            }
        }),
        (ModuloPlan::Same, ..) => unreachable_plan(
            plan,
            &binding,
            "the remainder is never wider than its operands",
        ),
        (ModuloPlan::Aligned(_), ..) => unreachable_plan(
            plan,
            &binding,
            "operands stay at their own scales only when aligning needs more than 76 digits",
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::*;

    const OPS: [ArithmeticOp; 5] = ArithmeticOp::ALL;

    // Every legal signature binds: sizes are legal, every operand converts to its
    // bound size without overflow, and the binder reaches only the storage arms
    // its plans declare reachable (the others are `unreachable!`).
    #[test]
    fn sampled_signatures_bind() {
        let types = operand_types(1..=76);
        for [x, y] in signatures(&types) {
            let original = [x, y].map(|ty| ty.get_decimal_properties().unwrap());
            for op in OPS {
                let binding = BinaryBinding::new(op, &original);
                let context = format!("{op:?} {x} {y} -> {binding:?}");
                // Check the metadata preconditions of the verified decisions at
                // the real sizing/binding boundary (not an alternate selector).
                let [a, b] = binding.original;
                let result = binding.result;
                let compute = binding.compute();
                for size in [a, b, result] {
                    assert!(compute.precision() >= size.precision(), "{context}");
                }
                match op {
                    ArithmeticOp::Plus | ArithmeticOp::Minus => {
                        assert_eq!(result.scale(), a.scale().max(b.scale()), "{context}");
                        assert_eq!(
                            result.scale(),
                            binding.left.scale().max(binding.right.scale()),
                            "{context}"
                        );
                    }
                    ArithmeticOp::Multiply => {
                        assert!(result.scale() >= a.scale().max(b.scale()), "{context}");
                        assert!(result.scale() <= a.scale() + b.scale(), "{context}");
                    }
                    ArithmeticOp::Divide => assert!(result.scale() >= a.scale(), "{context}"),
                    ArithmeticOp::Modulo => {}
                }
                for size in [binding.left, binding.right, binding.result] {
                    DecimalSize::new(size.precision(), size.scale()).expect(&context);
                }
                assert_eq!(
                    binding.left.precision(),
                    binding.right.precision(),
                    "{context}"
                );
                if matches!(op, ArithmeticOp::Divide | ArithmeticOp::Modulo) {
                    assert!(binding.compute().precision() >= binding.result.precision());
                }
                let inputs = [
                    (x, original[0], binding.left),
                    (y, original[1], binding.right),
                ]
                .map(|(ty, from, bound)| {
                    // Rescaling only appends digits, and the integer part
                    // keeps room, so a legal operand never overflows.
                    assert!(from.scale() <= bound.scale(), "{context}");
                    assert!(from.leading_digits() <= bound.leading_digits(), "{context}");
                    let input = InputPlan::prepare(ty, bound);
                    assert!(
                        width(input.storage()) <= width(binding.compute().data_kind()),
                        "{context}"
                    );
                    input
                });
                let eval = bind(op, binding, inputs);
                let accepts_encoded = match op {
                    ArithmeticOp::Plus | ArithmeticOp::Minus => {
                        !matches!(AddSubtractPlan::new(&binding), AddSubtractPlan::Aligned(_))
                    }
                    ArithmeticOp::Multiply => matches!(
                        MultiplyPlan::new(&binding),
                        MultiplyPlan::Integral | MultiplyPlan::CheckedIntegral
                    ),
                    ArithmeticOp::Divide | ArithmeticOp::Modulo => false,
                };
                assert_eq!(eval.accepts_encoded_args(), accepts_encoded, "{context}");
            }
        }
    }

    fn width(kind: DecimalDataKind) -> u8 {
        match kind {
            DecimalDataKind::Decimal64 => 64,
            DecimalDataKind::Decimal128 => 128,
            DecimalDataKind::Decimal256 => 255,
        }
    }

    fn binding(op: ArithmeticOp, a: (u8, u8), b: (u8, u8)) -> BinaryBinding {
        let size = |(p, s)| DecimalSize::new(p, s).unwrap();
        BinaryBinding::new(op, &[size(a), size(b)])
    }

    fn size(p: u8, s: u8) -> DecimalSize {
        DecimalSize::new(p, s).unwrap()
    }

    // Each plan arm at the size boundaries that select it.
    #[test]
    fn plan_selection() {
        use AddSubtractPlan as A;
        use Alignment::*;
        for (a, b, result, plan) in [
            ((17, 0), (17, 0), size(18, 0), A::Native),
            ((18, 0), (18, 0), size(19, 0), A::Native),
            ((37, 0), (37, 0), size(38, 0), A::Native),
            // Clamped precision: the carry may not fit.
            ((38, 0), (38, 0), size(38, 0), A::Checked),
            ((76, 0), (76, 0), size(76, 0), A::Checked),
            // Clamped precision with different scales: one side scales up.
            ((38, 0), (38, 38), size(38, 38), A::Aligned(Left(38))),
            ((38, 38), (38, 0), size(38, 38), A::Aligned(Right(38))),
            ((76, 0), (76, 76), size(76, 76), A::Aligned(Left(76))),
            // Unclamped precision with different scales rescales on input.
            ((10, 2), (10, 5), size(14, 5), A::Native),
        ] {
            for op in [ArithmeticOp::Plus, ArithmeticOp::Minus] {
                let binding = binding(op, a, b);
                assert_eq!(binding.result, result, "{op:?} {a:?} {b:?}");
                assert_eq!(AddSubtractPlan::new(&binding), plan, "{op:?} {a:?} {b:?}");
            }
        }

        use MultiplyPlan as M;
        for (a, b, result, plan) in [
            ((7, 0), (8, 0), size(15, 0), M::Integral),
            ((10, 0), (9, 0), size(19, 0), M::Integral),
            ((39, 0), (20, 0), size(59, 0), M::Integral),
            ((7, 7), (7, 7), size(12, 12), M::Rounded(2)),
            ((39, 20), (20, 10), size(49, 20), M::Rounded(10)),
            ((38, 0), (38, 0), size(38, 0), M::CheckedIntegral),
            ((76, 0), (76, 0), size(76, 0), M::CheckedIntegral),
            // The product exceeds the storage although the result does not.
            ((18, 18), (18, 18), size(18, 18), M::CheckedRounded(18)),
            ((37, 37), (37, 37), size(37, 37), M::CheckedRounded(37)),
            ((76, 38), (76, 38), size(76, 38), M::CheckedRounded(38)),
        ] {
            let binding = binding(ArithmeticOp::Multiply, a, b);
            assert_eq!(binding.result, result, "{a:?} {b:?}");
            assert_eq!(MultiplyPlan::new(&binding), plan, "{a:?} {b:?}");
        }

        use DividePlan as D;
        for (a, b, result, compute, plan) in [
            // A narrow quotient keeps a wide divisor unnarrowed.
            ((4, 0), (76, 0), size(10, 6), 76, D::Native64 { shift: 6 }),
            ((12, 0), (1, 0), size(18, 6), 18, D::Native64 { shift: 6 }),
            ((18, 0), (1, 0), size(24, 6), 24, D::Native128 { shift: 6 }),
            ((38, 0), (38, 0), size(38, 6), 38, D::Checked { shift: 6 }),
            ((39, 0), (1, 0), size(45, 6), 45, D::Checked { shift: 6 }),
            ((30, 0), (76, 0), size(36, 6), 76, D::Checked { shift: 6 }),
            // Scaling needs more digits than any intermediate.
            ((12, 0), (76, 76), size(76, 6), 76, D::Checked { shift: 82 }),
        ] {
            let binding = binding(ArithmeticOp::Divide, a, b);
            assert_eq!(binding.result, result, "{a:?} {b:?}");
            assert_eq!(binding.compute().precision(), compute, "{a:?} {b:?}");
            assert_eq!(DividePlan::new(&binding), plan, "{a:?} {b:?}");
        }

        use ModuloPlan as R;
        for (a, b, result, plan) in [
            ((10, 0), (4, 0), size(4, 0), R::Same),
            ((30, 6), (30, 12), size(30, 12), R::Same),
            // Aligning in place would need more than 76 digits.
            ((76, 0), (76, 76), size(76, 76), R::Aligned(Left(76))),
            ((76, 76), (76, 0), size(76, 76), R::Aligned(Right(76))),
        ] {
            let binding = binding(ArithmeticOp::Modulo, a, b);
            assert_eq!(binding.result, result, "{a:?} {b:?}");
            assert_eq!(ModuloPlan::new(&binding), plan, "{a:?} {b:?}");
        }
    }

    // How each operand reaches its bound size, and the storage the kernel reads.
    #[test]
    fn operand_recipes() {
        let int = DataType::Number;
        for (from, bound, recipe, storage) in [
            // Same scale: the buffer is read as-is and widens lazily.
            (
                decimal_type(4, 0),
                size(38, 0),
                "Keep",
                DecimalDataKind::Decimal64,
            ),
            (
                decimal_type(38, 2),
                size(76, 2),
                "Keep",
                DecimalDataKind::Decimal128,
            ),
            (
                decimal_type(10, 2),
                size(20, 4),
                "Rescale",
                DecimalDataKind::Decimal128,
            ),
            // A zero-scale integer keeps its own exact size.
            (
                int(NumberDataType::Int8),
                size(38, 0),
                "Integer",
                DecimalDataKind::Decimal64,
            ),
            (
                int(NumberDataType::Int64),
                size(38, 0),
                "Integer",
                DecimalDataKind::Decimal128,
            ),
            (
                int(NumberDataType::UInt64),
                size(76, 0),
                "Integer",
                DecimalDataKind::Decimal128,
            ),
            (
                int(NumberDataType::Int8),
                size(10, 2),
                "Integer",
                DecimalDataKind::Decimal64,
            ),
            (
                int(NumberDataType::Int64),
                size(40, 2),
                "Integer",
                DecimalDataKind::Decimal256,
            ),
        ] {
            let plan = InputPlan::prepare(&from, bound);
            assert!(
                format!("{plan:?}").starts_with(recipe),
                "{from} -> {bound:?}: {plan:?}"
            );
            assert_eq!(plan.storage(), storage, "{from} -> {bound:?}");
        }
    }
}
