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

use std::sync::Arc;

use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::ScalarFunctionDomain;
use databend_common_expression::Value;
use databend_common_expression::types::SimpleDomain;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_1_arg;

mod domain;
mod encoded;
mod kernel;
mod plan;
mod row;
pub(crate) use kernel::Kernel;
pub(crate) use kernel::evaluator;
use plan::BinaryBinding;
pub(crate) use plan::InputPlan;
pub(crate) use row::Rounded64;
pub(crate) use row::Rounded128;
pub(crate) use row::Rounded256;

use crate::decimal::DecimalExt;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum ArithmeticOp {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
}

impl ArithmeticOp {
    const ALL: [ArithmeticOp; 5] = [
        ArithmeticOp::Plus,
        ArithmeticOp::Minus,
        ArithmeticOp::Multiply,
        ArithmeticOp::Divide,
        ArithmeticOp::Modulo,
    ];

    const fn name(self) -> &'static str {
        match self {
            ArithmeticOp::Plus => "plus",
            ArithmeticOp::Minus => "minus",
            ArithmeticOp::Multiply => "multiply",
            ArithmeticOp::Divide => "divide",
            ArithmeticOp::Modulo => "modulo",
        }
    }

    // A legal decimal signature always has bound input/result sizes. Sizing
    // rule violations are internal errors, never an overload miss.
    fn size(&self, precision: u8, scale: u8) -> DecimalSize {
        DecimalSize::new(precision, scale).unwrap_or_else(|error| {
            panic!("internal {self:?} binding produced Decimal({precision},{scale}): {error}")
        })
    }

    fn result_size(
        &self,
        a: &DecimalSize,
        b: &DecimalSize,
    ) -> (DecimalSize, DecimalSize, DecimalSize) {
        if *self == ArithmeticOp::Modulo {
            return Self::modulo_size(a, b);
        }

        // from snowflake: https://docs.snowflake.com/sql-reference/operators-arithmetic
        let (precision, scale) = match self {
            ArithmeticOp::Multiply => {
                let scale = (a.scale() + b.scale()).min(a.scale().max(b.scale()).max(12));
                let leading = a.leading_digits() + b.leading_digits();
                (leading + scale, scale)
            }

            ArithmeticOp::Divide => {
                let scale = a.scale().max((a.scale() + 6).min(12)); // scale must be >= a.sale()
                let leading = a.leading_digits() + b.scale(); // leading must be >= a.leading_digits()
                (leading + scale, scale) // so precision must be >= a.precision()
            }

            ArithmeticOp::Plus | ArithmeticOp::Minus => {
                let scale = a.scale().max(b.scale());
                // for addition/subtraction, we add 1 to the width to ensure we don't overflow
                let plus_min_precision = a.leading_digits().max(b.leading_digits()) + scale + 1;
                (plus_min_precision, scale)
            }

            ArithmeticOp::Modulo => unreachable!(),
        };

        // if the args both are Decimal128, we need to clamp the precision to 38
        let precision =
            if a.precision() <= i128::MAX_PRECISION && b.precision() <= i128::MAX_PRECISION {
                precision.min(i128::MAX_PRECISION)
            } else {
                precision.min(i256::MAX_PRECISION)
            };

        let result_type = self.size(precision, scale);
        match self {
            ArithmeticOp::Multiply => (
                self.size(precision, a.scale()),
                self.size(precision, b.scale()),
                result_type,
            ),

            ArithmeticOp::Divide => {
                let p = precision.max(a.precision()).max(b.precision());
                (
                    self.size(p, a.scale()),
                    self.size(p, b.scale()),
                    result_type,
                )
            }

            ArithmeticOp::Plus | ArithmeticOp::Minus => {
                let required = a.leading_digits().max(b.leading_digits()) + scale + 1;
                if required <= precision {
                    (result_type, result_type, result_type)
                } else {
                    (
                        self.size(precision, a.scale()),
                        self.size(precision, b.scale()),
                        result_type,
                    )
                }
            }

            ArithmeticOp::Modulo => unreachable!(),
        }
    }

    // Align inside the arithmetic operation: aligned operands can exceed even
    // Decimal256 while the remainder fits. Preserve each operand's own scale
    // and use the larger input precision independently of the result precision.
    fn modulo_size(a: &DecimalSize, b: &DecimalSize) -> (DecimalSize, DecimalSize, DecimalSize) {
        let scale = a.scale().max(b.scale());
        let precision = a.precision().max(b.precision());
        let op = ArithmeticOp::Modulo;
        let result = op.size(a.leading_digits().min(b.leading_digits()) + scale, scale);
        let aligned = a.leading_digits().max(b.leading_digits()) + scale;
        if aligned <= i256::MAX_PRECISION {
            let operands = op.size(aligned, scale);
            (operands, operands, result)
        } else {
            (
                op.size(precision, a.scale()),
                op.size(precision, b.scale()),
                result,
            )
        }
    }
}

fn bind_function(
    op: ArithmeticOp,
    args_type: [DataType; 2],
    binding: BinaryBinding,
    domain: impl ScalarFunctionDomain,
    nullable: bool,
) -> Arc<Function> {
    let inputs = [
        InputPlan::prepare(&args_type[0], binding.left),
        InputPlan::prepare(&args_type[1], binding.right),
    ];
    let signature = FunctionSignature {
        name: op.name().to_owned(),
        args_type: args_type.to_vec(),
        return_type: DataType::Decimal(binding.result),
    };
    let bound = plan::bind(op, binding, inputs);
    let eval = match encoded::EncodedFastPath::new(op, &binding, inputs, bound) {
        Ok(fast) => Box::new(fast),
        Err(bound) => bound,
    };
    Arc::new(Function::with_passthrough_nullable(
        signature, domain, eval, None, nullable,
    ))
}

fn register_decimal_binary_op(registry: &mut FunctionRegistry, arithmetic_op: ArithmeticOp) {
    let name = arithmetic_op.name();

    let factory = FunctionFactory::Closure(Box::new(move |_, args_type| {
        if args_type.len() != 2 {
            return None;
        }

        let has_nullable = args_type.iter().any(|x| x.is_nullable_or_null());
        let args_type = [
            args_type[0].remove_nullable(),
            args_type[1].remove_nullable(),
        ];

        // number X decimal -> decimal
        // decimal X number -> decimal
        // decimal X decimal -> decimal
        if !args_type[0].is_decimal() && !args_type[1].is_decimal() {
            return None;
        }

        let decimal_a = args_type[0].get_decimal_properties()?;
        let decimal_b = args_type[1].get_decimal_properties()?;

        let op = arithmetic_op;
        let binding = BinaryBinding::new(op, &[decimal_a, decimal_b]);
        Some(match op {
            ArithmeticOp::Plus => bind_function(
                op,
                args_type,
                binding,
                domain::AddSubtractDomain::<false>::new(binding),
                has_nullable,
            ),
            ArithmeticOp::Minus => bind_function(
                op,
                args_type,
                binding,
                domain::AddSubtractDomain::<true>::new(binding),
                has_nullable,
            ),
            ArithmeticOp::Multiply => bind_function(
                op,
                args_type,
                binding,
                domain::MultiplyDomain::new(binding),
                has_nullable,
            ),
            ArithmeticOp::Divide | ArithmeticOp::Modulo => bind_function(
                op,
                args_type,
                binding,
                FunctionDomain::MayThrow,
                has_nullable,
            ),
        })
    }));

    registry.register_function_factory(name, factory);
}

pub fn register_decimal_minus(registry: &mut FunctionRegistry) {
    registry.register_function_factory(
        "minus",
        FunctionFactory::Closure(Box::new(|_params, args_type| {
            if args_type.len() != 1 {
                return None;
            }

            let is_nullable = args_type[0].is_nullable();
            let arg_type = args_type[0].remove_nullable();
            if !arg_type.is_decimal() {
                return None;
            }

            let signature = FunctionSignature {
                name: "minus".to_string(),
                args_type: vec![arg_type.clone()],
                return_type: arg_type.clone(),
            };

            Some(Arc::new(Function::with_passthrough_nullable(
                signature,
                minus_domain,
                unary_minus_decimal,
                None,
                is_nullable,
            )))
        })),
    );
}

fn minus_domain(_: &FunctionContext, d: &[Domain]) -> FunctionDomain<AnyType> {
    match &d[0] {
        Domain::Decimal(DecimalDomain::Decimal64(d, size)) => {
            FunctionDomain::Domain(Domain::Decimal(DecimalDomain::Decimal64(
                SimpleDomain {
                    min: -d.max,
                    max: d.min.checked_neg().unwrap_or(i64::DECIMAL_MAX),
                },
                *size,
            )))
        }
        Domain::Decimal(DecimalDomain::Decimal128(d, size)) => {
            FunctionDomain::Domain(Domain::Decimal(DecimalDomain::Decimal128(
                SimpleDomain {
                    min: -d.max,
                    max: d.min.checked_neg().unwrap_or(i128::DECIMAL_MAX),
                },
                *size,
            )))
        }
        Domain::Decimal(DecimalDomain::Decimal256(d, size)) => {
            FunctionDomain::Domain(Domain::Decimal(DecimalDomain::Decimal256(
                SimpleDomain {
                    min: -d.max,
                    max: d.min.checked_neg().unwrap_or(i256::DECIMAL_MAX),
                },
                *size,
            )))
        }
        _ => unreachable!(),
    }
}

fn unary_minus_decimal(args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    let arg = &args[0];
    let (decimal, _) = DecimalDataType::from_value(arg).unwrap();
    match decimal {
        DecimalDataType::Decimal64(size) => {
            let arg = arg.try_downcast().unwrap();
            type T = DecimalType<i64>;
            vectorize_1_arg::<T, T>(|t, _| DecimalExt::wrapping_neg(t))(arg, ctx)
                .upcast_with_type(&DataType::Decimal(size))
        }
        DecimalDataType::Decimal128(size) => {
            let arg = arg.try_downcast().unwrap();
            type T = DecimalType<i128>;
            vectorize_1_arg::<T, T>(|t, _| DecimalExt::wrapping_neg(t))(arg, ctx)
                .upcast_with_type(&DataType::Decimal(size))
        }
        DecimalDataType::Decimal256(size) => {
            let arg = arg.try_downcast().unwrap();
            type T = DecimalType<i256>;
            vectorize_1_arg::<T, T>(|t, _| DecimalExt::wrapping_neg(t))(arg, ctx)
                .upcast_with_type(&DataType::Decimal(size))
        }
    }
}

pub fn register_decimal_arithmetic(registry: &mut FunctionRegistry) {
    for op in ArithmeticOp::ALL {
        register_decimal_binary_op(registry, op);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_expression::Domain;
    use databend_common_expression::Function;
    use databend_common_expression::FunctionContext;
    use databend_common_expression::FunctionDomain;
    use databend_common_expression::FunctionEval;
    use databend_common_expression::FunctionRegistry;
    use databend_common_expression::Value;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::DecimalSize;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::*;
    use num_bigint::BigInt;
    use proptest::prelude::*;

    use super::ArithmeticOp;
    use super::BinaryBinding;
    use crate::testing::*;

    const OPS: [ArithmeticOp; 5] = ArithmeticOp::ALL;

    /// Fewer shapes for evaluation: scales {0, p/2, p}, and the narrowest and
    /// widest signed and unsigned integers.
    fn evaluated_types(precisions: impl IntoIterator<Item = u8>) -> Vec<DataType> {
        let mut types = vec![];
        for p in precisions {
            types.extend(scales(p, [0, p / 2, p]).map(|s| decimal_type(p, s)));
        }
        types.extend(
            [
                NumberDataType::Int8,
                NumberDataType::UInt8,
                NumberDataType::Int64,
                NumberDataType::UInt64,
            ]
            .map(DataType::Number),
        );
        types
    }

    fn registry() -> FunctionRegistry {
        let mut registry = FunctionRegistry::empty();
        super::super::register_decimal_arithmetic(&mut registry);
        registry
    }

    struct Bound<'a> {
        op: ArithmeticOp,
        types: [&'a DataType; 2],
        function: Arc<Function>,
        result: DecimalSize,
    }

    impl<'a> Bound<'a> {
        fn new(registry: &FunctionRegistry, op: ArithmeticOp, types: [&'a DataType; 2]) -> Self {
            let function = bind_function(registry, op.name(), vec![], &types.map(Clone::clone));
            let result = *function.signature.return_type.as_decimal().unwrap();
            Self {
                op,
                types,
                function,
                result,
            }
        }

        /// Exact result of one row: operands are scaled exactly, products and
        /// quotients round half away from zero, remainders truncate.
        fn expected(&self, a: &BigInt, b: &BigInt) -> Expected {
            let [sa, sb] = self
                .types
                .map(|ty| ty.get_decimal_properties().unwrap().scale() as u32);
            let scale = self.result.scale() as u32;
            let value = match self.op {
                ArithmeticOp::Plus => a * pow10(scale - sa) + b * pow10(scale - sb),
                ArithmeticOp::Minus => a * pow10(scale - sa) - b * pow10(scale - sb),
                ArithmeticOp::Multiply => round_half_away(&(a * b), &pow10(sa + sb - scale)),
                _ if b == &BigInt::from(0) => return None,
                ArithmeticOp::Divide => round_half_away(&(a * pow10(sb + scale - sa)), b),
                ArithmeticOp::Modulo => (a * pow10(scale - sa)) % (b * pow10(scale - sb)),
            };
            fits(&value, self.result.precision()).then_some(value)
        }

        /// Evaluates rows `a[i] op b[i]`. A scalar side broadcasts its first value;
        /// rows hidden by `visible` hold storage extremes instead of their values.
        fn check(&self, a: &[BigInt], b: &[BigInt], scalar: [bool; 2], visible: Option<&[bool]>) {
            let num_rows = a.len();
            let operand = |side: usize, values: &[BigInt]| -> (Value<AnyType>, Vec<BigInt>) {
                let ty = self.types[side];
                if scalar[side] {
                    let value = values[0].clone();
                    return (Value::Scalar(scalar_value(ty, &value)), vec![
                        value;
                        num_rows
                    ]);
                }
                let payloads = hidden_payloads(ty);
                let rows = (0..num_rows)
                    .map(|row| match visible {
                        Some(visible) if !visible[row] => payloads[(row + side) % 2].clone(),
                        _ => values[row].clone(),
                    })
                    .collect::<Vec<_>>();
                (Value::Column(column(ty, &rows)), rows)
            };
            let (left, a) = operand(0, a);
            let (right, b) = operand(1, b);
            let expected = a
                .iter()
                .zip(&b)
                .map(|(a, b)| self.expected(a, b))
                .collect::<Vec<_>>();
            check_rows(&self.function, &[left, right], visible, &expected, |row| {
                format!(
                    "{:?}({}, {}) -> {:?}, row {row}: {} {:?} {}, scalar={scalar:?}",
                    self.op, self.types[0], self.types[1], self.result, a[row], self.op, b[row]
                )
            });
        }
    }

    // Edge values of both operands, crossed, for every sampled signature: once
    // without validity, once with every third row hidden behind storage extremes.
    #[test]
    fn kernels_match_bigint_on_edges() {
        let registry = registry();
        let types = evaluated_types([1, 2, 18, 19, 38, 39, 76]);
        for [x, y] in signatures(&types) {
            let (xs, ys) = (edges(x), edges(y));
            let a = xs.iter().flat_map(|a| ys.iter().map(move |_| a.clone()));
            let b = xs.iter().flat_map(|_| ys.iter().cloned());
            let (a, b) = (a.collect::<Vec<_>>(), b.collect::<Vec<_>>());
            let visible = (0..a.len()).map(|row| row % 3 != 0).collect::<Vec<_>>();
            for op in OPS {
                let bound = Bound::new(&registry, op, [x, y]);
                bound.check(&a, &b, [false; 2], None);
                bound.check(&a, &b, [false; 2], Some(&visible));
            }
        }
    }

    fn operand_type() -> impl Strategy<Value = DataType> {
        prop_oneof![
            4 => (1_u8..=76, any::<u8>()).prop_map(|(p, s)| decimal_type(p, s % (p + 1))),
            1 => (0..INTEGERS.len()).prop_map(|i| DataType::Number(INTEGERS[i])),
        ]
    }

    /// A value of `ty` drawn from `seed`: uniform over the whole range, small, or
    /// an edge value.
    fn sample(ty: &DataType, seed: &[u8; 32]) -> BigInt {
        let edges = edges(ty);
        let (min, max) = (edges[0].clone(), edges[edges.len() - 1].clone());
        let random = BigInt::from_signed_bytes_le(&seed[2..]);
        match seed[0] % 3 {
            0 => &min + (random % (&max - &min + 1u8) + (&max - &min + 1u8)) % (&max - &min + 1u8),
            1 => BigInt::from(seed[1] as i8).clamp(min, max),
            _ => edges[seed[1] as usize % edges.len()].clone(),
        }
    }

    proptest! {
        #[test]
        fn kernels_match_bigint(
            op in 0..OPS.len(),
            x in operand_type(),
            y in operand_type(),
            seeds in any::<[[u8; 32]; 8]>(),
            visible in any::<[bool; 4]>(),
            scalar in any::<[bool; 2]>(),
            masked in any::<bool>(),
        ) {
            prop_assume!(x.is_decimal() || y.is_decimal());
            let bound = Bound::new(&registry(), OPS[op], [&x, &y]);
            let a = seeds[..4].iter().map(|seed| sample(&x, seed)).collect::<Vec<_>>();
            let b = seeds[4..].iter().map(|seed| sample(&y, seed)).collect::<Vec<_>>();
            bound.check(&a, &b, scalar, masked.then_some(&visible[..]));
        }
    }

    // Drivers decide which rows a row kernel sees: `SKIP` kernels see only visible
    // rows, and none at all for an invisible or empty batch; the others see every
    // row once. A scalar pair is one evaluation.
    #[test]
    fn drivers_call_kernels_on_selected_rows() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        use super::InputPlan;
        use super::kernel;
        use super::kernel::DecimalArgs;

        let size = DecimalSize::new(18, 0).unwrap();
        let ty = DataType::Decimal(size);
        let plans = [InputPlan::prepare(&ty, size); 2];
        let func_ctx = FunctionContext::default();
        let operand = |is_scalar: bool, value: i64, rows: Vec<i64>| {
            if is_scalar {
                Value::Scalar(scalar_value(&ty, &BigInt::from(value)))
            } else {
                Value::Column(column(
                    &ty,
                    &rows.into_iter().map(BigInt::from).collect::<Vec<_>>(),
                ))
            }
        };
        let run = |skip: bool, a: &Value<AnyType>, b: &Value<AnyType>, validity: &[bool]| {
            let calls = AtomicUsize::new(0);
            let visible = validity.iter().filter(|v| **v).count() > 0;
            let row = |a: i64, b: i64| {
                calls.fetch_add(1, Ordering::Relaxed);
                assert!(
                    !skip || (visible && (a, b) == (3, 2)),
                    "kernel saw a hidden row"
                );
                a.wrapping_mul(b)
            };
            for fallible in [false, true] {
                let mut ctx = context(
                    &func_ctx,
                    validity.len(),
                    Some(validity.iter().copied().collect()),
                );
                let args = DecimalArgs::<i64, i64>::new([a, b], &plans);
                match (skip, fallible) {
                    (true, false) => drop(kernel::map::<true, i64, i64, i64, i64, _>(
                        args, &mut ctx, size, row,
                    )),
                    (false, false) => drop(kernel::map::<false, i64, i64, i64, i64, _>(
                        args, &mut ctx, size, row,
                    )),
                    (true, true) => drop(kernel::checked::<true, i64, i64, i64, i64, _>(
                        args,
                        &mut ctx,
                        size,
                        |a, b| Ok(row(a, b)),
                        1,
                    )),
                    (false, true) => drop(kernel::checked::<false, i64, i64, i64, i64, _>(
                        args,
                        &mut ctx,
                        size,
                        |a, b| Ok(row(a, b)),
                        1,
                    )),
                }
                assert!(ctx.errors.is_none());
            }
            calls.load(Ordering::Relaxed) / 2
        };
        for scalar in [[false, false], [false, true], [true, false], [true, true]] {
            let a = operand(scalar[0], 3, vec![i64::MAX, 3, i64::MIN, 3]);
            let b = operand(scalar[1], 2, vec![i64::MIN, 2, i64::MAX, 2]);
            let pair = scalar == [true, true];
            let partial = [false, true, false, true];
            assert_eq!(
                run(true, &a, &b, &partial),
                if pair { 1 } else { 2 },
                "{scalar:?}"
            );
            assert_eq!(
                run(false, &a, &b, &partial),
                if pair { 1 } else { 4 },
                "{scalar:?}"
            );
            assert_eq!(run(true, &a, &b, &[false; 4]), 0, "{scalar:?}");
        }
        let empty = Value::Column(column(&ty, &[]));
        assert_eq!(run(true, &empty, &empty, &[]), 0);
    }

    fn decimal_bounds(domain: &DecimalDomain) -> (BigInt, BigInt, DecimalSize) {
        match domain {
            DecimalDomain::Decimal64(d, size) => (bigint(d.min), bigint(d.max), *size),
            DecimalDomain::Decimal128(d, size) => (bigint(d.min), bigint(d.max), *size),
            DecimalDomain::Decimal256(d, size) => (bigint(d.min), bigint(d.max), *size),
        }
    }

    /// Ranges built from the type's minimum, -1, 0, 1 and maximum.
    fn intervals(ty: &DataType) -> Vec<[BigInt; 2]> {
        let edges = edges(ty);
        let (min, max) = (edges[0].clone(), edges[edges.len() - 1].clone());
        let points = [
            min.clone(),
            BigInt::from(-1),
            BigInt::from(0),
            BigInt::from(1),
            max.clone(),
        ]
        .into_iter()
        .filter(|v| &min <= v && v <= &max)
        .collect::<Vec<_>>();
        points
            .iter()
            .flat_map(|lo| {
                points
                    .iter()
                    .filter(move |hi| lo <= *hi)
                    .map(move |hi| [lo.clone(), hi.clone()])
            })
            .collect()
    }

    // Add, subtract and multiply are monotone in each operand, so a domain is the
    // exact range of its corner results. A domain may stay conservative (MayThrow)
    // only if a corner fails or an endpoint does not fit the size the domain
    // computes in: the result size for add/sub, the bound operand sizes for
    // multiply.
    #[test]
    fn domains_are_exact_or_conservative() {
        let registry = registry();
        let func_ctx = FunctionContext::default();
        let types = operand_types([1, 18, 19, 38, 39, 76]);
        for [x, y] in signatures(&types) {
            let original = [x, y].map(|ty| ty.get_decimal_properties().unwrap());
            for op in [
                ArithmeticOp::Plus,
                ArithmeticOp::Minus,
                ArithmeticOp::Multiply,
            ] {
                let bound = &Bound::new(&registry, op, [x, y]);
                let FunctionEval::Scalar { calc_domain, .. } = &bound.function.eval else {
                    unreachable!("scalar function")
                };
                let binding = BinaryBinding::new(op, &original);
                let sizes = match op {
                    ArithmeticOp::Multiply => [binding.left, binding.right],
                    _ => [binding.result; 2],
                };
                let fits_size = |side: usize, value: &BigInt| {
                    let shift = sizes[side].scale() - original[side].scale();
                    fits(&(value * pow10(shift as u32)), sizes[side].precision())
                };
                for a in intervals(x) {
                    for b in intervals(y) {
                        let domains = [column(x, &a).domain(), column(y, &b).domain()];
                        let corners = a
                            .iter()
                            .flat_map(|a| b.iter().map(move |b| bound.expected(a, b)))
                            .collect::<Option<Vec<_>>>();
                        let context = format!("{op:?}({x}, {y}): {a:?} {b:?}");
                        match (calc_domain.domain_eval(&func_ctx, &domains), corners) {
                            (FunctionDomain::Domain(Domain::Decimal(domain)), Some(corners)) => {
                                let range = (
                                    corners.iter().min().unwrap().clone(),
                                    corners.iter().max().unwrap().clone(),
                                    bound.result,
                                );
                                assert_eq!(decimal_bounds(&domain), range, "{context}");
                            }
                            (FunctionDomain::MayThrow, None) => {}
                            (FunctionDomain::MayThrow, Some(_)) => assert!(
                                !a.iter().all(|v| fits_size(0, v))
                                    || !b.iter().all(|v| fits_size(1, v)),
                                "needlessly conservative: {context}"
                            ),
                            (domain, corners) => {
                                panic!("{context}: domain {domain:?}, corners {corners:?}")
                            }
                        }
                    }
                }
            }
        }
    }
}
