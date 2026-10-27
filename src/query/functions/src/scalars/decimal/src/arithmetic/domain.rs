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

//! Conservative domain calculation is independent of row execution plans.
//! Protected constants are prepared only when bounds are actually requested.

use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::ScalarFunctionDomain;
use databend_common_expression::Value;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::SimpleDomain;
use databend_common_expression::types::decimal::*;
use num_bigint::BigInt;

use super::plan::BinaryBinding;
use super::row;
use super::row::Bounds;
use crate::cast::typed;

// Both endpoints are aligned to the result scale before this is called.
pub(super) fn domain_plus(
    lhs: &SimpleDomain<i256>,
    rhs: &SimpleDomain<i256>,
    precision: u8,
) -> Option<SimpleDomain<i256>> {
    let bounds = Bounds::<i256>::new(precision);
    Some(SimpleDomain {
        min: lhs.min.checked_add(rhs.min).and_then(|v| bounds.fit(v))?,
        max: lhs.max.checked_add(rhs.max).and_then(|v| bounds.fit(v))?,
    })
}

// Both endpoints are aligned to the result scale before this is called.
pub(super) fn domain_minus(
    lhs: &SimpleDomain<i256>,
    rhs: &SimpleDomain<i256>,
    precision: u8,
) -> Option<SimpleDomain<i256>> {
    let bounds = Bounds::<i256>::new(precision);
    Some(SimpleDomain {
        min: lhs.min.checked_sub(rhs.max).and_then(|v| bounds.fit(v))?,
        max: lhs.max.checked_sub(rhs.min).and_then(|v| bounds.fit(v))?,
    })
}

pub(super) struct AddSubtractDomain<const SUBTRACT: bool> {
    binding: BinaryBinding,
}

impl<const SUBTRACT: bool> AddSubtractDomain<SUBTRACT> {
    pub(super) fn new(binding: BinaryBinding) -> Self {
        Self { binding }
    }
}

impl<const SUBTRACT: bool> ScalarFunctionDomain for AddSubtractDomain<SUBTRACT> {
    fn domain_eval(&self, ctx: &FunctionContext, domains: &[Domain]) -> FunctionDomain<AnyType> {
        let size = self.binding.result;
        calculate(ctx, domains, [size; 2], size, |a, b| {
            if SUBTRACT {
                domain_minus(a, b, size.precision())
            } else {
                domain_plus(a, b, size.precision())
            }
        })
    }
}

pub(super) struct MultiplyDomain {
    binding: BinaryBinding,
    shift: u32,
}

impl MultiplyDomain {
    pub(super) fn new(binding: BinaryBinding) -> Self {
        let [a, b] = binding.original;
        let shift = (a.scale() + b.scale() - binding.result.scale()) as u32;
        Self { binding, shift }
    }

    pub(super) fn domain(
        &self,
        lhs: &SimpleDomain<i256>,
        rhs: &SimpleDomain<i256>,
    ) -> Option<SimpleDomain<i256>> {
        let divisor = BigInt::from(10).pow(self.shift);
        let half = &divisor / 2_u8;
        let limit = BigInt::from(10).pow(self.binding.result.precision() as u32) - 1_u8;
        let min = -&limit;
        let multiply = |a, b| {
            let product = row::to_bigint(a) * row::to_bigint(b);
            let negative = product < BigInt::from(0);
            let numerator = if negative {
                product - &half
            } else {
                product + &half
            };
            let result = numerator / &divisor;
            if result < min || result > limit {
                None
            } else {
                i256::from_bigint(result)
            }
        };
        let a = multiply(lhs.min, rhs.min)?;
        let b = multiply(lhs.min, rhs.max)?;
        let c = multiply(lhs.max, rhs.min)?;
        let d = multiply(lhs.max, rhs.max)?;
        Some(SimpleDomain {
            min: a.min(b).min(c).min(d),
            max: a.max(b).max(c).max(d),
        })
    }
}

impl ScalarFunctionDomain for MultiplyDomain {
    fn domain_eval(&self, ctx: &FunctionContext, domains: &[Domain]) -> FunctionDomain<AnyType> {
        calculate(
            ctx,
            domains,
            [self.binding.left, self.binding.right],
            self.binding.result,
            |a, b| self.domain(a, b),
        )
    }
}

fn calculate(
    ctx: &FunctionContext,
    domains: &[Domain],
    inputs: [DecimalSize; 2],
    result: DecimalSize,
    calculate_bounds: impl FnOnce(
        &SimpleDomain<i256>,
        &SimpleDomain<i256>,
    ) -> Option<SimpleDomain<i256>>,
) -> FunctionDomain<AnyType> {
    let (Some(a), Some(b)) = (
        cast_bounds(&domains[0], inputs[0], ctx),
        cast_bounds(&domains[1], inputs[1], ctx),
    ) else {
        return FunctionDomain::MayThrow;
    };
    let Some(bounds) = calculate_bounds(&a, &b) else {
        return FunctionDomain::MayThrow;
    };
    let output = match result.data_kind() {
        DecimalDataKind::Decimal64 => DecimalDomain::Decimal64(
            SimpleDomain {
                min: bounds.min.as_i64(),
                max: bounds.max.as_i64(),
            },
            result,
        ),
        DecimalDataKind::Decimal128 => DecimalDomain::Decimal128(
            SimpleDomain {
                min: bounds.min.as_i128(),
                max: bounds.max.as_i128(),
            },
            result,
        ),
        DecimalDataKind::Decimal256 => DecimalDomain::Decimal256(bounds, result),
    };
    FunctionDomain::Domain(Domain::Decimal(output))
}

// Evaluate exactly two scalar endpoints through the shared checked converters,
// without manufacturing a two-row column or capturing an execution plan.
fn cast_bounds(
    domain: &Domain,
    size: DecimalSize,
    func_ctx: &FunctionContext,
) -> Option<SimpleDomain<i256>> {
    if !matches!(domain, Domain::Decimal(_) | Domain::Number(_)) {
        return None;
    }
    let (min, max) = domain.to_minmax();
    let from_type = min.as_ref().infer_data_type();
    let convert = |scalar, ctx: &mut EvalContext| {
        let value = Value::Scalar(scalar);
        match &from_type {
            DataType::Decimal(from) => typed::decimal256((*from).into(), size, &value, ctx),
            DataType::Number(ty) => typed::number::<i256>(&value, *ty, size, ctx),
            _ => unreachable!("numeric domain"),
        }
    };
    let mut ctx = EvalContext {
        generics: &[],
        num_rows: 1,
        func_ctx,
        validity: None,
        errors: None,
        suppress_error: false,
        strict_eval: false,
    };
    let (Value::Scalar(min), Value::Scalar(max)) = (convert(min, &mut ctx), convert(max, &mut ctx))
    else {
        unreachable!("scalar endpoint cast");
    };
    if ctx.errors.is_some() {
        None
    } else {
        Some(SimpleDomain { min, max })
    }
}
