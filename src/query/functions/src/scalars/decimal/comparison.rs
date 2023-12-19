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

use std::cmp::Ord;
use std::ops::*;
use std::sync::Arc;

use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::SimpleDomainCmp;
use databend_common_expression::Value;
use databend_common_expression::ValueRef;
use ethnum::i256;

macro_rules! register_decimal_compare_op {
    ($registry: expr, $name: expr, $op: ident, $domain_op: tt) => {
        $registry.register_function_factory($name, |_, args_type| {
            if args_type.len() != 2 {
                return None;
            }

            let has_nullable = args_type.iter().any(|x| x.is_nullable_or_null());
            let args_type: Vec<DataType> = args_type.iter().map(|x| x.remove_nullable()).collect();

            // Only works for one of is decimal types
            if !args_type[0].is_decimal() && !args_type[1].is_decimal() {
                return None;
            }

            let common_type = common_super_type(args_type[0].clone(), args_type[1].clone(), &[])?;

            if !common_type.is_decimal() {
                return None;
            }

            // Comparison between different decimal types must be same siganature types
            let function = Function {
                signature: FunctionSignature {
                    name: $name.to_string(),
                    args_type: vec![common_type.clone(), common_type.clone()],
                    return_type: DataType::Boolean,
                },
                eval: FunctionEval::Scalar {
                    calc_domain: Box::new(|_, d| {
                        let new_domain = match (&d[0], &d[1]) {
                            (
                                Domain::Decimal(DecimalDomain::Decimal128(d1, _)),
                                Domain::Decimal(DecimalDomain::Decimal128(d2, _)),
                            ) => d1.$domain_op(d2),
                            (
                                Domain::Decimal(DecimalDomain::Decimal256(d1, _)),
                                Domain::Decimal(DecimalDomain::Decimal256(d2, _)),
                            ) => d1.$domain_op(d2),
                            _ => unreachable!("Expect two same decimal domains, got {:?}", d),
                        };
                        new_domain.map(|d| Domain::Boolean(d))
                    }),
                    eval: Box::new(move |args, ctx| {
                        op_decimal! { &args[0], &args[1], common_type, $op, ctx}
                    }),
                },
            };
            if has_nullable {
                Some(Arc::new(function.passthrough_nullable()))
            } else {
                Some(Arc::new(function))
            }
        });
    };
}

macro_rules! op_decimal {
    ($a: expr, $b: expr,  $common_type: expr, $op: ident, $ctx: expr) => {
        match $common_type {
            DataType::Decimal(DecimalDataType::Decimal128(_)) => {
                let f = |a: i128, b: i128, _: &mut EvalContext| -> bool { a.cmp(&b).$op() };
                compare_decimal($a, $b, f, $ctx)
            }
            DataType::Decimal(DecimalDataType::Decimal256(_)) => {
                let f = |a: i256, b: i256, _: &mut EvalContext| -> bool { a.cmp(&b).$op() };
                compare_decimal($a, $b, f, $ctx)
            }
            _ => unreachable!(),
        }
    };
}

fn compare_decimal<T, F>(
    a: &ValueRef<AnyType>,
    b: &ValueRef<AnyType>,
    f: F,
    ctx: &mut EvalContext,
) -> Value<AnyType>
where
    T: Decimal,
    F: Fn(T, T, &mut EvalContext) -> bool + Copy + Send + Sync,
{
    let a = a.try_downcast().unwrap();
    let b = b.try_downcast().unwrap();
    let value = vectorize_2_arg::<DecimalType<T>, DecimalType<T>, BooleanType>(f)(a, b, ctx);
    value.upcast()
}

pub fn register_decimal_compare_op(registry: &mut FunctionRegistry) {
    register_decimal_compare_op!(registry, "lt", is_lt, domain_lt);
    register_decimal_compare_op!(registry, "eq", is_eq, domain_eq);
    register_decimal_compare_op!(registry, "gt", is_gt, domain_gt);
    register_decimal_compare_op!(registry, "lte", is_le, domain_lte);
    register_decimal_compare_op!(registry, "gte", is_ge, domain_gte);
    register_decimal_compare_op!(registry, "noteq", is_ne, domain_noteq);
}
