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

use std::fmt::Debug;
use std::ops::BitAnd;
use std::ops::BitOr;
use std::ops::Not;
use std::sync::Arc;

use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;

use super::EvalContext;
use super::Function;
use super::FunctionContext;
use super::FunctionEval;
use super::FunctionFactory;
use super::FunctionFactoryClosure;
use super::ScalarFunction;
use super::ScalarFunctionDomain;
use crate::Column;
use crate::Domain;
use crate::FunctionDomain;
use crate::FunctionSignature;
use crate::Scalar;
use crate::Value;
use crate::function_stat::DeriveStat;
use crate::function_stat::ScalarFunctionStat;
use crate::types::AnyType;
use crate::types::DataType;
use crate::types::NullableColumn;
use crate::types::NullableType;
use crate::types::ValueType;
use crate::types::nullable::NullableDomain;

impl<F> ScalarFunctionDomain for F
where F: Fn(&FunctionContext, &[Domain]) -> FunctionDomain<AnyType> + Send + Sync + 'static
{
    fn domain_eval(&self, ctx: &FunctionContext, domains: &[Domain]) -> FunctionDomain<AnyType> {
        (self)(ctx, domains)
    }
}

impl ScalarFunctionDomain for Box<dyn ScalarFunctionDomain> {
    fn domain_eval(&self, ctx: &FunctionContext, domains: &[Domain]) -> FunctionDomain<AnyType> {
        (**self).domain_eval(ctx, domains)
    }
}

impl<F> ScalarFunction for F
where F: Fn(&[Value<AnyType>], &mut EvalContext) -> Value<AnyType> + Send + Sync + 'static
{
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        (self)(args, ctx)
    }
}

impl ScalarFunction for Box<dyn ScalarFunction> {
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        (**self).eval(args, ctx)
    }
}

impl ScalarFunctionDomain for FunctionDomain<AnyType> {
    fn domain_eval(&self, ctx: &FunctionContext, domains: &[Domain]) -> FunctionDomain<AnyType> {
        self.clone()
    }
}

pub fn domain_evaluator<F>(f: F) -> Box<dyn ScalarFunctionDomain>
where F: Fn(&FunctionContext, &[Domain]) -> FunctionDomain<AnyType> + Send + Sync + 'static {
    Box::new(f)
}

pub fn scalar_evaluator<F>(f: F) -> Box<dyn ScalarFunction>
where F: Fn(&[Value<AnyType>], &mut EvalContext) -> Value<AnyType> + Send + Sync + 'static {
    Box::new(f)
}

impl Function {
    pub fn passthrough_nullable(self) -> Self {
        let Function { signature, eval } = self;
        let (calc_domain, derive_stat, eval) = eval.into_scalar().unwrap();
        Function::with_passthrough_nullable(signature, calc_domain, eval, derive_stat, true)
    }

    pub fn with_passthrough_nullable(
        signature: FunctionSignature,
        calc_domain: impl ScalarFunctionDomain,
        eval: impl ScalarFunction,
        derive_stat: Option<Box<dyn ScalarFunctionStat>>,
        is_nullable: bool,
    ) -> Self {
        if !is_nullable {
            return Self {
                signature,
                eval: FunctionEval::Scalar {
                    calc_domain: Box::new(calc_domain),
                    eval: Box::new(eval),
                    derive_stat,
                },
            };
        }

        debug_assert!(
            signature
                .args_type
                .iter()
                .all(|ty| !ty.is_nullable_or_null())
        );

        let signature = FunctionSignature {
            name: signature.name.clone(),
            args_type: signature
                .args_type
                .iter()
                .map(|ty| ty.wrap_nullable())
                .collect(),
            return_type: signature.return_type.wrap_nullable(),
        };

        Function {
            signature,
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(PassthroughNullableDomain(calc_domain)),
                eval: Box::new(PassthroughNullable(eval)),
                derive_stat,
            },
        }
    }

    pub fn error_to_null(self) -> Self {
        debug_assert!(!self.signature.return_type.is_nullable_or_null());

        let mut signature = self.signature;
        let return_type = signature.return_type.wrap_nullable();
        signature.return_type = return_type.clone();

        let (calc_domain, derive_stat, eval) = self.eval.into_scalar().unwrap();

        let new_calc_domain = Box::new(move |ctx: &FunctionContext, domains: &[Domain]| {
            let domain = calc_domain.domain_eval(ctx, domains);
            match domain {
                FunctionDomain::Domain(domain) => {
                    let new_domain = NullableDomain {
                        has_null: false,
                        value: Some(Box::new(domain)),
                    };
                    FunctionDomain::Domain(NullableType::<AnyType>::upcast_domain_with_type(
                        new_domain,
                        &return_type,
                    ))
                }
                FunctionDomain::Full | FunctionDomain::MayThrow => FunctionDomain::Full,
            }
        });
        let new_eval = Box::new(move |val: &[Value<AnyType>], ctx: &mut EvalContext| {
            let num_rows = ctx.num_rows;
            let output = eval.eval(val, ctx);
            if let Some((validity, _)) = ctx.errors.take() {
                match output {
                    Value::Scalar(_) => Value::Scalar(Scalar::Null),
                    Value::Column(column) => {
                        Value::Column(NullableColumn::new_column(column, validity.into()))
                    }
                }
            } else {
                match output {
                    Value::Scalar(scalar) => Value::Scalar(scalar),
                    Value::Column(column) => Value::Column(NullableColumn::new_column(
                        column,
                        Bitmap::new_constant(true, num_rows),
                    )),
                }
            }
        });

        Function {
            signature,
            eval: FunctionEval::Scalar {
                calc_domain: new_calc_domain,
                eval: new_eval,
                derive_stat,
            },
        }
    }
}

pub struct PassthroughNullableDomain<T>(pub T);

impl<T> ScalarFunctionDomain for PassthroughNullableDomain<T>
where T: ScalarFunctionDomain
{
    fn domain_eval(&self, ctx: &FunctionContext, domains: &[Domain]) -> FunctionDomain<AnyType> {
        let mut args_has_null = false;
        let mut args_domain = Vec::with_capacity(domains.len());
        for domain in domains {
            match domain {
                Domain::Nullable(NullableDomain {
                    has_null,
                    value: Some(value),
                }) => {
                    args_has_null = args_has_null || *has_null;
                    args_domain.push(value.as_ref().clone());
                }
                Domain::Nullable(NullableDomain { value: None, .. }) => {
                    return FunctionDomain::Domain(Domain::Nullable(NullableDomain {
                        has_null: true,
                        value: None,
                    }));
                }
                _ => unreachable!(),
            }
        }

        self.0.domain_eval(ctx, &args_domain).map(|result_domain| {
            let (result_has_null, result_domain) = match result_domain {
                Domain::Nullable(NullableDomain { has_null, value }) => (has_null, value),
                domain => (false, Some(Box::new(domain))),
            };
            Domain::Nullable(NullableDomain {
                has_null: args_has_null || result_has_null,
                value: result_domain,
            })
        })
    }
}

pub struct PassthroughNullable<T>(pub T);

impl<T> ScalarFunction for PassthroughNullable<T>
where T: ScalarFunction
{
    fn eval<'a>(&self, args: &[Value<AnyType>], ctx: &mut EvalContext<'a>) -> Value<AnyType> {
        type T = NullableType<AnyType>;
        type Result = AnyType;

        let mut bitmap: Option<MutableBitmap> = None;
        let mut nonull_args: Vec<Value<Result>> = Vec::with_capacity(args.len());

        let mut len = 1;
        for arg in args {
            let arg = arg.try_downcast::<T>().unwrap();
            match arg {
                Value::Scalar(None) => return Value::Scalar(Scalar::Null),
                Value::Scalar(Some(s)) => {
                    nonull_args.push(Value::Scalar(s.clone()));
                }
                Value::Column(v) => {
                    len = v.len();
                    nonull_args.push(Value::Column(v.column.clone()));
                    bitmap = match bitmap {
                        Some(m) => Some(m.bitand(&v.validity)),
                        None => Some(v.validity.clone().make_mut()),
                    };
                }
            }
        }

        let bitmap = match bitmap {
            Some(bitmap) => bitmap.freeze(),
            None => Bitmap::new_trued(len),
        };
        if bitmap.null_count() > 0 {
            ctx.validity = ctx.validity.as_ref().map(|validity| validity & &bitmap);
        }
        let results = self.0.eval(&nonull_args, ctx);
        if let Some((error_bitmap, _)) = ctx.errors.as_mut() {
            // If the original value is NULL, we can ignore the error.
            let res = error_bitmap.clone().bitor(&bitmap.not());
            if res.null_count() == 0 {
                ctx.errors = None;
            } else {
                *error_bitmap = res;
            }
        }

        match results {
            Value::Scalar(s) => {
                if bitmap.get(0).unwrap() {
                    Value::Scalar(s)
                } else {
                    Value::Scalar(Scalar::Null)
                }
            }
            Value::Column(column) => {
                let result = match column {
                    Column::Nullable(box nullable_column) => NullableColumn::new_column(
                        nullable_column.column,
                        nullable_column.validity.bitand(&bitmap),
                    ),
                    _ => NullableColumn::new_column(column, bitmap),
                };
                Value::Column(result)
            }
        }
    }
}

pub struct ImplByEvaluator(pub &'static str);

impl ScalarFunction for ImplByEvaluator {
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        unreachable!("`{}` should be handled by the `Evaluator`", self.0)
    }
}

pub struct FunctionFactoryHelper {
    fixed_arg_count: Option<usize>,
    passthrough_nullable: bool,
    pub(super) create: Box<dyn FunctionFactoryClosure>,
}

impl Debug for FunctionFactoryHelper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionFactoryHelper")
            .field("fixed_arg_count", &self.fixed_arg_count)
            .field("passthrough_nullable", &self.passthrough_nullable)
            .finish()
    }
}

impl FunctionFactoryHelper {
    pub fn create_1_arg_passthrough_nullable(
        create: fn(&[Scalar], &DataType) -> Option<Function>,
    ) -> FunctionFactory {
        FunctionFactory::Helper(Self {
            fixed_arg_count: Some(1),
            passthrough_nullable: true,
            create: Box::new(move |params, args: &[DataType]| match args {
                [DataType::Nullable(box arg0)] => {
                    create(params, arg0).map(|func| Arc::new(func.passthrough_nullable()))
                }
                [arg0] => create(params, arg0).map(Arc::new),
                _ => None,
            }),
        })
    }
}
