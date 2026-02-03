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

use std::marker::PhantomData;

use super::register_vectorize::*;
use crate::EvalContext;
use crate::Function;
use crate::FunctionContext;
use crate::FunctionDomain;
use crate::FunctionEval;
use crate::FunctionRegistry;
use crate::FunctionSignature;
use crate::ScalarFunction;
use crate::ScalarFunctionDomain;
use crate::property::Domain;
use crate::types::nullable::NullableDomain;
use crate::types::*;
use crate::values::Value;

impl FunctionRegistry {
    pub fn register_1_arg<I1: ArgType, O: ArgType, G>(
        &mut self,
        name: &str,
        calc_domain: fn(&FunctionContext, &I1::Domain) -> FunctionDomain<O>,
        func: G,
    ) where
        G: Fn(I1::ScalarRef<'_>, &mut EvalContext) -> O::Scalar
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
    {
        self.scalar_builder(name)
            .function()
            .typed_1_arg::<I1, O>()
            .passthrough_nullable()
            .calc_domain(calc_domain)
            .each_row(func)
            .register();
    }

    pub fn register_2_arg<I1: ArgType, I2: ArgType, O: ArgType, G>(
        &mut self,
        name: &str,
        calc_domain: fn(&FunctionContext, &I1::Domain, &I2::Domain) -> FunctionDomain<O>,
        func: G,
    ) where
        G: Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>, &mut EvalContext) -> O::Scalar
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
    {
        self.scalar_builder(name)
            .function()
            .typed_2_arg::<I1, I2, O>()
            .passthrough_nullable()
            .calc_domain(calc_domain)
            .each_row(func)
            .register();
    }

    pub fn register_3_arg<I1: ArgType, I2: ArgType, I3: ArgType, O: ArgType, F, G>(
        &mut self,
        name: &str,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(&FunctionContext, &I1::Domain, &I2::Domain, &I3::Domain) -> FunctionDomain<O>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
        G: Fn(
                I1::ScalarRef<'_>,
                I2::ScalarRef<'_>,
                I3::ScalarRef<'_>,
                &mut EvalContext,
            ) -> O::Scalar
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
    {
        self.register_passthrough_nullable_3_arg::<I1, I2, I3, O, _, _>(
            name,
            calc_domain,
            vectorize_3_arg(func),
        )
    }

    pub fn register_4_arg<I1: ArgType, I2: ArgType, I3: ArgType, I4: ArgType, O: ArgType, F, G>(
        &mut self,
        name: &str,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(
                &FunctionContext,
                &I1::Domain,
                &I2::Domain,
                &I3::Domain,
                &I4::Domain,
            ) -> FunctionDomain<O>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
        G: Fn(
                I1::ScalarRef<'_>,
                I2::ScalarRef<'_>,
                I3::ScalarRef<'_>,
                I4::ScalarRef<'_>,
                &mut EvalContext,
            ) -> O::Scalar
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
    {
        self.register_passthrough_nullable_4_arg::<I1, I2, I3, I4, O, _, _>(
            name,
            calc_domain,
            vectorize_4_arg(func),
        )
    }

    pub fn register_passthrough_nullable_1_arg<I1: ArgType, O: ArgType, G>(
        &mut self,
        name: &str,
        calc_domain: fn(&FunctionContext, &I1::Domain) -> FunctionDomain<O>,
        func: G,
    ) where
        G: Fn(Value<I1>, &mut EvalContext) -> Value<O> + 'static + Clone + Copy + Send + Sync,
    {
        self.scalar_builder(name)
            .function()
            .typed_1_arg()
            .passthrough_nullable()
            .calc_domain(calc_domain)
            .vectorized(func)
            .register();
    }

    pub fn register_passthrough_nullable_2_arg<I1: ArgType, I2: ArgType, O: ArgType, F, G>(
        &mut self,
        name: &str,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(&FunctionContext, &I1::Domain, &I2::Domain) -> FunctionDomain<O>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
        G: Fn(Value<I1>, Value<I2>, &mut EvalContext) -> Value<O>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
    {
        let has_nullable = &[I1::data_type(), I2::data_type(), O::data_type()]
            .iter()
            .any(|ty| ty.as_nullable().is_some() || ty.is_null());

        assert!(
            !has_nullable,
            "Function {} has nullable argument or output, please use register_2_arg_core instead",
            name
        );

        self.register_2_arg_core::<I1, I2, O, _, _>(name, calc_domain, func);

        self.register_2_arg_core::<NullableType<I1>, NullableType<I2>, NullableType<O>, _, _>(
            name,
            move |ctx, arg1, arg2| match (&arg1.value, &arg2.value) {
                (Some(value1), Some(value2)) => {
                    if let Some(domain) = calc_domain(ctx, value1, value2).normalize() {
                        FunctionDomain::Domain(NullableDomain {
                            has_null: arg1.has_null || arg2.has_null,
                            value: Some(Box::new(domain)),
                        })
                    } else {
                        FunctionDomain::MayThrow
                    }
                }
                _ => FunctionDomain::Domain(NullableDomain {
                    has_null: true,
                    value: None,
                }),
            },
            passthrough_nullable_2_arg(func),
        );
    }

    pub fn register_passthrough_nullable_3_arg<
        I1: ArgType,
        I2: ArgType,
        I3: ArgType,
        O: ArgType,
        F,
        G,
    >(
        &mut self,
        name: &str,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(&FunctionContext, &I1::Domain, &I2::Domain, &I3::Domain) -> FunctionDomain<O>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
        G: Fn(Value<I1>, Value<I2>, Value<I3>, &mut EvalContext) -> Value<O>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
    {
        let has_nullable = &[
            I1::data_type(),
            I2::data_type(),
            I3::data_type(),
            O::data_type(),
        ]
        .iter()
        .any(|ty| ty.as_nullable().is_some() || ty.is_null());

        assert!(
            !has_nullable,
            "Function {} has nullable argument or output, please use register_3_arg_core instead",
            name
        );

        self.register_3_arg_core::<I1, I2, I3, O, _, _>(name, calc_domain, func);

        self.register_3_arg_core::<NullableType<I1>, NullableType<I2>, NullableType<I3>,  NullableType<O>, _, _>(
                        name,
                        move |ctx, arg1,arg2,arg3,| {
                            match (&arg1.value,&arg2.value,&arg3.value) {
                                (Some(value1),Some(value2),Some(value3)) => {
                                    if let Some(domain) = calc_domain(ctx, value1,value2,value3,).normalize() {
                                        FunctionDomain::Domain(NullableDomain {
                                            has_null: arg1.has_null||arg2.has_null||arg3.has_null,
                                            value: Some(Box::new(domain)),
                                        })
                                    } else {
                                        FunctionDomain::MayThrow
                                    }
                                },
                                _ => {
                                    FunctionDomain::Domain(NullableDomain {
                                        has_null: true,
                                        value: None,
                                    })
                                },
                            }
                        },
                        passthrough_nullable_3_arg(func),
                    );
    }

    pub fn register_passthrough_nullable_4_arg<
        I1: ArgType,
        I2: ArgType,
        I3: ArgType,
        I4: ArgType,
        O: ArgType,
        F,
        G,
    >(
        &mut self,
        name: &str,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(
                &FunctionContext,
                &I1::Domain,
                &I2::Domain,
                &I3::Domain,
                &I4::Domain,
            ) -> FunctionDomain<O>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
        G: Fn(Value<I1>, Value<I2>, Value<I3>, Value<I4>, &mut EvalContext) -> Value<O>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
    {
        let has_nullable = &[
            I1::data_type(),
            I2::data_type(),
            I3::data_type(),
            I4::data_type(),
            O::data_type(),
        ]
        .iter()
        .any(|ty| ty.as_nullable().is_some() || ty.is_null());

        assert!(
            !has_nullable,
            "Function {} has nullable argument or output, please use register_4_arg_core instead",
            name
        );

        self.register_4_arg_core::<I1, I2, I3, I4, O, _, _>(name, calc_domain, func);

        self.register_4_arg_core::<NullableType<I1>, NullableType<I2>, NullableType<I3>, NullableType<I4>,  NullableType<O>, _, _>(
                        name,
                        move |ctx, arg1,arg2,arg3,arg4,| {
                            match (&arg1.value,&arg2.value,&arg3.value,&arg4.value) {
                                (Some(value1),Some(value2),Some(value3),Some(value4)) => {
                                    if let Some(domain) = calc_domain(ctx, value1,value2,value3,value4,).normalize() {
                                        FunctionDomain::Domain(NullableDomain {
                                            has_null: arg1.has_null||arg2.has_null||arg3.has_null||arg4.has_null,
                                            value: Some(Box::new(domain)),
                                        })
                                    } else {
                                        FunctionDomain::MayThrow
                                    }
                                },
                                _ => {
                                    FunctionDomain::Domain(NullableDomain {
                                        has_null: true,
                                        value: None,
                                    })
                                },
                            }
                        },
                        passthrough_nullable_4_arg(func),
                    );
    }

    pub fn register_combine_nullable_1_arg<I1: ArgType, O: ArgType, F, G>(
        &mut self,
        name: &str,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(&FunctionContext, &I1::Domain) -> FunctionDomain<NullableType<O>>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
        G: Fn(Value<I1>, &mut EvalContext) -> Value<NullableType<O>>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
    {
        let has_nullable = &[I1::data_type(), O::data_type()]
            .iter()
            .any(|ty| ty.as_nullable().is_some() || ty.is_null());

        assert!(
            !has_nullable,
            "Function {} has nullable argument or output, please use register_1_arg_core instead",
            name
        );

        self.register_1_arg_core::<I1, NullableType<O>, _, _>(name, calc_domain, func);

        self.register_1_arg_core::<NullableType<I1>, NullableType<O>, _, _>(
            name,
            move |ctx, arg1| match (&arg1.value) {
                (Some(value1)) => {
                    if let Some(domain) = calc_domain(ctx, value1).normalize() {
                        FunctionDomain::Domain(NullableDomain {
                            has_null: arg1.has_null || domain.has_null,
                            value: domain.value,
                        })
                    } else {
                        FunctionDomain::MayThrow
                    }
                }
                _ => FunctionDomain::Domain(NullableDomain {
                    has_null: true,
                    value: None,
                }),
            },
            combine_nullable_1_arg(func),
        );
    }

    pub fn register_combine_nullable_2_arg<I1: ArgType, I2: ArgType, O: ArgType, F, G>(
        &mut self,
        name: &str,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(&FunctionContext, &I1::Domain, &I2::Domain) -> FunctionDomain<NullableType<O>>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
        G: Fn(Value<I1>, Value<I2>, &mut EvalContext) -> Value<NullableType<O>>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
    {
        let has_nullable = &[I1::data_type(), I2::data_type(), O::data_type()]
            .iter()
            .any(|ty| ty.as_nullable().is_some() || ty.is_null());

        assert!(
            !has_nullable,
            "Function {} has nullable argument or output, please use register_2_arg_core instead",
            name
        );

        self.register_2_arg_core::<I1, I2, NullableType<O>, _, _>(name, calc_domain, func);

        self.register_2_arg_core::<NullableType<I1>, NullableType<I2>, NullableType<O>, _, _>(
            name,
            move |ctx, arg1, arg2| match (&arg1.value, &arg2.value) {
                (Some(value1), Some(value2)) => {
                    if let Some(domain) = calc_domain(ctx, value1, value2).normalize() {
                        FunctionDomain::Domain(NullableDomain {
                            has_null: arg1.has_null || arg2.has_null || domain.has_null,
                            value: domain.value,
                        })
                    } else {
                        FunctionDomain::MayThrow
                    }
                }
                _ => FunctionDomain::Domain(NullableDomain {
                    has_null: true,
                    value: None,
                }),
            },
            combine_nullable_2_arg(func),
        );
    }

    pub fn register_0_arg_core<O: ArgType, G>(
        &mut self,
        name: &str,
        calc_domain: fn(&FunctionContext) -> FunctionDomain<O>,
        func: G,
    ) where
        G: Fn(&mut EvalContext) -> Value<O> + 'static + Clone + Copy + Send + Sync,
    {
        self.scalar_builder(name)
            .function()
            .typed_0_arg()
            .calc_domain(calc_domain)
            .vectorized(func)
            .register();
    }

    pub fn register_1_arg_core<I1: ArgType, O: ArgType, F, G>(
        &mut self,
        name: &str,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(&FunctionContext, &I1::Domain) -> FunctionDomain<O>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
        G: Fn(Value<I1>, &mut EvalContext) -> Value<O> + 'static + Clone + Copy + Send + Sync,
    {
        let func = Function {
            signature: FunctionSignature {
                name: name.to_string(),
                args_type: vec![I1::data_type()],
                return_type: O::data_type(),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(EraseCalcDomainGeneric1Arg::<I1, O, F> {
                    calc_domain,
                    _marker: PhantomData,
                }),
                eval: Box::new(EraseFunctionGeneric1Arg {
                    func,
                    _marker: PhantomData,
                }),
                derive_stat: None,
            },
        };
        self.register_function(func);
    }

    pub fn register_2_arg_core<I1: ArgType, I2: ArgType, O: ArgType, F, G>(
        &mut self,
        name: &str,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(&FunctionContext, &I1::Domain, &I2::Domain) -> FunctionDomain<O>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
        G: Fn(Value<I1>, Value<I2>, &mut EvalContext) -> Value<O>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
    {
        let func = Function {
            signature: FunctionSignature {
                name: name.to_string(),
                args_type: vec![I1::data_type(), I2::data_type()],
                return_type: O::data_type(),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(EraseCalcDomainGeneric2Arg::<I1, I2, O, F> {
                    calc_domain,
                    _marker: PhantomData,
                }),
                eval: Box::new(EraseFunctionGeneric2Arg {
                    func,
                    _marker: PhantomData,
                }),
                derive_stat: None,
            },
        };
        self.register_function(func);
    }

    pub fn register_3_arg_core<I1: ArgType, I2: ArgType, I3: ArgType, O: ArgType, F, G>(
        &mut self,
        name: &str,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(&FunctionContext, &I1::Domain, &I2::Domain, &I3::Domain) -> FunctionDomain<O>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
        G: Fn(Value<I1>, Value<I2>, Value<I3>, &mut EvalContext) -> Value<O>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
    {
        let func = Function {
            signature: FunctionSignature {
                name: name.to_string(),
                args_type: vec![I1::data_type(), I2::data_type(), I3::data_type()],
                return_type: O::data_type(),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(EraseCalcDomainGeneric3Arg::<I1, I2, I3, O, F> {
                    calc_domain,
                    _marker: PhantomData,
                }),
                eval: Box::new(EraseFunctionGeneric3Arg {
                    func,
                    _marker: PhantomData,
                }),
                derive_stat: None,
            },
        };
        self.register_function(func);
    }

    pub fn register_4_arg_core<
        I1: ArgType,
        I2: ArgType,
        I3: ArgType,
        I4: ArgType,
        O: ArgType,
        F,
        G,
    >(
        &mut self,
        name: &str,
        calc_domain: F,
        func: G,
    ) where
        F: Fn(
                &FunctionContext,
                &I1::Domain,
                &I2::Domain,
                &I3::Domain,
                &I4::Domain,
            ) -> FunctionDomain<O>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
        G: Fn(Value<I1>, Value<I2>, Value<I3>, Value<I4>, &mut EvalContext) -> Value<O>
            + 'static
            + Clone
            + Copy
            + Send
            + Sync,
    {
        let func = Function {
            signature: FunctionSignature {
                name: name.to_string(),
                args_type: vec![
                    I1::data_type(),
                    I2::data_type(),
                    I3::data_type(),
                    I4::data_type(),
                ],
                return_type: O::data_type(),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(EraseCalcDomainGeneric4Arg::<I1, I2, I3, I4, O, F> {
                    calc_domain,
                    _marker: PhantomData,
                }),
                eval: Box::new(EraseFunctionGeneric4Arg {
                    func,
                    _marker: PhantomData,
                }),
                derive_stat: None,
            },
        };
        self.register_function(func);
    }
}

struct EraseCalcDomainGeneric0Arg<F> {
    func: F,
}

impl<O, F> ScalarFunctionDomain for EraseCalcDomainGeneric0Arg<F>
where
    O: ArgType,
    F: Fn(&FunctionContext) -> FunctionDomain<O> + Send + Sync + 'static,
{
    fn domain_eval(&self, ctx: &FunctionContext, _domains: &[Domain]) -> FunctionDomain<AnyType> {
        (self.func)(ctx).map(|d| O::upcast_domain(d))
    }
}

struct EraseCalcDomainGeneric1Arg<I1, O, F> {
    calc_domain: F,
    _marker: PhantomData<fn(I1, O)>,
}

impl<I1, O, F> ScalarFunctionDomain for EraseCalcDomainGeneric1Arg<I1, O, F>
where
    I1: ArgType,
    O: ArgType,
    F: Fn(&FunctionContext, &I1::Domain) -> FunctionDomain<O> + Send + Sync + 'static,
{
    fn domain_eval(&self, ctx: &FunctionContext, domains: &[Domain]) -> FunctionDomain<AnyType> {
        let arg1 = I1::try_downcast_domain(&domains[0]).unwrap();
        (self.calc_domain)(ctx, &arg1).map(|d| O::upcast_domain(d))
    }
}

struct EraseCalcDomainGeneric2Arg<I1, I2, O, F> {
    calc_domain: F,
    _marker: PhantomData<fn(I1, I2, O)>,
}

impl<I1, I2, O, F> ScalarFunctionDomain for EraseCalcDomainGeneric2Arg<I1, I2, O, F>
where
    I1: ArgType,
    I2: ArgType,
    O: ArgType,
    F: Fn(&FunctionContext, &I1::Domain, &I2::Domain) -> FunctionDomain<O> + Send + Sync + 'static,
{
    fn domain_eval(&self, ctx: &FunctionContext, domains: &[Domain]) -> FunctionDomain<AnyType> {
        let arg1 = I1::try_downcast_domain(&domains[0]).unwrap();
        let arg2 = I2::try_downcast_domain(&domains[1]).unwrap();
        (self.calc_domain)(ctx, &arg1, &arg2).map(|d| O::upcast_domain(d))
    }
}

struct EraseCalcDomainGeneric3Arg<I1, I2, I3, O, F> {
    calc_domain: F,
    _marker: PhantomData<fn(I1, I2, I3, O)>,
}

impl<I1, I2, I3, O, F> ScalarFunctionDomain for EraseCalcDomainGeneric3Arg<I1, I2, I3, O, F>
where
    I1: ArgType,
    I2: ArgType,
    I3: ArgType,
    O: ArgType,
    F: Fn(&FunctionContext, &I1::Domain, &I2::Domain, &I3::Domain) -> FunctionDomain<O>
        + Send
        + Sync
        + 'static,
{
    fn domain_eval(&self, ctx: &FunctionContext, domains: &[Domain]) -> FunctionDomain<AnyType> {
        let arg1 = I1::try_downcast_domain(&domains[0]).unwrap();
        let arg2 = I2::try_downcast_domain(&domains[1]).unwrap();
        let arg3 = I3::try_downcast_domain(&domains[2]).unwrap();
        (self.calc_domain)(ctx, &arg1, &arg2, &arg3).map(|d| O::upcast_domain(d))
    }
}

struct EraseCalcDomainGeneric4Arg<I1, I2, I3, I4, O, F> {
    calc_domain: F,
    _marker: PhantomData<fn(I1, I2, I3, I4, O)>,
}

impl<I1, I2, I3, I4, O, F> ScalarFunctionDomain for EraseCalcDomainGeneric4Arg<I1, I2, I3, I4, O, F>
where
    I1: ArgType,
    I2: ArgType,
    I3: ArgType,
    I4: ArgType,
    O: ArgType,
    F: Fn(
            &FunctionContext,
            &I1::Domain,
            &I2::Domain,
            &I3::Domain,
            &I4::Domain,
        ) -> FunctionDomain<O>
        + Send
        + Sync
        + 'static,
{
    fn domain_eval(&self, ctx: &FunctionContext, domains: &[Domain]) -> FunctionDomain<AnyType> {
        let arg1 = I1::try_downcast_domain(&domains[0]).unwrap();
        let arg2 = I2::try_downcast_domain(&domains[1]).unwrap();
        let arg3 = I3::try_downcast_domain(&domains[2]).unwrap();
        let arg4 = I4::try_downcast_domain(&domains[3]).unwrap();
        (self.calc_domain)(ctx, &arg1, &arg2, &arg3, &arg4).map(|d| O::upcast_domain(d))
    }
}

struct EraseFunctionGeneric0Arg<F> {
    func: F,
}

impl<O, F> ScalarFunction for EraseFunctionGeneric0Arg<F>
where
    O: ArgType,
    F: Fn(&mut EvalContext) -> Value<O> + Send + Sync + 'static,
{
    fn eval(&self, _args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        Value::upcast((self.func)(ctx))
    }
}

struct EraseFunctionGeneric1Arg<I1, F> {
    func: F,
    _marker: PhantomData<fn(I1)>,
}

impl<I1, O, F> ScalarFunction for EraseFunctionGeneric1Arg<I1, F>
where
    I1: ArgType,
    O: ArgType,
    F: Fn(Value<I1>, &mut EvalContext) -> Value<O> + Send + Sync + 'static,
{
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        match args[0].try_downcast() {
            Ok(arg1) => Value::upcast((self.func)(arg1, ctx)),
            Err(_) => match &args[0] {
                Value::Scalar(scalar) => {
                    unreachable!("can't downcast from {scalar:?} to {}", I1::data_type())
                }
                Value::Column(column) => unreachable!(
                    "can't downcast from {} to {}",
                    column.data_type(),
                    I1::data_type()
                ),
            },
        }
    }
}

struct EraseFunctionGeneric2Arg<I1, I2, O, F> {
    func: F,
    _marker: PhantomData<fn(I1, I2, O)>,
}

impl<I1, I2, O, F> ScalarFunction for EraseFunctionGeneric2Arg<I1, I2, O, F>
where
    I1: ArgType,
    I2: ArgType,
    O: ArgType,
    F: Fn(Value<I1>, Value<I2>, &mut EvalContext) -> Value<O> + Send + Sync + 'static,
{
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        let arg1 = args[0].try_downcast().unwrap();
        let arg2 = args[1].try_downcast().unwrap();
        Value::upcast((self.func)(arg1, arg2, ctx))
    }
}

struct EraseFunctionGeneric3Arg<I1, I2, I3, O, F> {
    func: F,
    _marker: PhantomData<fn(I1, I2, I3, O)>,
}

impl<I1, I2, I3, O, F> ScalarFunction for EraseFunctionGeneric3Arg<I1, I2, I3, O, F>
where
    I1: ArgType,
    I2: ArgType,
    I3: ArgType,
    O: ArgType,
    F: Fn(Value<I1>, Value<I2>, Value<I3>, &mut EvalContext) -> Value<O> + Send + Sync + 'static,
{
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        let arg1 = args[0].try_downcast().unwrap();
        let arg2 = args[1].try_downcast().unwrap();
        let arg3 = args[2].try_downcast().unwrap();
        Value::upcast((self.func)(arg1, arg2, arg3, ctx))
    }
}

struct EraseFunctionGeneric4Arg<I1, I2, I3, I4, O, F> {
    func: F,
    _marker: PhantomData<fn(I1, I2, I3, I4, O)>,
}

impl<I1, I2, I3, I4, O, F> ScalarFunction for EraseFunctionGeneric4Arg<I1, I2, I3, I4, O, F>
where
    I1: ArgType,
    I2: ArgType,
    I3: ArgType,
    I4: ArgType,
    O: ArgType,
    F: Fn(Value<I1>, Value<I2>, Value<I3>, Value<I4>, &mut EvalContext) -> Value<O>
        + Send
        + Sync
        + 'static,
{
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        let arg1 = args[0].try_downcast().unwrap();
        let arg2 = args[1].try_downcast().unwrap();
        let arg3 = args[2].try_downcast().unwrap();
        let arg4 = args[3].try_downcast().unwrap();
        Value::upcast((self.func)(arg1, arg2, arg3, arg4, ctx))
    }
}
