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

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use super::DynamicCastRules;
use super::Function;
use super::FunctionEval;
use super::FunctionFactory;
use super::FunctionProperty;
use super::FunctionRegistry;
use super::FunctionSignature;
use super::ScalarFunction;
use super::ScalarFunctionDomain;
use super::function_factory::PassthroughNullableDomain;
use super::register_vectorize::VectorizedFn1;
use super::register_vectorize::VectorizedFn2;
use super::register_vectorize::passthrough_nullable_1_arg;
use super::register_vectorize::passthrough_nullable_2_arg;
use super::register_vectorize::vectorize_1_arg;
use super::register_vectorize::vectorize_2_arg;
use super::register_vectorize::vectorize_with_builder_1_arg;
use crate::EvalContext;
use crate::FunctionContext;
use crate::FunctionDomain;
use crate::Scalar;
use crate::VectorizedFn0;
use crate::function_stat::DeriveStat;
use crate::function_stat::ReturnStat;
use crate::function_stat::StatBinaryArg;
use crate::function_stat::StatUnaryArg;
use crate::property::Domain;
use crate::types::AccessType;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::NullableType;
use crate::types::ReturnType;
use crate::types::ValueType;
use crate::types::nullable::NullableDomain;
use crate::values::Value;

impl FunctionRegistry {
    pub fn scalar_builder(&mut self, name: impl Into<String>) -> ScalarBuilder<'_> {
        ScalarBuilder::new(self, name.into())
    }

    pub fn factory_builder(&mut self, name: impl Into<String>) -> FactoryBuilder<'_> {
        FactoryBuilder::new(self, name.into())
    }
}

pub struct FunctionBuilder<'a, const IS_FACTORY: bool> {
    registry: &'a mut FunctionRegistry,
    name: String,
    property: Option<FunctionProperty>,
    aliases: Vec<String>,
    additional_cast_rules: Vec<(DataType, DataType)>,
    dynamic_cast_rules: DynamicCastRules,
    functions: Vec<Function>,
    factory: Option<FunctionFactory>,
}

pub type ScalarBuilder<'a> = FunctionBuilder<'a, false>;
pub type FactoryBuilder<'a> = FunctionBuilder<'a, true>;

impl<'a, const IS_FACTORY: bool> FunctionBuilder<'a, IS_FACTORY> {
    fn new(registry: &'a mut FunctionRegistry, name: String) -> Self {
        Self {
            registry,
            name,
            property: None,
            aliases: Vec::new(),
            additional_cast_rules: Vec::new(),
            dynamic_cast_rules: Vec::new(),
            functions: Vec::new(),
            factory: None,
        }
    }

    pub fn property(mut self, property: FunctionProperty) -> Self {
        self.property = Some(property);
        self
    }

    pub fn aliases(mut self, aliases: &[&'static str]) -> Self {
        self.aliases
            .extend(aliases.iter().copied().map(str::to_string));
        self
    }

    pub fn additional_cast_rules(
        mut self,
        cast_rules: impl IntoIterator<Item = (DataType, DataType)>,
    ) -> Self {
        self.additional_cast_rules.extend(cast_rules);
        self
    }

    pub fn dynamic_cast_rules(
        mut self,
        rules: impl IntoIterator<Item = Arc<dyn Fn(&DataType, &DataType) -> bool + Send + Sync>>,
    ) -> Self {
        self.dynamic_cast_rules.extend(rules);
        self
    }

    fn register_common(
        registry: &mut FunctionRegistry,
        name: &str,
        property: Option<FunctionProperty>,
        aliases: Vec<String>,
        additional_cast_rules: Vec<(DataType, DataType)>,
        dynamic_cast_rules: DynamicCastRules,
    ) {
        if let Some(property) = property {
            registry.properties.insert(name.to_string(), property);
        }

        if !aliases.is_empty() {
            for alias in aliases {
                let previous = registry.aliases.insert(alias, name.to_string());
                if previous.is_some() {
                    unreachable!();
                }
            }
        }

        if !additional_cast_rules.is_empty() {
            registry
                .additional_cast_rules
                .entry(name.to_string())
                .or_default()
                .extend(additional_cast_rules);
        }

        if !dynamic_cast_rules.is_empty() {
            registry
                .dynamic_cast_rules
                .entry(name.to_string())
                .or_default()
                .extend(dynamic_cast_rules);
        }
    }
}

impl<'a> ScalarBuilder<'a> {
    pub fn function(self) -> ScalarFunctionArityBuilder<Self> {
        ScalarFunctionArityBuilder { builder: self }
    }

    pub fn register(self) -> &'a mut FunctionRegistry {
        let FunctionBuilder {
            registry,
            name,
            property,
            aliases,
            additional_cast_rules,
            dynamic_cast_rules,
            mut functions,
            ..
        } = self;

        assert!(
            !functions.is_empty(),
            "at least one function must be provided before registering"
        );

        for mut function in functions.drain(..) {
            assert_eq!(function.signature.name, name);
            registry.register_function(function);
        }

        Self::register_common(
            registry,
            &name,
            property,
            aliases,
            additional_cast_rules,
            dynamic_cast_rules,
        );

        registry
    }
}

impl<'a> FactoryBuilder<'a> {
    pub fn factory(
        mut self,
        create: impl Fn(
            &[Scalar],
            &[DataType],
            ScalarFunctionArityBuilder<InlineFunctionBuilder>,
        ) -> Option<InlineFunctionBuilder>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        let name = self.name.clone();
        self.factory = Some(FunctionFactory::Closure(Box::new(move |params, args| {
            create(params, args, ScalarFunctionArityBuilder {
                builder: InlineFunctionBuilder::new(name.clone()),
            })
            .map(|builder| Arc::new(builder.finish()))
        })));
        self
    }

    pub fn register(self) -> &'a mut FunctionRegistry {
        let FunctionBuilder {
            registry,
            name,
            property,
            aliases,
            additional_cast_rules,
            dynamic_cast_rules,
            factory,
            ..
        } = self;

        let factory = factory.expect("function factory must be set before registering");

        registry.register_function_factory(&name, factory);

        Self::register_common(
            registry,
            &name,
            property,
            aliases,
            additional_cast_rules,
            dynamic_cast_rules,
        );

        registry
    }
}

pub struct InlineFunctionBuilder {
    name: String,
    function: Option<Function>,
}

pub trait ScalarFunctionCollect {
    const FOR_FACTORY: bool;
    fn name(&self) -> &str;
    fn collect(&mut self, function: Function);
}

impl<'a> ScalarFunctionCollect for ScalarBuilder<'a> {
    const FOR_FACTORY: bool = false;

    fn name(&self) -> &str {
        &self.name
    }

    fn collect(&mut self, function: Function) {
        self.functions.push(function);
    }
}

impl InlineFunctionBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            function: None,
        }
    }

    pub fn finish(self) -> Function {
        self.function
            .expect("typed builder must produce exactly one function")
    }
}

impl ScalarFunctionCollect for InlineFunctionBuilder {
    const FOR_FACTORY: bool = true;

    fn name(&self) -> &str {
        &self.name
    }

    fn collect(&mut self, function: Function) {
        assert!(self.function.is_none(), "function already built");
        self.function = Some(function);
    }
}

pub struct ScalarFunctionArityBuilder<B> {
    builder: B,
}

impl<B: ScalarFunctionCollect> ScalarFunctionArityBuilder<B> {
    pub fn typed_0_arg<O: ArgType>(self) -> TypedNullaryFunctionBuilder<O, B> {
        self.with_type_0_arg(O::data_type())
    }

    pub fn with_type_0_arg<O: ReturnType>(
        self,
        return_type: DataType,
    ) -> TypedNullaryFunctionBuilder<O, B> {
        TypedNullaryFunctionBuilder {
            builder: self.builder,
            return_type,
            calc_domain: None,
            derive_stat: None,
        }
    }

    pub fn typed_1_arg<I: ArgType, O: ArgType>(self) -> TypedUnaryFunctionBuilder<I, O, B> {
        TypedUnaryFunctionBuilder::new(self.builder)
    }

    pub fn with_type_1_arg<I: AccessType, O: ReturnType>(
        self,
        arg_type: DataType,
        return_type: DataType,
    ) -> TypedUnaryFunctionBuilder<I, O, B> {
        TypedUnaryFunctionBuilder::new_with_type(self.builder, arg_type, return_type)
    }

    pub fn typed_2_arg<I1: ArgType, I2: ArgType, O: ArgType>(
        self,
    ) -> TypedBinaryFunctionBuilder<I1, I2, O, B> {
        self.with_type_2_arg([I1::data_type(), I2::data_type()], O::data_type())
    }

    pub fn with_type_2_arg<I1: AccessType, I2: AccessType, O: ReturnType>(
        self,
        args_type: [DataType; 2],
        return_type: DataType,
    ) -> TypedBinaryFunctionBuilder<I1, I2, O, B> {
        TypedBinaryFunctionBuilder::with_type(self.builder, args_type, return_type)
    }
}

pub struct TypedNullaryFunctionBuilder<O: ReturnType, B: ScalarFunctionCollect> {
    builder: B,
    return_type: DataType,
    calc_domain: Option<fn(&FunctionContext) -> FunctionDomain<O>>,
    derive_stat: Option<DeriveStat>,
}

impl<O: ReturnType, B: ScalarFunctionCollect> TypedNullaryFunctionBuilder<O, B> {
    pub fn calc_domain(mut self, calc_domain: fn(&FunctionContext) -> FunctionDomain<O>) -> Self {
        assert!(
            self.calc_domain.is_none(),
            "typed calc_domain already specified"
        );
        self.calc_domain = Some(calc_domain);
        self
    }

    pub fn derive_stat(
        mut self,
        func: fn(cardinality: f64, ctx: &FunctionContext) -> Result<Option<ReturnStat>, String>,
    ) -> Self {
        self.derive_stat = Some(DeriveStat::Nullary(func));
        self
    }

    pub fn vectorized<F>(self, eval: F) -> B
    where F: VectorizedFn0<O> + 'static {
        let Self {
            builder,
            return_type,
            calc_domain,
            derive_stat,
        } = self;
        let mut builder = builder;
        let calc_domain = calc_domain.expect("typed calc_domain must be specified");

        let signature = FunctionSignature {
            name: builder.name().to_string(),
            args_type: Vec::new(),
            return_type: return_type.clone(),
        };

        let calc_wrapper = TypedNullaryCalcDomain {
            calc_domain,
            return_type: return_type.clone(),
            _marker: PhantomData,
        };
        let eval_wrapper = TypedNullaryFunction {
            func: eval,
            return_type,
            _marker: PhantomData,
        };

        builder.collect(Function {
            signature,
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(calc_wrapper),
                eval: Box::new(eval_wrapper),
                derive_stat: derive_stat.map(|eval| Box::new(eval) as _),
            },
        });
        builder
    }
}

#[derive(Clone)]
struct TypedNullaryCalcDomain<O: ReturnType> {
    calc_domain: fn(&FunctionContext) -> FunctionDomain<O>,
    return_type: DataType,
    _marker: PhantomData<fn() -> O>,
}

impl<O> ScalarFunctionDomain for TypedNullaryCalcDomain<O>
where O: ReturnType
{
    fn domain_eval(&self, ctx: &FunctionContext, domains: &[Domain]) -> FunctionDomain<AnyType> {
        debug_assert!(
            domains.is_empty(),
            "nullary functions expect zero arguments"
        );
        (self.calc_domain)(ctx).map(|domain| O::upcast_domain_with_type(domain, &self.return_type))
    }
}

#[derive(Clone)]
struct TypedNullaryFunction<O: ReturnType, F: VectorizedFn0<O> + 'static> {
    func: F,
    return_type: DataType,
    _marker: PhantomData<fn() -> O>,
}

impl<O, F> ScalarFunction for TypedNullaryFunction<O, F>
where
    O: ReturnType,
    F: VectorizedFn0<O>,
{
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        debug_assert!(args.is_empty(), "nullary functions expect zero arguments");
        Value::upcast_with_type((self.func)(ctx), &self.return_type)
    }
}

pub struct TypedUnaryFunctionBuilder<I: AccessType, O: AccessType, B: ScalarFunctionCollect> {
    builder: B,
    args_type: [DataType; 1],
    return_type: DataType,
    calc_domain: Option<fn(&FunctionContext, &I::Domain) -> FunctionDomain<O>>,
    derive_stat: Option<DeriveStat>,
    passthrough_nullable: bool,
    is_heavy: bool,
    _marker: PhantomData<fn(I) -> O>,
}

impl<I: ArgType, O: ArgType, B: ScalarFunctionCollect> TypedUnaryFunctionBuilder<I, O, B> {
    pub fn new(builder: B) -> Self {
        Self::new_with_type(builder, I::data_type(), O::data_type())
    }
}

impl<I: AccessType, O: ReturnType, B: ScalarFunctionCollect> TypedUnaryFunctionBuilder<I, O, B> {
    pub fn new_with_type(builder: B, arg_type: DataType, return_type: DataType) -> Self {
        TypedUnaryFunctionBuilder {
            builder,
            args_type: [arg_type],
            return_type,
            calc_domain: None,
            derive_stat: None,
            passthrough_nullable: false,
            is_heavy: false,
            _marker: PhantomData,
        }
    }

    pub fn passthrough_nullable(mut self) -> Self {
        assert!(
            ![&self.args_type[0], &self.return_type]
                .into_iter()
                .any(|ty| ty.is_nullable()),
            "function `{}` has nullable argument or output, passthrough nullable requires non-nullable types",
            self.builder.name()
        );
        self.passthrough_nullable = true;
        self
    }

    pub fn calc_domain(
        mut self,
        calc_domain: fn(&FunctionContext, &I::Domain) -> FunctionDomain<O>,
    ) -> Self {
        assert!(
            self.calc_domain.is_none(),
            "typed calc_domain already specified"
        );
        self.calc_domain = Some(calc_domain);
        self
    }

    pub fn derive_stat(
        mut self,
        func: fn(StatUnaryArg, ctx: &FunctionContext) -> Result<Option<ReturnStat>, String>,
    ) -> Self {
        self.derive_stat = Some(DeriveStat::Unary(func));
        self
    }

    pub fn vectorized<F>(self, func: F) -> B
    where F: VectorizedFn1<I, O> + 'static {
        self.finish_with(func)
    }

    pub fn each_row<F>(self, func: F) -> B
    where F: Fn(I::ScalarRef<'_>, &mut EvalContext) -> O::Scalar + Send + Sync + Copy + 'static
    {
        self.finish_with(vectorize_1_arg(func))
    }

    pub fn each_row_throw<F, E>(self, func: F) -> B
    where
        E: fmt::Display + 'static,
        F: Fn(I::ScalarRef<'_>, &mut EvalContext) -> Result<O::Scalar, E>
            + Send
            + Sync
            + Copy
            + 'static,
    {
        self.finish_with(vectorize_with_builder_1_arg::<I, O>(
            move |a, output, ctx| match func(a, ctx) {
                Err(msg) => {
                    ctx.set_error(O::builder_len(output), msg);
                    O::push_default(output);
                }
                Ok(val) => O::push_item(output, O::to_scalar_ref(&val)),
            },
        ))
    }

    pub fn each_row_heavy(
        mut self,
        func: fn(I::ScalarRef<'_>, &mut EvalContext) -> O::Scalar,
    ) -> B {
        self.is_heavy = true;
        self.finish_with(vectorize_1_arg(func))
    }

    fn finish_with<F>(self, func: F) -> B
    where F: VectorizedFn1<I, O> + 'static {
        let Self {
            builder,
            args_type,
            return_type,
            calc_domain,
            derive_stat,
            passthrough_nullable,
            ..
        } = self;
        let mut builder = builder;

        let signature = FunctionSignature {
            name: builder.name().to_string(),
            args_type: args_type.to_vec(),
            return_type: return_type.clone(),
        };

        let calc_wrapper = TypedUnaryCalcDomain::<I, O> {
            calc_domain: calc_domain.unwrap_or(|_, _| FunctionDomain::Full),
            return_type: return_type.clone(),
            _marker: PhantomData,
        };

        if !(passthrough_nullable && B::FOR_FACTORY) {
            builder.collect(Function {
                signature: signature.clone(),
                eval: FunctionEval::Scalar {
                    calc_domain: Box::new(calc_wrapper.clone()),
                    eval: Box::new(TypedUnaryFunction::<I, O, F> {
                        func,
                        return_type,
                        _marker: PhantomData,
                    }),
                    derive_stat: derive_stat.map(|eval| Box::new(eval) as _),
                },
            });
            if B::FOR_FACTORY {
                return builder;
            }
        }

        if passthrough_nullable {
            builder.collect(Self::build_passthrough_nullable(
                signature,
                calc_wrapper,
                derive_stat,
                func,
            ));
        }

        builder
    }

    fn build_passthrough_nullable<F>(
        signature: FunctionSignature,
        calc_wrapper: TypedUnaryCalcDomain<I, O>,
        derive_stat: Option<DeriveStat>,
        eval: F,
    ) -> Function
    where
        F: VectorizedFn1<I, O> + 'static,
    {
        let FunctionSignature {
            name,
            args_type,
            return_type,
        } = signature;
        Function {
            signature: FunctionSignature {
                name,
                args_type: args_type.into_iter().map(|ty| ty.wrap_nullable()).collect(),
                return_type: return_type.wrap_nullable(),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(PassthroughNullableDomain(calc_wrapper)),
                eval: Box::new(NullableUnaryFunction::<I, O, _> {
                    eval,
                    return_type: return_type.wrap_nullable(),
                    _marker: PhantomData,
                }),
                derive_stat: derive_stat.map(|eval| Box::new(eval) as _),
            },
        }
    }
}

#[derive(Clone)]
struct TypedUnaryCalcDomain<I: AccessType, O: ReturnType> {
    calc_domain: fn(&FunctionContext, &I::Domain) -> FunctionDomain<O>,
    return_type: DataType,
    _marker: PhantomData<fn(I) -> O>,
}

impl<I, O> ScalarFunctionDomain for TypedUnaryCalcDomain<I, O>
where
    I: AccessType,
    O: ReturnType,
{
    fn domain_eval(&self, ctx: &FunctionContext, domains: &[Domain]) -> FunctionDomain<AnyType> {
        debug_assert_eq!(domains.len(), 1, "unary functions expect one argument");
        let arg = I::try_downcast_domain(&domains[0]).unwrap();
        (self.calc_domain)(ctx, &arg)
            .map(|domain| O::upcast_domain_with_type(domain, &self.return_type))
    }
}

#[derive(Clone)]
struct TypedUnaryFunction<I: AccessType, O: ReturnType, F: VectorizedFn1<I, O> + 'static> {
    func: F,
    return_type: DataType,
    _marker: PhantomData<fn(I) -> O>,
}

impl<I, O, F> ScalarFunction for TypedUnaryFunction<I, O, F>
where
    I: AccessType,
    O: ReturnType,
    F: VectorizedFn1<I, O> + 'static,
{
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        debug_assert_eq!(args.len(), 1, "unary functions expect one argument");
        let arg = args[0].try_downcast().unwrap();
        let value = (self.func)(arg, ctx);
        Value::upcast_with_type(value, &self.return_type)
    }
}

#[derive(Clone)]
struct NullableUnaryFunction<I: AccessType, O: ReturnType, F: VectorizedFn1<I, O> + 'static> {
    eval: F,
    return_type: DataType,
    _marker: PhantomData<fn(I) -> O>,
}

impl<I, O, F> ScalarFunction for NullableUnaryFunction<I, O, F>
where
    I: AccessType,
    O: ReturnType,
    F: VectorizedFn1<I, O> + 'static,
{
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        debug_assert_eq!(args.len(), 1, "unary functions expect one argument");
        let arg = args[0].try_downcast::<NullableType<I>>().unwrap();
        let value = passthrough_nullable_1_arg(self.eval)(arg, ctx);
        Value::upcast_with_type(value, &self.return_type)
    }
}

pub struct TypedBinaryFunctionBuilder<
    I1: AccessType,
    I2: AccessType,
    O: ReturnType,
    B: ScalarFunctionCollect,
> {
    builder: B,
    args_type: [DataType; 2],
    return_type: DataType,
    calc_domain: Option<fn(&FunctionContext, &I1::Domain, &I2::Domain) -> FunctionDomain<O>>,
    derive_stat: Option<DeriveStat>,
    passthrough_nullable: bool,
    is_heavy: bool,
    _marker: PhantomData<fn(I1, I2) -> O>,
}

impl<I1, I2, O, B> TypedBinaryFunctionBuilder<I1, I2, O, B>
where
    I1: AccessType,
    I2: AccessType,
    O: ReturnType,
    B: ScalarFunctionCollect,
{
    pub fn with_type(builder: B, args_type: [DataType; 2], return_type: DataType) -> Self {
        Self {
            builder,
            args_type,
            return_type,
            calc_domain: None,
            derive_stat: None,
            passthrough_nullable: false,
            is_heavy: false,
            _marker: PhantomData,
        }
    }

    pub fn calc_domain(
        mut self,
        calc_domain: fn(&FunctionContext, &I1::Domain, &I2::Domain) -> FunctionDomain<O>,
    ) -> Self {
        assert!(self.calc_domain.is_none(), "calc_domain already specified");
        self.calc_domain = Some(calc_domain);
        self
    }

    pub fn derive_stat(
        mut self,
        func: fn(StatBinaryArg, ctx: &FunctionContext) -> Result<Option<ReturnStat>, String>,
    ) -> Self {
        self.derive_stat = Some(DeriveStat::Binary(func));
        self
    }

    pub fn passthrough_nullable(mut self) -> Self {
        assert!(
            ![&self.args_type[0], &self.args_type[1], &self.return_type]
                .into_iter()
                .any(|ty| ty.is_nullable()),
            "function `{}` has nullable argument or output, passthrough nullable requires non-nullable types",
            self.builder.name()
        );

        self.passthrough_nullable = true;
        self
    }

    pub fn vectorized<F>(self, func: F) -> B
    where F: VectorizedFn2<I1, I2, O> + 'static {
        self.finish_with(func)
    }

    pub fn each_row<F>(self, func: F) -> B
    where F: Fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>, &mut EvalContext) -> O::Scalar
            + Send
            + Sync
            + Copy
            + 'static {
        self.finish_with(vectorize_2_arg(func))
    }

    pub fn each_row_heavy(
        mut self,
        func: fn(I1::ScalarRef<'_>, I2::ScalarRef<'_>, &mut EvalContext) -> O::Scalar,
    ) -> B {
        self.is_heavy = true;
        self.finish_with(vectorize_2_arg(func))
    }

    fn finish_with<F>(self, func: F) -> B
    where F: VectorizedFn2<I1, I2, O> + 'static {
        let Self {
            builder,
            args_type,
            return_type,
            calc_domain,
            derive_stat,
            passthrough_nullable,
            ..
        } = self;
        let mut builder = builder;

        let calc_domain = calc_domain.expect("typed calc_domain must be specified");

        let signature = FunctionSignature {
            name: builder.name().to_string(),
            args_type: args_type.to_vec(),
            return_type: return_type.clone(),
        };

        let calc_wrapper = TypedBinaryCalcDomain::<I1, I2, O> {
            calc_domain,
            return_type: signature.return_type.clone(),
            _marker: PhantomData,
        };
        let eval_wrapper = TypedBinaryFunction::<I1, I2, O, _> {
            func,
            return_type: signature.return_type.clone(),
            _marker: PhantomData,
        };

        if !(passthrough_nullable && B::FOR_FACTORY) {
            builder.collect(Function {
                signature: signature.clone(),
                eval: FunctionEval::Scalar {
                    calc_domain: Box::new(calc_wrapper.clone()),
                    eval: Box::new(eval_wrapper),
                    derive_stat: derive_stat.map(|eval| Box::new(eval) as _),
                },
            });
            if B::FOR_FACTORY {
                return builder;
            }
        }

        if passthrough_nullable {
            builder.collect(Self::build_passthrough_nullable_binary::<_>(
                signature,
                calc_wrapper,
                derive_stat,
                func,
            ));
        }

        builder
    }

    fn build_passthrough_nullable_binary<F>(
        signature: FunctionSignature,
        calc_wrapper: TypedBinaryCalcDomain<I1, I2, O>,
        derive_stat: Option<DeriveStat>,
        eval: F,
    ) -> Function
    where
        F: VectorizedFn2<I1, I2, O> + 'static,
    {
        let FunctionSignature {
            name,
            args_type,
            return_type,
        } = signature;
        Function {
            signature: FunctionSignature {
                name,
                args_type: args_type.into_iter().map(|ty| ty.wrap_nullable()).collect(),
                return_type: return_type.wrap_nullable(),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(PassthroughNullableDomain(calc_wrapper)),
                eval: Box::new(NullableBinaryFunction::<I1, I2, O, _> {
                    eval,
                    return_type: return_type.wrap_nullable(),
                    _marker: PhantomData,
                }),
                derive_stat: derive_stat.map(|eval| Box::new(eval) as _),
            },
        }
    }
}

#[derive(Clone)]
struct TypedBinaryCalcDomain<I1: AccessType, I2: AccessType, O: ReturnType> {
    calc_domain: fn(&FunctionContext, &I1::Domain, &I2::Domain) -> FunctionDomain<O>,
    return_type: DataType,
    _marker: PhantomData<fn(I1, I2) -> O>,
}

impl<I1, I2, O> ScalarFunctionDomain for TypedBinaryCalcDomain<I1, I2, O>
where
    I1: AccessType,
    I2: AccessType,
    O: ReturnType,
{
    fn domain_eval(&self, ctx: &FunctionContext, domains: &[Domain]) -> FunctionDomain<AnyType> {
        debug_assert_eq!(domains.len(), 2, "binary functions expect two arguments");
        let arg1 = I1::try_downcast_domain(&domains[0]).unwrap();
        let arg2 = I2::try_downcast_domain(&domains[1]).unwrap();
        (self.calc_domain)(ctx, &arg1, &arg2)
            .map(|domain| O::upcast_domain_with_type(domain, &self.return_type))
    }
}

#[derive(Clone)]
struct TypedBinaryFunction<
    I1: AccessType,
    I2: AccessType,
    O: ReturnType,
    F: VectorizedFn2<I1, I2, O> + 'static,
> {
    func: F,
    return_type: DataType,
    _marker: PhantomData<fn(I1, I2) -> O>,
}

impl<I1, I2, O, F> ScalarFunction for TypedBinaryFunction<I1, I2, O, F>
where
    I1: AccessType,
    I2: AccessType,
    O: ReturnType,
    F: VectorizedFn2<I1, I2, O> + 'static,
{
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        debug_assert_eq!(args.len(), 2, "binary functions expect two arguments");
        let arg1 = args[0].try_downcast().unwrap();
        let arg2 = args[1].try_downcast().unwrap();
        let value = (self.func)(arg1, arg2, ctx);
        Value::upcast_with_type(value, &self.return_type)
    }
}

#[derive(Clone)]
struct NullableBinaryFunction<
    I1: AccessType,
    I2: AccessType,
    O: ReturnType,
    F: VectorizedFn2<I1, I2, O> + 'static,
> {
    eval: F,
    return_type: DataType,
    _marker: PhantomData<fn(I1, I2) -> O>,
}

impl<I1, I2, O, F> ScalarFunction for NullableBinaryFunction<I1, I2, O, F>
where
    I1: AccessType,
    I2: AccessType,
    O: ReturnType,
    F: VectorizedFn2<I1, I2, O> + 'static,
{
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        debug_assert_eq!(args.len(), 2, "binary functions expect two arguments");
        let arg1 = args[0].try_downcast::<NullableType<I1>>().unwrap();
        let arg2 = args[1].try_downcast::<NullableType<I2>>().unwrap();
        let value = passthrough_nullable_2_arg(|a, b, ctx| (self.eval)(a, b, ctx))(arg1, arg2, ctx);
        Value::upcast_with_type(value, &self.return_type)
    }
}
