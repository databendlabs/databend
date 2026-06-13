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

use std::hash::Hash;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::ScalarFunction;
use databend_common_expression::Value;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::F32;
use databend_common_expression::types::F64;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::decimal::*;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::with_integer_mapped_type;

pub fn register_decimal_hash<H: HashFunction>(registry: &mut FunctionRegistry) {
    registry.register_function_factory(
        H::name(),
        FunctionFactory::Closure(Box::new(|_, args_type| {
            decimal_hash_factory_1_arg::<H>(args_type)
        })),
    );
}

pub fn register_decimal_hash_with_seed<H: HashFunctionWithSeed>(registry: &mut FunctionRegistry) {
    registry.register_function_factory(
        H::name(),
        FunctionFactory::Closure(Box::new(|_, args_type| {
            decimal_hash_factory_2_arg::<H>(args_type)
        })),
    );
}

pub trait HashFunction: 'static {
    type Hasher: Hasher + Default;
    const IS_HASH_32: bool = false;
    fn name() -> &'static str;
}

pub trait HashFunctionWithSeed: Hasher + 'static {
    fn name() -> &'static str;

    fn with_seed(seed: u64) -> Self;
}

fn decimal_hash_factory_1_arg<H: HashFunction>(args_type: &[DataType]) -> Option<Arc<Function>> {
    let (nullable, size) = match args_type {
        [DataType::Null] => (true, DecimalSize::default_128()),
        [DataType::Nullable(box DataType::Decimal(size))] => (true, *size),
        [DataType::Decimal(size)] => (false, *size),
        _ => return None,
    };

    let signature = FunctionSignature {
        name: H::name().to_string(),
        args_type: [DataType::Decimal(size)].into(),
        return_type: if H::IS_HASH_32 {
            DataType::Number(u32::data_type())
        } else {
            DataType::Number(u64::data_type())
        },
    };

    let eval = DecimalHash::<H> { _h: PhantomData };
    Some(Arc::new(Function::with_passthrough_nullable(
        signature,
        FunctionDomain::Full,
        eval,
        None,
        nullable,
    )))
}

fn decimal_hash_factory_2_arg<H: HashFunctionWithSeed>(
    args_type: &[DataType],
) -> Option<Arc<Function>> {
    let (nullable, size, seed_type) = match args_type {
        [DataType::Null, DataType::Number(number)] => (true, DecimalSize::default_128(), *number),
        [
            DataType::Nullable(box DataType::Decimal(size)),
            DataType::Number(number),
        ] => (true, *size, *number),
        [DataType::Decimal(size), DataType::Number(number)] => (false, *size, *number),
        [
            DataType::Null,
            DataType::Nullable(box DataType::Number(number)),
        ] => (true, DecimalSize::default_128(), *number),
        [
            DataType::Nullable(box DataType::Decimal(size)),
            DataType::Nullable(box DataType::Number(number)),
        ] => (true, *size, *number),
        [
            DataType::Decimal(size),
            DataType::Nullable(box DataType::Number(number)),
        ] => (true, *size, *number),
        _ => return None,
    };

    let signature = FunctionSignature {
        name: H::name().to_string(),
        args_type: [DataType::Decimal(size), DataType::Number(seed_type)].into(),
        return_type: DataType::Number(u64::data_type()),
    };

    let eval = DecimalHashWithSeed::<H> {
        seed_type,
        _h: PhantomData,
    };
    Some(Arc::new(Function::with_passthrough_nullable(
        signature,
        FunctionDomain::Full,
        eval,
        None,
        nullable,
    )))
}

fn decimal_hash_typed<H, R, D, T>(
    arg: Value<D>,
    ctx: &mut EvalContext,
    scale: u8,
    cast: fn(u64) -> R::Scalar,
) -> Value<AnyType>
where
    T: Decimal + Hash,
    H: Hasher + Default,
    R: ArgType,
    D: for<'a> AccessType<ScalarRef<'a> = T>,
{
    vectorize_with_builder_1_arg::<D, R>(|arg, output, _| {
        let mut state = H::default();
        scale.hash(&mut state);
        arg.hash(&mut state);
        R::push_item(output, R::to_scalar_ref(&cast(state.finish())));
    })(arg, ctx)
    .upcast()
}

fn decimal_hash<H, R>(
    arg: Value<AnyType>,
    ctx: &mut EvalContext,
    cast: fn(u64) -> R::Scalar,
) -> Value<AnyType>
where
    H: Hasher + Default,
    R: ArgType,
{
    let (decimal_type, _) = DecimalDataType::from_value(&arg).unwrap();
    let size = decimal_type.size();
    if size.can_carried_by_128() {
        match decimal_type {
            DecimalDataType::Decimal64(_) => {
                let arg = arg.try_downcast::<Decimal64As128Type>().unwrap();
                decimal_hash_typed::<H, R, _, _>(arg, ctx, size.scale(), cast)
            }
            DecimalDataType::Decimal128(_) => {
                let arg = arg.try_downcast::<Decimal128Type>().unwrap();
                decimal_hash_typed::<H, R, _, _>(arg, ctx, size.scale(), cast)
            }
            DecimalDataType::Decimal256(_) => {
                let arg = arg.try_downcast::<Decimal256As128Type>().unwrap();
                decimal_hash_typed::<H, R, _, _>(arg, ctx, size.scale(), cast)
            }
        }
    } else {
        match decimal_type {
            DecimalDataType::Decimal64(_) => {
                let arg = arg.try_downcast::<Decimal64As256Type>().unwrap();
                decimal_hash_typed::<H, R, _, _>(arg, ctx, size.scale(), cast)
            }
            DecimalDataType::Decimal128(_) => {
                let arg = arg.try_downcast::<Decimal128As256Type>().unwrap();
                decimal_hash_typed::<H, R, _, _>(arg, ctx, size.scale(), cast)
            }
            DecimalDataType::Decimal256(_) => {
                let arg = arg.try_downcast::<Decimal256Type>().unwrap();
                decimal_hash_typed::<H, R, _, _>(arg, ctx, size.scale(), cast)
            }
        }
    }
}

fn decimal_hash_typed_with_seed<H, S, T, D>(
    arg: Value<D>,
    seed: Value<S>,
    ctx: &mut EvalContext,
    scale: u8,
    cast: fn(S::ScalarRef<'_>) -> u64,
) -> Value<AnyType>
where
    H: HashFunctionWithSeed,
    S: AccessType,
    T: Decimal + Hash,
    D: for<'a> AccessType<ScalarRef<'a> = T>,
{
    vectorize_with_builder_2_arg::<D, S, UInt64Type>(|arg, seed, output, _| {
        let mut state = H::with_seed(cast(seed));
        scale.hash(&mut state);
        arg.hash(&mut state);
        output.push(state.finish());
    })(arg, seed, ctx)
    .upcast()
}

fn decimal_hash_with_seed<H, S>(
    arg: Value<AnyType>,
    seed: Value<AnyType>,
    ctx: &mut EvalContext,
    cast: fn(S::ScalarRef<'_>) -> u64,
) -> Value<AnyType>
where
    H: HashFunctionWithSeed,
    S: AccessType,
{
    let (decimal_type, _) = DecimalDataType::from_value(&arg).unwrap();
    let size = decimal_type.size();
    let scale = size.scale();
    if size.can_carried_by_128() {
        match decimal_type {
            DecimalDataType::Decimal64(_) => {
                let arg = arg.try_downcast::<Decimal64As128Type>().unwrap();
                let seed: Value<S> = seed.try_downcast().unwrap();
                decimal_hash_typed_with_seed::<H, S, _, _>(arg, seed, ctx, scale, cast)
            }
            DecimalDataType::Decimal128(_) => {
                let arg = arg.try_downcast::<Decimal128Type>().unwrap();
                let seed: Value<S> = seed.try_downcast().unwrap();
                decimal_hash_typed_with_seed::<H, S, _, _>(arg, seed, ctx, scale, cast)
            }
            DecimalDataType::Decimal256(_) => {
                let arg = arg.try_downcast::<Decimal256As128Type>().unwrap();
                let seed: Value<S> = seed.try_downcast().unwrap();
                decimal_hash_typed_with_seed::<H, S, _, _>(arg, seed, ctx, size.scale(), cast)
            }
        }
    } else {
        match decimal_type {
            DecimalDataType::Decimal64(_) => {
                let arg = arg.try_downcast::<Decimal64As256Type>().unwrap();
                let seed: Value<S> = seed.try_downcast().unwrap();
                decimal_hash_typed_with_seed::<H, S, _, _>(arg, seed, ctx, size.scale(), cast)
            }
            DecimalDataType::Decimal128(_) => {
                let arg = arg.try_downcast::<Decimal128As256Type>().unwrap();
                let seed: Value<S> = seed.try_downcast().unwrap();
                decimal_hash_typed_with_seed::<H, S, _, _>(arg, seed, ctx, size.scale(), cast)
            }
            DecimalDataType::Decimal256(_) => {
                let arg = arg.try_downcast::<Decimal256Type>().unwrap();
                let seed: Value<S> = seed.try_downcast().unwrap();
                decimal_hash_typed_with_seed::<H, S, _, _>(arg, seed, ctx, size.scale(), cast)
            }
        }
    }
}

#[derive(Clone)]
struct DecimalHash<H> {
    _h: PhantomData<fn(H)>,
}

impl<H> ScalarFunction for DecimalHash<H>
where H: HashFunction
{
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        let arg = args[0].clone();
        if H::IS_HASH_32 {
            decimal_hash::<H::Hasher, UInt32Type>(arg, ctx, |res| res.try_into().unwrap())
        } else {
            decimal_hash::<H::Hasher, UInt64Type>(arg, ctx, |res| res)
        }
    }
}

#[derive(Clone)]
struct DecimalHashWithSeed<H> {
    seed_type: NumberDataType,
    _h: PhantomData<fn(H)>,
}

impl<H> ScalarFunction for DecimalHashWithSeed<H>
where H: HashFunctionWithSeed
{
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        let arg = &args[0];
        let seed = &args[1];
        with_integer_mapped_type!(|NUM| match self.seed_type {
            NumberDataType::NUM =>
                decimal_hash_with_seed::<H, NumberType<NUM>>(arg.clone(), seed.clone(), ctx, |s| s
                    as _),
            NumberDataType::Float32 => decimal_hash_with_seed::<H, NumberType<F32>>(
                arg.clone(),
                seed.clone(),
                ctx,
                |s| s.0 as _
            ),
            NumberDataType::Float64 => decimal_hash_with_seed::<H, NumberType<F64>>(
                arg.clone(),
                seed.clone(),
                ctx,
                |s| s.0 as _
            ),
        })
    }
}
