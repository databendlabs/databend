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
use databend_common_expression::FunctionRegistry;

mod arithmetic;
mod arithmetic_modulo;
mod array;
mod bitmap;
mod boolean;
mod comparison;
mod control;
mod datetime;
mod decimal;
mod geo;
mod geo_h3;
mod hash;
mod map;
mod math;
mod other;
mod string;
mod string_multi_args;
mod tuple;
mod variant;
mod vector;

pub use comparison::check_pattern_type;
pub use comparison::is_like_pattern_escape;
pub use comparison::PatternType;
pub use comparison::ALL_COMP_FUNC_NAMES;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ValueType;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::Value;
use databend_common_expression::ValueRef;

pub fn register(registry: &mut FunctionRegistry) {
    variant::register(registry);
    arithmetic::register(registry);
    array::register(registry);
    boolean::register(registry);
    control::register(registry);
    comparison::register(registry);
    datetime::register(registry);
    math::register(registry);
    map::register(registry);
    string::register(registry);
    string_multi_args::register(registry);
    tuple::register(registry);
    geo::register(registry);
    geo_h3::register(registry);
    hash::register(registry);
    other::register(registry);
    decimal::register_to_decimal(registry);
    vector::register(registry);
    bitmap::register(registry);
}

pub fn binary_op<'a, I1, I2, O, F>(
    a: ValueRef<'a, I1>,
    b: ValueRef<'a, I2>,
    func: F,
    ctx: &mut EvalContext,
) -> Value<AnyType>
where
    I1: ArgType,
    I2: ArgType,
    O: ArgType,
    F: Fn(
            <I1 as ValueType>::ScalarRef<'_>,
            <I2 as ValueType>::ScalarRef<'_>,
            &mut EvalContext,
        ) -> <O as ValueType>::Scalar
        + Copy
        + Send
        + Sync,
{
    let result = vectorize_2_arg::<I1, I2, O>(func)(a, b, ctx);
    match result {
        Value::Scalar(x) => Value::Scalar(O::upcast_scalar(x)),
        Value::Column(x) => Value::Column(O::upcast_column(x)),
    }
}

pub fn binary_op_with_builder<'a, I1, I2, O, F>(
    a: ValueRef<'a, I1>,
    b: ValueRef<'a, I2>,
    func: F,
    ctx: &mut EvalContext,
) -> Value<AnyType>
where
    I1: ArgType,
    I2: ArgType,
    O: ArgType,
    F: Fn(
            <I1 as ValueType>::ScalarRef<'_>,
            <I2 as ValueType>::ScalarRef<'_>,
            &mut O::ColumnBuilder,
            &mut EvalContext,
        ) + Copy
        + Send
        + Sync,
{
    let result = vectorize_with_builder_2_arg::<I1, I2, O>(func)(a, b, ctx);
    match result {
        Value::Scalar(x) => Value::Scalar(O::upcast_scalar(x)),
        Value::Column(x) => Value::Column(O::upcast_column(x)),
    }
}
