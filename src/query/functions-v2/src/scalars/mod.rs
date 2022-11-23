// Copyright 2022 Datafuse Labs.
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

use common_expression::FunctionRegistry;
use ctor::ctor;

mod arithmetic;
mod arithmetic_modulo;
mod array;
mod boolean;
mod control;
mod datetime;
mod geo;
mod math;
mod tuple;
mod variant;

mod comparison;
mod string;
mod string_multi_args;

#[ctor]
pub static BUILTIN_FUNCTIONS: FunctionRegistry = builtin_functions();

fn builtin_functions() -> FunctionRegistry {
    let mut registry = FunctionRegistry::new();
    arithmetic::register(&mut registry);
    array::register(&mut registry);
    boolean::register(&mut registry);
    control::register(&mut registry);
    comparison::register(&mut registry);
    datetime::register(&mut registry);
    math::register(&mut registry);
    string::register(&mut registry);
    string_multi_args::register(&mut registry);
    tuple::register(&mut registry);
    variant::register(&mut registry);
    geo::register(&mut registry);
    registry
}
