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

#![allow(clippy::uninlined_format_args)]
#![feature(core_intrinsics)]
#![feature(box_patterns)]
#![feature(type_ascription)]

use aggregates::AggregateFunctionFactory;
use common_expression::FunctionRegistry;
use ctor::ctor;

pub mod aggregates;
mod cast_rules;
pub mod scalars;
pub mod srfs;

pub fn is_builtin_function(name: &str) -> bool {
    BUILTIN_FUNCTIONS.contains(name)
        || AggregateFunctionFactory::instance().contains(name)
        || GENERAL_WINDOW_FUNCTIONS.contains(&name)
}

#[ctor]
pub static BUILTIN_FUNCTIONS: FunctionRegistry = builtin_functions();

pub const GENERAL_WINDOW_FUNCTIONS: [&str; 12] = [
    "row_number",
    "rank",
    "dense_rank",
    "percent_rank",
    "lag",
    "lead",
    "first_value",
    "first",
    "last_value",
    "last",
    "nth_value",
    "ntile",
];

fn builtin_functions() -> FunctionRegistry {
    let mut registry = FunctionRegistry::empty();

    cast_rules::register(&mut registry);
    scalars::register(&mut registry);
    srfs::register(&mut registry);

    registry
}
