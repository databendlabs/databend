// Copyright 2021 Datafuse Labs.
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

use common_base::containers::concat;
use common_expression::types::DataType;
use common_expression::FunctionRegistry;

mod arithmetic;
mod arithmetic_modulo;
mod boolean;
mod control;
mod datetime;
mod math;
mod string;
mod string_multi_args;

pub fn builtin_functions() -> FunctionRegistry {
    let mut registry = FunctionRegistry::new();
    arithmetic::register(&mut registry);
    boolean::register(&mut registry);
    control::register(&mut registry);
    datetime::register(&mut registry);
    math::register(&mut registry);
    string::register(&mut registry);
    string_multi_args::register(&mut registry);
    registry
}

const ALL_INTEGER_TYPES: &[DataType; 8] = &[
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
];
const ALL_FLOAT_TYPES: &[DataType; 2] = &[DataType::Float32, DataType::Float64];
const ALL_NUMERICS_TYPES: &[DataType; 10] = &concat(ALL_INTEGER_TYPES, ALL_FLOAT_TYPES);
