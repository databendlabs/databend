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

use databend_common_expression::FunctionRegistry;

pub mod math_func {
    pub use databend_functions_scalar_math::*;
}
mod array;
mod binary;
mod bitmap;
mod boolean;
mod comparison;
mod control;

pub mod geo_func {
    pub use databend_functions_scalar_geo::*;
}

mod hash;
mod hilbert;

pub mod dt_func {
    pub use databend_functions_scalar_datetime::*;
}

mod map;

mod obfuscator;
mod other;
mod string;
mod string_multi_args;
mod tuple;
mod variant;
mod vector;

pub use comparison::ALL_COMP_FUNC_NAMES;
use databend_functions_scalar_arithmetic::arithmetic;
use databend_functions_scalar_numeric_basic_arithmetic::register_numeric_basic_arithmetic;
pub use hash::DFHash;
pub use string::ALL_STRING_FUNC_NAMES;

pub fn register(registry: &mut FunctionRegistry) {
    variant::register(registry);
    arithmetic::register(registry);
    // register basic arithmetic operation (+ - * / %)
    databend_functions_scalar_decimal::register_decimal_arithmetic(registry);
    databend_functions_scalar_integer_basic_arithmetic::register_integer_basic_arithmetic(registry);
    register_numeric_basic_arithmetic(registry);
    arithmetic::register_binary_arithmetic(registry);
    arithmetic::register_unary_arithmetic(registry);
    array::register(registry);
    boolean::register(registry);
    control::register(registry);
    comparison::register(registry);
    dt_func::datetime::register(registry);
    math_func::math::register(registry);
    map::register(registry);
    string::register(registry);
    binary::register(registry);
    string_multi_args::register(registry);
    tuple::register(registry);
    geo_func::geo::register(registry);
    geo_func::geo_h3::register(registry);
    hash::register(registry);
    other::register(registry);
    databend_functions_scalar_decimal::register_to_decimal(registry);
    vector::register(registry);
    bitmap::register(registry);
    geo_func::geometry::register(registry);
    geo_func::geography::register(registry);
    hilbert::register(registry);
    dt_func::interval::register(registry);
    obfuscator::register(registry);
}
