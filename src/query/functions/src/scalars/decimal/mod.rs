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

mod arithmetic;
mod cast;
mod comparison;
mod math;

pub(crate) use arithmetic::register_decimal_arithmetic;
pub(crate) use cast::convert_to_decimal;
pub(crate) use cast::convert_to_decimal_domain;
pub(crate) use cast::register_decimal_to_float;
pub(crate) use cast::register_decimal_to_int;
pub(crate) use cast::register_decimal_to_string;
pub(crate) use cast::register_to_decimal;
pub(crate) use comparison::register_decimal_compare_op;
pub(crate) use math::register_decimal_math;
