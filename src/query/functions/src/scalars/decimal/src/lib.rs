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

#![allow(clippy::arc_with_non_send_sync)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::ptr_arg)]
#![allow(clippy::type_complexity)]
#![allow(internal_features)]
#![feature(core_intrinsics)]
#![feature(box_patterns)]
#![feature(type_ascription)]
#![feature(try_blocks)]
#![feature(downcast_unchecked)]
#![feature(str_internals)]

mod arithmetic;
mod cast;
mod comparison;
mod math;

pub use arithmetic::register_decimal_arithmetic;
pub use cast::convert_to_decimal;
pub use cast::convert_to_decimal_domain;
pub use cast::register_decimal_to_float;
pub use cast::register_decimal_to_int;
pub use cast::register_decimal_to_string;
pub use cast::register_to_decimal;
pub use comparison::register_decimal_compare_op;
pub use math::register_decimal_math;
