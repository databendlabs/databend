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

#![feature(const_try)]
#![feature(iterator_try_reduce)]
#![feature(const_fmt_arguments_new)]
#![feature(box_patterns)]
#![feature(type_ascription)]
#![feature(associated_type_defaults)]
#![feature(const_maybe_uninit_as_mut_ptr)]
#![feature(anonymous_lifetime_in_impl_trait)]
#![feature(const_mut_refs)]
#![feature(generic_const_exprs)]
#![feature(trait_alias)]
#![feature(iterator_try_collect)]
#![feature(core_intrinsics)]
#![feature(iter_order_by)]
#![allow(clippy::len_without_is_empty)]
#![allow(clippy::needless_lifetimes)]
#![allow(incomplete_features)]

pub type Result<T> = std::result::Result<T, (crate::Span, String)>;
#[allow(dead_code)]
mod chunk;

pub mod converts;
mod deserializations;
mod evaluator;
mod expression;
mod function;
mod kernels;
mod property;
mod register;
pub mod type_check;
pub mod types;
pub mod utils;
pub mod values;

pub use crate::chunk::*;
pub use crate::deserializations::TypeDeserializer;
pub use crate::evaluator::*;
pub use crate::expression::*;
pub use crate::function::*;
pub use crate::kernels::*;
pub use crate::property::*;
pub use crate::register::*;
pub use crate::values::*;
