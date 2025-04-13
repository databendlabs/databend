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
#![allow(clippy::len_without_is_empty)]
#![allow(clippy::missing_transmute_annotations)]
#![allow(clippy::arc_with_non_send_sync)]
#![allow(internal_features)]
// FIXME: we should avoid this by implementing Ord correctly.
#![allow(clippy::non_canonical_partial_ord_impl)]
#![allow(incomplete_features)]
#![feature(fmt_internals)]
#![feature(const_try)]
#![feature(trivial_bounds)]
#![feature(iterator_try_reduce)]
#![feature(box_patterns)]
#![feature(type_ascription)]
#![allow(clippy::type_complexity)]
#![feature(associated_type_defaults)]
#![feature(anonymous_lifetime_in_impl_trait)]
#![feature(generic_const_exprs)]
#![feature(trait_alias)]
#![feature(vec_into_raw_parts)]
#![feature(iterator_try_collect)]
#![feature(core_intrinsics)]
#![feature(trusted_len)]
#![feature(iter_order_by)]
#![feature(int_roundings)]
#![feature(try_blocks)]
#![feature(let_chains)]
#![feature(alloc_layout_extra)]
#![feature(debug_closure_helpers)]
#![feature(never_type)]

#[allow(dead_code)]
mod block;

pub mod aggregate;
pub mod converts;
mod evaluator;
mod expression;
pub mod filter;
mod function;
mod hilbert;
mod input_columns;
mod kernels;
mod property;
mod register;
mod register_comparison;
#[allow(dead_code)]
mod register_vectorize;

pub mod row;
pub mod sampler;
pub mod schema;
pub mod type_check;
pub mod types;
pub mod utils;
pub mod values;

pub use crate::aggregate::*;
pub use crate::block::BlockMetaInfo;
pub use crate::block::BlockMetaInfoPtr;
pub use crate::block::*;
pub use crate::evaluator::*;
pub use crate::expression::*;
pub use crate::filter::*;
pub use crate::function::*;
pub use crate::hilbert::*;
pub use crate::input_columns::*;
pub use crate::kernels::*;
pub use crate::property::*;
pub use crate::register_comparison::*;
pub use crate::register_vectorize::*;
pub use crate::row::*;
pub use crate::schema::*;
pub use crate::utils::block_thresholds::BlockThresholds;
pub use crate::utils::*;
pub use crate::values::*;

pub mod expr {
    pub use super::expression::Cast;
    pub use super::expression::ColumnRef;
    pub use super::expression::Constant;
    pub use super::expression::Expr;
    pub use super::expression::FunctionCall;
    pub use super::expression::LambdaFunctionCall;
}

type IndexType = usize;
type ColumnSet = std::collections::BTreeSet<IndexType>;
