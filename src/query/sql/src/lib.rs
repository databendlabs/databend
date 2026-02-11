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

#![allow(
    clippy::collapsible_if,
    clippy::derivable_impls,
    clippy::iter_kv_map,
    clippy::let_and_return,
    clippy::replace_box,
    clippy::unnecessary_unwrap,
    clippy::uninlined_format_args
)]
#![allow(non_local_definitions)]
#![feature(box_patterns)]
#![feature(iterator_try_reduce)]
#![feature(trivial_bounds)]
#![feature(try_blocks)]
#![feature(extend_one)]
#![feature(if_let_guard)]
#![feature(iter_next_chunk)]
#![feature(once_cell_try)]

pub mod evaluator;
pub mod executor;
pub mod planner;

pub use planner::*;

/// Convert a meta service error to an ErrorCode.
pub(crate) fn meta_service_error(
    e: databend_meta_types::MetaError,
) -> databend_common_exception::ErrorCode {
    databend_common_exception::ErrorCode::MetaServiceError(e.to_string())
}
