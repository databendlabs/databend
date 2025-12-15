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

extern crate core;

mod context;
#[cfg(target_os = "linux")]
mod elf;
pub mod exception;
mod exception_backtrace;
mod exception_code;
mod exception_flight;
mod exception_into;

pub use context::ErrorFrame;
pub use context::ResultExt;
pub use context::display_error_stack;
pub use databend_common_ast::ParseError;
pub use databend_common_ast::span;
pub use exception::ErrorCode;
pub use exception::ErrorCodeResultExt;
pub use exception::Result;
pub use exception::ToErrorCode;
pub use exception::error_code_groups;
pub use exception_backtrace::StackTrace;
pub use exception_backtrace::USER_SET_ENABLE_BACKTRACE;
pub use exception_backtrace::set_backtrace;
pub use exception_into::SerializedError;
pub use span::merge_span;
