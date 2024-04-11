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
pub mod exception;
mod exception_backtrace;
mod exception_code;
mod exception_flight;
mod exception_into;
mod span;
mod with_context;

pub use exception::ErrorCode;
pub use exception::Result;
pub use exception::ToErrorCode;
pub use exception_into::SerializedError;
pub use span::merge_span;
pub use span::offset_span;
pub use span::pretty_print_error;
pub use span::Range;
pub use span::Span;
pub use with_context::ErrorWithContext;
pub use with_context::WithContext;
