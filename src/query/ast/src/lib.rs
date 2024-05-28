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

#![feature(box_patterns)]
// TODO(xuanwo): Add crate level documents here.
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::type_complexity)]

pub mod ast;
mod error;
pub mod parser;
pub mod span;

pub use error::ParseError;
pub use error::Result;
pub use span::Range;
pub use span::Span;
