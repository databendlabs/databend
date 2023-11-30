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
mod error;
pub use error::display_parser_error;
pub use error::Backtrace;
pub use error::Error;
pub use error::ErrorKind;

mod visitors;
pub use visitors::walk_expr;
pub use visitors::walk_expr_mut;
pub use visitors::walk_query;
pub use visitors::walk_query_mut;
pub use visitors::walk_select_target;
pub use visitors::walk_select_target_mut;
pub use visitors::walk_statement_mut;
pub use visitors::walk_table_reference_mut;
pub use visitors::Visitor;
pub use visitors::VisitorMut;

mod input;
pub use input::Dialect;
pub use input::Input;

mod util;
pub use util::match_text;
pub use util::match_token;
pub use util::IResult;

pub mod ast;
pub mod parser;
