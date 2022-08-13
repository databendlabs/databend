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

#![deny(unused_crate_dependencies)]

pub use analyzer::analyzer_expr_sync;
pub use analyzer::analyzer_value_expr;
mod analyzer;
mod parser;
pub mod sql_common;
mod sql_dialect;

pub use parser::ExprParser;
pub use parser::ExpressionParser;
pub use sql_dialect::SQLDialect;
