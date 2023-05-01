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

mod format;
mod metadata;
#[allow(clippy::module_inception)]
mod planner;
mod semantic;

pub mod binder;
mod expression_parser;
pub mod optimizer;
pub mod plans;
mod udf_validator;

pub use binder::parse_result_scan_args;
pub use binder::BindContext;
pub use binder::Binder;
pub use binder::ColumnBinding;
pub use binder::ScalarBinder;
pub use binder::ScalarVisitor;
pub use binder::SelectBuilder;
pub use binder::Visibility;
pub use expression_parser::*;
pub use metadata::*;
pub use planner::PlanExtras;
pub use planner::Planner;
pub use plans::ScalarExpr;
pub use semantic::*;
