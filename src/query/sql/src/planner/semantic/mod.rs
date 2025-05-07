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

mod aggregate_rewriter;
mod aggregating_index_visitor;
mod async_function_rewriter;
mod count_set_ops;
mod distinct_to_groupby;
mod grouping_check;
mod lowering;
mod name_resolution;
mod type_check;
mod udf_rewriter;
mod view_rewriter;
mod window_check;

pub use aggregate_rewriter::AggregateRewriter;
pub use aggregating_index_visitor::AggregatingIndexChecker;
pub use aggregating_index_visitor::AggregatingIndexRewriter;
pub use aggregating_index_visitor::RefreshAggregatingIndexRewriter;
pub(crate) use async_function_rewriter::AsyncFunctionRewriter;
pub use count_set_ops::CountSetOps;
pub use distinct_to_groupby::DistinctToGroupBy;
pub use grouping_check::GroupingChecker;
pub use lowering::*;
pub use name_resolution::compare_table_name;
pub use name_resolution::normalize_identifier;
pub use name_resolution::IdentifierNormalizer;
pub use name_resolution::NameResolutionContext;
pub use name_resolution::NameResolutionSuggest;
pub use name_resolution::VariableNormalizer;
pub use type_check::resolve_type_name;
pub use type_check::resolve_type_name_by_str;
pub use type_check::resolve_type_name_udf;
pub use type_check::validate_function_arg;
pub use type_check::TypeChecker;
pub(crate) use udf_rewriter::UdfRewriter;
pub use view_rewriter::ViewRewriter;
pub use window_check::WindowChecker;

pub(crate) const SUPPORTED_AGGREGATING_INDEX_FUNCTIONS: [&str; 6] =
    ["sum", "min", "max", "avg", "count", "approx_count_distinct"];
