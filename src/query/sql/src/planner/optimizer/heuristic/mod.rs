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

mod decorrelate;
#[allow(clippy::module_inception)]
mod heuristic;
mod subquery_rewriter;

pub use decorrelate::decorrelate_subquery;
pub use heuristic::HeuristicOptimizer;
pub use heuristic::DEFAULT_REWRITE_RULES;
pub use heuristic::RESIDUAL_RULES;
pub use subquery_rewriter::FlattenInfo;
pub use subquery_rewriter::SubqueryRewriter;
