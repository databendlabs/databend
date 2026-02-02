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

mod cascades;
mod cse;
pub mod cte_filter_pushdown;
pub mod distributed;
mod eliminate_self_join;
mod hyper_dp;
pub mod operator;
pub mod recursive;
pub mod rule;
pub use cascades::CascadesOptimizer;
pub use cse::CommonSubexpressionOptimizer;
pub use cte_filter_pushdown::CTEFilterPushdownOptimizer;
pub use eliminate_self_join::EliminateSelfJoinOptimizer;
pub use hyper_dp::DPhpyOptimizer;
pub use operator::CleanupUnusedCTEOptimizer;
