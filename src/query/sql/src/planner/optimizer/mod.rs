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
mod cost;
mod distributed;
mod hyper_dp;
pub mod ir;
pub mod operator;
#[allow(clippy::module_inception)]
mod optimizer;
pub mod rule;
mod statistics;
mod util;

mod dynamic_sample;

pub use cascades::CascadesOptimizer;
pub use hyper_dp::DPhpy;
pub use optimizer::optimize;
pub use optimizer::optimize_query;
pub use optimizer::OptimizerContext;
pub use optimizer::RecursiveOptimizer;
pub use rule::agg_index;
pub use rule::RuleFactory;
pub use rule::RuleID;
pub use rule::RuleSet;
pub use rule::DEFAULT_REWRITE_RULES;
pub use util::contains_local_table_scan;
