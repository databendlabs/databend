// Copyright 2021 Datafuse Labs.
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
mod group;
mod heuristic;
mod m_expr;
mod memo;
mod optimize_context;
mod pattern_extractor;
mod property;
mod rule;
mod s_expr;

use common_exception::Result;
pub use m_expr::MExpr;
pub use memo::Memo;
pub use optimize_context::OptimizeContext;
pub use pattern_extractor::PatternExtractor;
pub use property::ColumnSet;
pub use property::PhysicalProperty;
pub use property::RelationalProperty;
pub use property::RequiredProperty;
pub use s_expr::SExpr;

use crate::sql::optimizer::cascades::CascadesOptimizer;
use crate::sql::optimizer::heuristic::HeuristicOptimizer;
use crate::sql::optimizer::rule::RuleID;
use crate::sql::optimizer::rule::RuleSet;

pub fn optimize(expression: SExpr, ctx: OptimizeContext) -> Result<SExpr> {
    let mut heuristic = HeuristicOptimizer::create()?;
    let expression = heuristic.optimize(expression)?;
    let mut cascades = CascadesOptimizer::create(ctx);
    cascades.optimize(expression)
}
