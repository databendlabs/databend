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

use std::fmt::Display;
use std::fmt::Formatter;

use common_exception::Result;

use crate::sql::optimizer::SExpr;

mod factory;
mod rewrite;
mod rule_implement_get;
mod rule_implement_hash_join;
mod rule_set;
mod transform;
mod transform_state;

pub use factory::RuleFactory;
pub use rule_set::AppliedRules;
pub use rule_set::RuleSet;
pub use transform_state::TransformState;

pub type RulePtr = Box<dyn Rule>;

pub trait Rule {
    fn id(&self) -> RuleID;

    fn apply(&self, s_expr: &SExpr, state: &mut TransformState) -> Result<()>;

    fn pattern(&self) -> &SExpr;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum RuleID {
    // Rewrite rules
    NormalizeScalarFilter,
    NormalizeDisjunctiveFilter,
    PushDownFilterProject,
    PushDownFilterEvalScalar,
    PushDownFilterJoin,
    PushDownFilterScan,
    PushDownLimitOuterJoin,
    PushDownLimitProject,
    PushDownLimitSort,
    PushDownLimitScan,
    PushDownSortScan,
    EliminateEvalScalar,
    EliminateFilter,
    EliminateProject,
    MergeProject,
    MergeEvalScalar,
    MergeFilter,
    SplitAggregate,

    // Exploration rules
    CommuteJoin,

    // Implementation rules
    ImplementGet,
    ImplementHashJoin,
}

impl Display for RuleID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RuleID::PushDownFilterProject => write!(f, "PushDownFilterProject"),
            RuleID::PushDownFilterEvalScalar => write!(f, "PushDownFilterEvalScalar"),
            RuleID::PushDownFilterJoin => write!(f, "PushDownFilterJoin"),
            RuleID::PushDownFilterScan => write!(f, "PushDownFilterScan"),
            RuleID::PushDownLimitOuterJoin => write!(f, "PushDownLimitOuterJoin"),
            RuleID::PushDownLimitProject => write!(f, "PushDownLimitProject"),
            RuleID::PushDownLimitSort => write!(f, "PushDownLimitSort"),
            RuleID::PushDownLimitScan => write!(f, "PushDownLimitScan"),
            RuleID::PushDownSortScan => write!(f, "PushDownSortScan"),
            RuleID::EliminateEvalScalar => write!(f, "EliminateEvalScalar"),
            RuleID::EliminateFilter => write!(f, "EliminateFilter"),
            RuleID::EliminateProject => write!(f, "EliminateProject"),
            RuleID::MergeProject => write!(f, "MergeProject"),
            RuleID::MergeEvalScalar => write!(f, "MergeEvalScalar"),
            RuleID::MergeFilter => write!(f, "MergeFilter"),
            RuleID::NormalizeScalarFilter => write!(f, "NormalizeScalarFilter"),
            RuleID::SplitAggregate => write!(f, "SplitAggregate"),
            RuleID::NormalizeDisjunctiveFilter => write!(f, "NormalizeDisjunctiveFilter"),

            RuleID::CommuteJoin => write!(f, "CommuteJoin"),

            RuleID::ImplementGet => write!(f, "ImplementGet"),
            RuleID::ImplementHashJoin => write!(f, "ImplementHashJoin"),
        }
    }
}
