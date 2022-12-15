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

use std::fmt::Display;
use std::fmt::Formatter;

use common_exception::Result;

use crate::optimizer::rule::TransformResult;
use crate::optimizer::SExpr;

pub type RulePtr = Box<dyn Rule>;

pub trait Rule {
    fn id(&self) -> RuleID;

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()>;

    fn pattern(&self) -> &SExpr;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum RuleID {
    // Rewrite rules
    NormalizeScalarFilter,
    NormalizeDisjunctiveFilter,
    PushDownFilterEvalScalar,
    PushDownFilterUnion,
    PushDownFilterJoin,
    PushDownFilterScan,
    PushDownLimitUnion,
    PushDownLimitOuterJoin,
    PushDownLimitSort,
    PushDownLimitScan,
    PushDownSortScan,
    EliminateEvalScalar,
    EliminateFilter,
    MergeEvalScalar,
    MergeFilter,
    SplitAggregate,
    FoldCountAggregate,

    // Exploration rules
    CommuteJoin,
    LeftAssociateJoin,
    RightAssociateJoin,

    // Implementation rules
    ImplementGet,
    ImplementHashJoin,
}

impl Display for RuleID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RuleID::PushDownFilterUnion => write!(f, "PushDownFilterUnion"),
            RuleID::PushDownFilterEvalScalar => write!(f, "PushDownFilterEvalScalar"),
            RuleID::PushDownFilterJoin => write!(f, "PushDownFilterJoin"),
            RuleID::PushDownFilterScan => write!(f, "PushDownFilterScan"),
            RuleID::PushDownLimitUnion => write!(f, "PushDownLimitUnion"),
            RuleID::PushDownLimitOuterJoin => write!(f, "PushDownLimitOuterJoin"),
            RuleID::PushDownLimitSort => write!(f, "PushDownLimitSort"),
            RuleID::PushDownLimitScan => write!(f, "PushDownLimitScan"),
            RuleID::PushDownSortScan => write!(f, "PushDownSortScan"),
            RuleID::EliminateEvalScalar => write!(f, "EliminateEvalScalar"),
            RuleID::EliminateFilter => write!(f, "EliminateFilter"),
            RuleID::MergeEvalScalar => write!(f, "MergeEvalScalar"),
            RuleID::MergeFilter => write!(f, "MergeFilter"),
            RuleID::NormalizeScalarFilter => write!(f, "NormalizeScalarFilter"),
            RuleID::SplitAggregate => write!(f, "SplitAggregate"),
            RuleID::NormalizeDisjunctiveFilter => write!(f, "NormalizeDisjunctiveFilter"),
            RuleID::FoldCountAggregate => write!(f, "FoldCountAggregate"),

            RuleID::CommuteJoin => write!(f, "CommuteJoin"),
            RuleID::LeftAssociateJoin => write!(f, "LeftAssociateJoin"),
            RuleID::RightAssociateJoin => write!(f, "RightAssociateJoin"),

            RuleID::ImplementGet => write!(f, "ImplementGet"),
            RuleID::ImplementHashJoin => write!(f, "ImplementHashJoin"),
        }
    }
}
