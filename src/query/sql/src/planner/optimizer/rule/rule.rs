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

use std::fmt::Display;
use std::fmt::Formatter;

use common_exception::Result;
use num_derive::FromPrimitive;
use num_derive::ToPrimitive;

use crate::optimizer::rule::TransformResult;
use crate::optimizer::SExpr;

pub type RulePtr = Box<dyn Rule>;

pub trait Rule {
    fn id(&self) -> RuleID;

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()>;

    fn patterns(&self) -> &Vec<SExpr>;

    fn transformation(&self) -> bool {
        true
    }
}

// If add a new rule, please add it to the operator's corresponding `transformation_candidate_rules`
// Such as `PushDownFilterAggregate` is related to `Filter` operator.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, FromPrimitive, ToPrimitive)]
pub enum RuleID {
    // Rewrite rules
    FoldConstant,
    NormalizeScalarFilter,
    NormalizeDisjunctiveFilter,
    PushDownFilterAggregate,
    PushDownFilterEvalScalar,
    PushDownFilterUnion,
    PushDownFilterJoin,
    PushDownFilterScan,
    PushDownFilterSort,
    PushDownLimitUnion,
    PushDownLimitOuterJoin,
    RulePushDownLimitExpression,
    PushDownLimitSort,
    PushDownLimitAggregate,
    PushDownLimitScan,
    PushDownSortScan,
    EliminateEvalScalar,
    EliminateFilter,
    MergeEvalScalar,
    MergeFilter,
    SplitAggregate,
    FoldCountAggregate,
    PushDownPrewhere,

    // Exploration rules
    CommuteJoin,
    RightAssociateJoin,
    LeftAssociateJoin,
    ExchangeJoin,
    CommuteJoinBaseTable,
    LeftExchangeJoin,
    EagerAggregation,
    RightExchangeJoin,
}

impl Display for RuleID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RuleID::FoldConstant => write!(f, "FoldConstant"),
            RuleID::PushDownFilterUnion => write!(f, "PushDownFilterUnion"),
            RuleID::PushDownFilterEvalScalar => write!(f, "PushDownFilterEvalScalar"),
            RuleID::PushDownFilterJoin => write!(f, "PushDownFilterJoin"),
            RuleID::PushDownFilterScan => write!(f, "PushDownFilterScan"),
            RuleID::PushDownFilterSort => write!(f, "PushDownFilterSort"),
            RuleID::PushDownLimitUnion => write!(f, "PushDownLimitUnion"),
            RuleID::PushDownLimitOuterJoin => write!(f, "PushDownLimitOuterJoin"),
            RuleID::RulePushDownLimitExpression => write!(f, "PushDownLimitExpression"),
            RuleID::PushDownLimitSort => write!(f, "PushDownLimitSort"),
            RuleID::PushDownLimitAggregate => write!(f, "PushDownLimitAggregate"),
            RuleID::PushDownFilterAggregate => write!(f, "PushDownFilterAggregate"),
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
            RuleID::PushDownPrewhere => write!(f, "PushDownPrewhere"),

            RuleID::CommuteJoin => write!(f, "CommuteJoin"),
            RuleID::CommuteJoinBaseTable => write!(f, "CommuteJoinBaseTable"),
            RuleID::LeftAssociateJoin => write!(f, "LeftAssociateJoin"),
            RuleID::RightAssociateJoin => write!(f, "RightAssociateJoin"),
            RuleID::LeftExchangeJoin => write!(f, "LeftExchangeJoin"),
            RuleID::EagerAggregation => write!(f, "EagerAggregation"),
            RuleID::RightExchangeJoin => write!(f, "RightExchangeJoin"),
            RuleID::ExchangeJoin => write!(f, "ExchangeJoin"),
        }
    }
}
