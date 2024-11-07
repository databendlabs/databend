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

use databend_common_exception::Result;

use super::rewrite::RuleCommuteJoin;
use super::rewrite::RuleEliminateEvalScalar;
use super::rewrite::RuleEliminateFilter;
use super::rewrite::RuleEliminateSort;
use super::rewrite::RuleEliminateUnion;
use super::rewrite::RuleFilterNulls;
use super::rewrite::RuleFoldCountAggregate;
use super::rewrite::RuleMergeEvalScalar;
use super::rewrite::RuleMergeFilter;
use super::rewrite::RuleNormalizeScalarFilter;
use super::rewrite::RulePushDownFilterAggregate;
use super::rewrite::RulePushDownFilterEvalScalar;
use super::rewrite::RulePushDownFilterJoin;
use super::rewrite::RulePushDownFilterProjectSet;
use super::rewrite::RulePushDownFilterScan;
use super::rewrite::RulePushDownFilterSort;
use super::rewrite::RulePushDownFilterUnion;
use super::rewrite::RulePushDownFilterWindow;
use super::rewrite::RulePushDownFilterWindowTopN;
use super::rewrite::RulePushDownLimit;
use super::rewrite::RulePushDownLimitEvalScalar;
use super::rewrite::RulePushDownLimitOuterJoin;
use super::rewrite::RulePushDownLimitScan;
use super::rewrite::RulePushDownLimitSort;
use super::rewrite::RulePushDownLimitUnion;
use super::rewrite::RulePushDownLimitWindow;
use super::rewrite::RulePushDownPrewhere;
use super::rewrite::RulePushDownRankLimitAggregate;
use super::rewrite::RulePushDownSortEvalScalar;
use super::rewrite::RulePushDownSortScan;
use super::rewrite::RuleSemiToInnerJoin;
use super::rewrite::RuleSplitAggregate;
use super::rewrite::RuleTryApplyAggIndex;
use super::transform::RuleCommuteJoinBaseTable;
use super::transform::RuleEagerAggregation;
use super::transform::RuleLeftExchangeJoin;
use super::RuleID;
use super::RulePtr;
use crate::optimizer::OptimizerContext;

pub struct RuleFactory;

pub const MAX_PUSH_DOWN_LIMIT: usize = 10000;

impl RuleFactory {
    pub fn create_rule(id: RuleID, ctx: OptimizerContext) -> Result<RulePtr> {
        match id {
            RuleID::EliminateUnion => Ok(Box::new(RuleEliminateUnion::new(ctx.metadata))),
            RuleID::EliminateEvalScalar => Ok(Box::new(RuleEliminateEvalScalar::new(ctx.metadata))),
            RuleID::FilterNulls => Ok(Box::new(RuleFilterNulls::new(
                ctx.enable_distributed_optimization,
            ))),
            RuleID::PushDownFilterUnion => Ok(Box::new(RulePushDownFilterUnion::new())),
            RuleID::PushDownFilterEvalScalar => Ok(Box::new(RulePushDownFilterEvalScalar::new())),
            RuleID::PushDownFilterJoin => Ok(Box::new(RulePushDownFilterJoin::new(ctx.metadata))),
            RuleID::PushDownFilterScan => Ok(Box::new(RulePushDownFilterScan::new(ctx.metadata))),
            RuleID::PushDownFilterSort => Ok(Box::new(RulePushDownFilterSort::new())),
            RuleID::PushDownFilterProjectSet => Ok(Box::new(RulePushDownFilterProjectSet::new())),
            RuleID::PushDownLimit => Ok(Box::new(RulePushDownLimit::new(ctx.metadata))),
            RuleID::PushDownLimitUnion => Ok(Box::new(RulePushDownLimitUnion::new())),
            RuleID::PushDownLimitScan => Ok(Box::new(RulePushDownLimitScan::new())),
            RuleID::PushDownSortScan => Ok(Box::new(RulePushDownSortScan::new())),
            RuleID::PushDownSortEvalScalar => {
                Ok(Box::new(RulePushDownSortEvalScalar::new(ctx.metadata)))
            }
            RuleID::PushDownLimitOuterJoin => Ok(Box::new(RulePushDownLimitOuterJoin::new())),
            RuleID::PushDownLimitEvalScalar => Ok(Box::new(RulePushDownLimitEvalScalar::new())),
            RuleID::PushDownLimitSort => {
                Ok(Box::new(RulePushDownLimitSort::new(MAX_PUSH_DOWN_LIMIT)))
            }
            RuleID::PushDownLimitWindow => {
                Ok(Box::new(RulePushDownLimitWindow::new(MAX_PUSH_DOWN_LIMIT)))
            }
            RuleID::RulePushDownRankLimitAggregate => {
                Ok(Box::new(RulePushDownRankLimitAggregate::new()))
            }
            RuleID::PushDownFilterAggregate => Ok(Box::new(RulePushDownFilterAggregate::new())),
            RuleID::PushDownFilterWindow => Ok(Box::new(RulePushDownFilterWindow::new())),
            RuleID::PushDownFilterWindowRank => Ok(Box::new(RulePushDownFilterWindowTopN::new())),
            RuleID::EliminateFilter => Ok(Box::new(RuleEliminateFilter::new(ctx.metadata))),
            RuleID::MergeEvalScalar => Ok(Box::new(RuleMergeEvalScalar::new())),
            RuleID::MergeFilter => Ok(Box::new(RuleMergeFilter::new())),
            RuleID::NormalizeScalarFilter => Ok(Box::new(RuleNormalizeScalarFilter::new())),
            RuleID::SplitAggregate => Ok(Box::new(RuleSplitAggregate::new())),
            RuleID::FoldCountAggregate => Ok(Box::new(RuleFoldCountAggregate::new())),
            RuleID::CommuteJoin => Ok(Box::new(RuleCommuteJoin::new())),
            RuleID::CommuteJoinBaseTable => Ok(Box::new(RuleCommuteJoinBaseTable::new())),
            RuleID::LeftExchangeJoin => Ok(Box::new(RuleLeftExchangeJoin::new())),
            RuleID::EagerAggregation => Ok(Box::new(RuleEagerAggregation::new(ctx.metadata))),
            RuleID::PushDownPrewhere => Ok(Box::new(RulePushDownPrewhere::new(ctx.metadata))),
            RuleID::TryApplyAggIndex => Ok(Box::new(RuleTryApplyAggIndex::new(ctx.metadata))),
            RuleID::EliminateSort => Ok(Box::new(RuleEliminateSort::new())),
            RuleID::SemiToInnerJoin => Ok(Box::new(RuleSemiToInnerJoin::new())),
        }
    }
}
