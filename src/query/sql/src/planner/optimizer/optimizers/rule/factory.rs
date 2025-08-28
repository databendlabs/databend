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

use std::sync::Arc;

use databend_common_exception::Result;

use crate::optimizer::optimizers::rule::RuleCommuteJoin;
use crate::optimizer::optimizers::rule::RuleCommuteJoinBaseTable;
use crate::optimizer::optimizers::rule::RuleDeduplicateSort;
use crate::optimizer::optimizers::rule::RuleEagerAggregation;
use crate::optimizer::optimizers::rule::RuleEliminateEvalScalar;
use crate::optimizer::optimizers::rule::RuleEliminateFilter;
use crate::optimizer::optimizers::rule::RuleEliminateSort;
use crate::optimizer::optimizers::rule::RuleEliminateUnion;
use crate::optimizer::optimizers::rule::RuleFilterFlattenOr;
use crate::optimizer::optimizers::rule::RuleFilterNulls;
use crate::optimizer::optimizers::rule::RuleFoldCountAggregate;
use crate::optimizer::optimizers::rule::RuleGroupingSetsToUnion;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::RuleLeftExchangeJoin;
use crate::optimizer::optimizers::rule::RuleMergeEvalScalar;
use crate::optimizer::optimizers::rule::RuleMergeFilter;
use crate::optimizer::optimizers::rule::RuleMergeFilterIntoMutation;
use crate::optimizer::optimizers::rule::RuleNormalizeScalarFilter;
use crate::optimizer::optimizers::rule::RulePtr;
use crate::optimizer::optimizers::rule::RulePushDownFilterAggregate;
use crate::optimizer::optimizers::rule::RulePushDownFilterEvalScalar;
use crate::optimizer::optimizers::rule::RulePushDownFilterJoin;
use crate::optimizer::optimizers::rule::RulePushDownFilterProjectSet;
use crate::optimizer::optimizers::rule::RulePushDownFilterScan;
use crate::optimizer::optimizers::rule::RulePushDownFilterSort;
use crate::optimizer::optimizers::rule::RulePushDownFilterUnion;
use crate::optimizer::optimizers::rule::RulePushDownFilterWindow;
use crate::optimizer::optimizers::rule::RulePushDownFilterWindowTopN;
use crate::optimizer::optimizers::rule::RulePushDownLimit;
use crate::optimizer::optimizers::rule::RulePushDownLimitEvalScalar;
use crate::optimizer::optimizers::rule::RulePushDownLimitOuterJoin;
use crate::optimizer::optimizers::rule::RulePushDownLimitScan;
use crate::optimizer::optimizers::rule::RulePushDownLimitSort;
use crate::optimizer::optimizers::rule::RulePushDownLimitUnion;
use crate::optimizer::optimizers::rule::RulePushDownLimitWindow;
use crate::optimizer::optimizers::rule::RulePushDownPrewhere;
use crate::optimizer::optimizers::rule::RulePushDownRankLimitAggregate;
use crate::optimizer::optimizers::rule::RulePushDownSortEvalScalar;
use crate::optimizer::optimizers::rule::RulePushDownSortFilterScan;
use crate::optimizer::optimizers::rule::RulePushDownSortScan;
use crate::optimizer::optimizers::rule::RuleSemiToInnerJoin;
use crate::optimizer::optimizers::rule::RuleSplitAggregate;
use crate::optimizer::optimizers::rule::RuleTryApplyAggIndex;
use crate::optimizer::OptimizerContext;

pub struct RuleFactory;

impl RuleFactory {
    pub fn create_rule(id: RuleID, ctx: Arc<OptimizerContext>) -> Result<RulePtr> {
        let metadata = ctx.get_metadata();
        match id {
            RuleID::EliminateUnion => Ok(Box::new(RuleEliminateUnion::new(metadata))),
            RuleID::EliminateEvalScalar => Ok(Box::new(RuleEliminateEvalScalar::new(metadata))),
            RuleID::FilterNulls => Ok(Box::new(RuleFilterNulls::new(
                ctx.get_enable_distributed_optimization(),
            ))),
            RuleID::FilterFlattenOr => Ok(Box::new(RuleFilterFlattenOr::new())),
            RuleID::PushDownFilterUnion => Ok(Box::new(RulePushDownFilterUnion::new())),
            RuleID::PushDownFilterEvalScalar => Ok(Box::new(RulePushDownFilterEvalScalar::new())),
            RuleID::PushDownFilterJoin => Ok(Box::new(RulePushDownFilterJoin::new(metadata))),
            RuleID::PushDownFilterScan => Ok(Box::new(RulePushDownFilterScan::new(metadata))),
            RuleID::PushDownFilterSort => Ok(Box::new(RulePushDownFilterSort::new())),
            RuleID::PushDownFilterProjectSet => Ok(Box::new(RulePushDownFilterProjectSet::new())),
            RuleID::PushDownLimit => Ok(Box::new(RulePushDownLimit::new(metadata))),
            RuleID::PushDownLimitUnion => Ok(Box::new(RulePushDownLimitUnion::new())),
            RuleID::PushDownLimitScan => Ok(Box::new(RulePushDownLimitScan::new())),
            RuleID::PushDownSortScan => Ok(Box::new(RulePushDownSortScan::new())),
            RuleID::PushDownSortFilterScan => Ok(Box::new(RulePushDownSortFilterScan::new())),
            RuleID::PushDownSortEvalScalar => {
                Ok(Box::new(RulePushDownSortEvalScalar::new(metadata)))
            }
            RuleID::PushDownLimitOuterJoin => Ok(Box::new(RulePushDownLimitOuterJoin::new())),
            RuleID::PushDownLimitEvalScalar => Ok(Box::new(RulePushDownLimitEvalScalar::new())),
            RuleID::PushDownLimitSort => Ok(Box::new(RulePushDownLimitSort::new(
                ctx.get_max_push_down_limit(),
            ))),
            RuleID::PushDownLimitWindow => Ok(Box::new(RulePushDownLimitWindow::new(
                ctx.get_max_push_down_limit(),
            ))),
            RuleID::PushDownRankLimitAggregate => Ok(Box::new(
                RulePushDownRankLimitAggregate::new(ctx.get_max_push_down_limit()),
            )),
            RuleID::PushDownFilterAggregate => Ok(Box::new(RulePushDownFilterAggregate::new())),
            RuleID::PushDownFilterWindow => Ok(Box::new(RulePushDownFilterWindow::new())),
            RuleID::PushDownFilterWindowTopN => {
                Ok(Box::new(RulePushDownFilterWindowTopN::new(metadata)))
            }
            RuleID::EliminateFilter => Ok(Box::new(RuleEliminateFilter::new(metadata))),
            RuleID::MergeEvalScalar => Ok(Box::new(RuleMergeEvalScalar::new())),
            RuleID::MergeFilter => Ok(Box::new(RuleMergeFilter::new())),
            RuleID::NormalizeScalarFilter => Ok(Box::new(RuleNormalizeScalarFilter::new())),
            RuleID::GroupingSetsToUnion => Ok(Box::new(RuleGroupingSetsToUnion::new(ctx))),
            RuleID::SplitAggregate => Ok(Box::new(RuleSplitAggregate::new())),
            RuleID::FoldCountAggregate => Ok(Box::new(RuleFoldCountAggregate::new())),
            RuleID::CommuteJoin => Ok(Box::new(RuleCommuteJoin::new())),
            RuleID::CommuteJoinBaseTable => Ok(Box::new(RuleCommuteJoinBaseTable::new())),
            RuleID::LeftExchangeJoin => Ok(Box::new(RuleLeftExchangeJoin::new())),
            RuleID::EagerAggregation => Ok(Box::new(RuleEagerAggregation::new(metadata))),
            RuleID::PushDownPrewhere => Ok(Box::new(RulePushDownPrewhere::new(metadata))),
            RuleID::TryApplyAggIndex => Ok(Box::new(RuleTryApplyAggIndex::new(metadata))),
            RuleID::EliminateSort => Ok(Box::new(RuleEliminateSort::new())),
            RuleID::DeduplicateSort => Ok(Box::new(RuleDeduplicateSort::new())),
            RuleID::SemiToInnerJoin => Ok(Box::new(RuleSemiToInnerJoin::new())),
            RuleID::MergeFilterIntoMutation => {
                Ok(Box::new(RuleMergeFilterIntoMutation::new(metadata)))
            }
        }
    }
}
