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

use common_exception::Result;

use super::rewrite::RuleEliminateEvalScalar;
use super::rewrite::RuleNormalizeDisjunctiveFilter;
use super::rewrite::RuleNormalizeScalarFilter;
use super::rewrite::RulePushDownFilterEvalScalar;
use super::rewrite::RulePushDownFilterJoin;
use super::rewrite::RulePushDownFilterProject;
use super::transform::RuleCommuteJoin;
use crate::sql::optimizer::rule::rewrite::RuleEliminateFilter;
use crate::sql::optimizer::rule::rewrite::RuleEliminateProject;
use crate::sql::optimizer::rule::rewrite::RuleMergeEvalScalar;
use crate::sql::optimizer::rule::rewrite::RuleMergeFilter;
use crate::sql::optimizer::rule::rewrite::RuleMergeProject;
use crate::sql::optimizer::rule::rewrite::RulePushDownFilterScan;
use crate::sql::optimizer::rule::rewrite::RulePushDownLimitOuterJoin;
use crate::sql::optimizer::rule::rewrite::RulePushDownLimitProject;
use crate::sql::optimizer::rule::rewrite::RulePushDownLimitScan;
use crate::sql::optimizer::rule::rewrite::RulePushDownLimitSort;
use crate::sql::optimizer::rule::rewrite::RulePushDownSortScan;
use crate::sql::optimizer::rule::rewrite::RuleSplitAggregate;
use crate::sql::optimizer::rule::rule_implement_get::RuleImplementGet;
use crate::sql::optimizer::rule::rule_implement_hash_join::RuleImplementHashJoin;
use crate::sql::optimizer::rule::RuleID;
use crate::sql::optimizer::rule::RulePtr;

pub struct RuleFactory;

impl RuleFactory {
    pub fn create() -> Self {
        RuleFactory {}
    }

    pub fn create_rule(&self, id: RuleID) -> Result<RulePtr> {
        match id {
            RuleID::ImplementGet => Ok(Box::new(RuleImplementGet::new())),
            RuleID::ImplementHashJoin => Ok(Box::new(RuleImplementHashJoin::new())),
            RuleID::EliminateEvalScalar => Ok(Box::new(RuleEliminateEvalScalar::new())),
            RuleID::PushDownFilterProject => Ok(Box::new(RulePushDownFilterProject::new())),
            RuleID::PushDownFilterEvalScalar => Ok(Box::new(RulePushDownFilterEvalScalar::new())),
            RuleID::PushDownFilterJoin => Ok(Box::new(RulePushDownFilterJoin::new())),
            RuleID::PushDownFilterScan => Ok(Box::new(RulePushDownFilterScan::new())),
            RuleID::PushDownLimitProject => Ok(Box::new(RulePushDownLimitProject::new())),
            RuleID::PushDownLimitScan => Ok(Box::new(RulePushDownLimitScan::new())),
            RuleID::PushDownSortScan => Ok(Box::new(RulePushDownSortScan::new())),
            RuleID::PushDownLimitOuterJoin => Ok(Box::new(RulePushDownLimitOuterJoin::new())),
            RuleID::PushDownLimitSort => Ok(Box::new(RulePushDownLimitSort::new())),
            RuleID::EliminateFilter => Ok(Box::new(RuleEliminateFilter::new())),
            RuleID::EliminateProject => Ok(Box::new(RuleEliminateProject::new())),
            RuleID::MergeProject => Ok(Box::new(RuleMergeProject::new())),
            RuleID::MergeEvalScalar => Ok(Box::new(RuleMergeEvalScalar::new())),
            RuleID::MergeFilter => Ok(Box::new(RuleMergeFilter::new())),
            RuleID::NormalizeScalarFilter => Ok(Box::new(RuleNormalizeScalarFilter::new())),
            RuleID::SplitAggregate => Ok(Box::new(RuleSplitAggregate::new())),
            RuleID::NormalizeDisjunctiveFilter => {
                Ok(Box::new(RuleNormalizeDisjunctiveFilter::new()))
            }
            RuleID::CommuteJoin => Ok(Box::new(RuleCommuteJoin::new())),
        }
    }
}
