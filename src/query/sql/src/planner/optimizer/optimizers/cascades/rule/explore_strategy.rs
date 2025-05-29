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

use std::fmt::Debug;

use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::RuleSet;

/// ExploreStrategy trait defines the interface for rule exploration strategies
pub trait ExploreStrategy: Debug + Send {
    /// Create a rule set based on the strategy
    fn create_rule_set(&self) -> RuleSet;
}

/// DPhypStrategy implements the strategy for DPhyp optimization
#[derive(Debug)]
pub struct DPhypStrategy;

impl ExploreStrategy for DPhypStrategy {
    fn create_rule_set(&self) -> RuleSet {
        // The join order has been optimized by dphyp, therefore we will not change the join order
        // and only attempt to exchange the order of build and probe.
        RuleSet::create_with_ids(vec![
        // TODO(fixme): Disabled eager aggregation for now, we need to test it fully
        // RuleID::EagerAggregation
        ])
    }
}

/// RSL1Strategy implements the strategy for RS-L1 optimization
#[derive(Debug)]
pub struct RSL1Strategy;

impl ExploreStrategy for RSL1Strategy {
    fn create_rule_set(&self) -> RuleSet {
        // Get rule set of join order RS-L1, which will only generate left-deep trees.
        // Read paper "The Complexity of Transformation-Based Join Enumeration" for more details.
        RuleSet::create_with_ids(vec![
            RuleID::CommuteJoinBaseTable,
            RuleID::LeftExchangeJoin,
            // TODO(fixme): Disabled eager aggregation for now, we need to test it fully
            // RuleID::EagerAggregation
            RuleID::CommuteJoin,
        ])
    }
}

/// StrategyFactory creates appropriate exploration strategies
#[derive(Debug)]
pub struct StrategyFactory;

impl StrategyFactory {
    /// Create a strategy based on optimization status
    pub fn create_strategy(optimized: bool) -> Box<dyn ExploreStrategy> {
        if optimized {
            Box::new(DPhypStrategy)
        } else {
            Box::new(RSL1Strategy)
        }
    }
}
