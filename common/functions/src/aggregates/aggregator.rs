// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::aggregates::aggregate_function_factory::FactoryCombinatorFuncRef;
use crate::aggregates::aggregate_function_factory::FactoryFuncRef;
use crate::aggregates::AggregateArgMaxFunction;
use crate::aggregates::AggregateArgMinFunction;
use crate::aggregates::AggregateAvgFunction;
use crate::aggregates::AggregateCountFunction;
use crate::aggregates::AggregateDistinctCombinator;
use crate::aggregates::AggregateIfCombinator;
use crate::aggregates::AggregateMaxFunction;
use crate::aggregates::AggregateMinFunction;
use crate::aggregates::AggregateSumFunction;

pub struct Aggregators;

impl Aggregators {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        // FuseQuery always uses lowercase function names to get functions.
        map.insert("count", AggregateCountFunction::try_create);
        map.insert("sum", AggregateSumFunction::try_create);
        map.insert("min", AggregateMinFunction::try_create);
        map.insert("max", AggregateMaxFunction::try_create);
        map.insert("avg", AggregateAvgFunction::try_create);
        map.insert("argmin", AggregateArgMinFunction::try_create);
        map.insert("argmax", AggregateArgMaxFunction::try_create);

        map.insert("uniq", AggregateDistinctCombinator::try_create_uniq);

        Ok(())
    }

    pub fn register_combinator(map: FactoryCombinatorFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("distinct", AggregateDistinctCombinator::try_create);
        map.insert("if", AggregateIfCombinator::try_create);

        Ok(())
    }
}
