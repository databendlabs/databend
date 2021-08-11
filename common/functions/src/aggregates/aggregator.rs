// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use super::aggregate_arg_min_max::try_create_aggregate_arg_minmax_function;
use super::aggregate_avg::try_create_aggregate_avg_function;
use super::aggregate_min_max::try_create_aggregate_minmax_function;
use super::aggregate_sum::try_create_aggregate_sum_function;
use crate::aggregates::aggregate_function_factory::FactoryCombinatorFuncRef;
use crate::aggregates::aggregate_function_factory::FactoryFuncRef;
use crate::aggregates::AggregateCountFunction;
use crate::aggregates::AggregateDistinctCombinator;
use crate::aggregates::AggregateIfCombinator;

pub struct Aggregators;

impl Aggregators {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        // DatafuseQuery always uses lowercase function names to get functions.
        map.insert("count".into(), AggregateCountFunction::try_create);
        map.insert("sum".into(), try_create_aggregate_sum_function);
        map.insert("avg".into(), try_create_aggregate_avg_function);

        map.insert("min".into(), |display_name, arguments| {
            try_create_aggregate_minmax_function(true, display_name, arguments)
        });
        map.insert("max".into(), |display_name, arguments| {
            try_create_aggregate_minmax_function(false, display_name, arguments)
        });
        map.insert("argMin".into(), |display_name, arguments| {
            try_create_aggregate_arg_minmax_function(true, display_name, arguments)
        });
        map.insert("argMax".into(), |display_name, arguments| {
            try_create_aggregate_arg_minmax_function(false, display_name, arguments)
        });

        map.insert("uniq".into(), AggregateDistinctCombinator::try_create_uniq);
        Ok(())
    }

    pub fn register_combinator(map: FactoryCombinatorFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("distinct".into(), AggregateDistinctCombinator::try_create);
        map.insert("if".into(), AggregateIfCombinator::try_create);

        Ok(())
    }
}
