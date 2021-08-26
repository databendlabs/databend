// Copyright 2020 Datafuse Labs.
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

use super::aggregate_arg_min_max::try_create_aggregate_arg_minmax_function;
use super::aggregate_avg::try_create_aggregate_avg_function;
use super::aggregate_min_max::try_create_aggregate_minmax_function;
use super::aggregate_sum::try_create_aggregate_sum_function;
use super::aggregate_window_funnel::try_create_aggregate_window_funnel_function;
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

        map.insert("min".into(), |display_name, params, arguments| {
            try_create_aggregate_minmax_function(true, display_name, params, arguments)
        });
        map.insert("max".into(), |display_name, params, arguments| {
            try_create_aggregate_minmax_function(false, display_name, params, arguments)
        });
        map.insert("argMin".into(), |display_name, params, arguments| {
            try_create_aggregate_arg_minmax_function(true, display_name, params, arguments)
        });
        map.insert("argMax".into(), |display_name, params, arguments| {
            try_create_aggregate_arg_minmax_function(false, display_name, params, arguments)
        });

        map.insert(
            "windowFunnel".into(),
            try_create_aggregate_window_funnel_function,
        );

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
