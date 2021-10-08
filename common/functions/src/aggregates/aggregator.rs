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

use crate::aggregates::aggregate_arg_min_max::aggregate_arg_max_function_desc;
use crate::aggregates::aggregate_arg_min_max::aggregate_arg_min_function_desc;
use crate::aggregates::aggregate_avg::aggregate_avg_function_desc;
use crate::aggregates::aggregate_covariance::aggregate_covariance_population_desc;
use crate::aggregates::aggregate_covariance::aggregate_covariance_sample_desc;
use crate::aggregates::aggregate_function_factory::AggregateFunctionFactory;
use crate::aggregates::aggregate_min_max::aggregate_max_function_desc;
use crate::aggregates::aggregate_min_max::aggregate_min_function_desc;
use crate::aggregates::aggregate_stddev_pop::aggregate_stddev_pop_function_desc;
use crate::aggregates::aggregate_sum::aggregate_sum_function_desc;
use crate::aggregates::aggregate_window_funnel::aggregate_window_funnel_function_desc;
use crate::aggregates::AggregateCountFunction;
use crate::aggregates::AggregateDistinctCombinator;
use crate::aggregates::AggregateIfCombinator;

pub struct Aggregators;

impl Aggregators {
    pub fn register(factory: &mut AggregateFunctionFactory) {
        // DatabendQuery always uses lowercase function names to get functions.
        factory.register("count", AggregateCountFunction::desc());
        factory.register("sum", aggregate_sum_function_desc());
        factory.register("avg", aggregate_avg_function_desc());
        factory.register("min", aggregate_min_function_desc());
        factory.register("max", aggregate_max_function_desc());
        factory.register("argMin", aggregate_arg_min_function_desc());
        factory.register("argMax", aggregate_arg_max_function_desc());
        factory.register("std", aggregate_stddev_pop_function_desc());
        factory.register("stddev", aggregate_stddev_pop_function_desc());
        factory.register("stddev_pop", aggregate_stddev_pop_function_desc());
        factory.register("windowFunnel", aggregate_window_funnel_function_desc());
        factory.register("uniq", AggregateDistinctCombinator::uniq_desc());
        factory.register("covar_samp", aggregate_covariance_sample_desc());
        factory.register("covar_pop", aggregate_covariance_population_desc());
    }

    pub fn register_combinator(factory: &mut AggregateFunctionFactory) {
        factory.register_combinator("if", AggregateIfCombinator::combinator_desc());
        factory.register_combinator("distinct", AggregateDistinctCombinator::combinator_desc());
    }
}
