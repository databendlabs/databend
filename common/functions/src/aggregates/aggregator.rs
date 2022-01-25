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

use super::AggregateCountFunction;
use super::AggregateFunctionFactory;
use crate::aggregates::aggregate_sum::aggregate_sum_function_desc;

pub struct Aggregators;

impl Aggregators {
    // pub fn register(factory: &mut AggregateFunctionFactory) {
    //     // DatabendQuery always uses lowercase function names to get functions.
    //     factory.register("avg", aggregate_avg_function_desc());
    //     factory.register("min", aggregate_min_function_desc());
    //     factory.register("max", aggregate_max_function_desc());
    //     factory.register("argMin", aggregate_arg_min_function_desc());
    //     factory.register("argMax", aggregate_arg_max_function_desc());
    //     factory.register("std", aggregate_stddev_pop_function_desc());
    //     factory.register("stddev", aggregate_stddev_pop_function_desc());
    //     factory.register("stddev_pop", aggregate_stddev_pop_function_desc());
    //     factory.register("windowFunnel", aggregate_window_funnel_function_desc());
    //     factory.register("uniq", AggregateDistinctCombinator::uniq_desc());
    //     factory.register("covar_samp", aggregate_covariance_sample_desc());
    //     factory.register("covar_pop", aggregate_covariance_population_desc());
    // }

    // pub fn register_combinator(factory: &mut AggregateFunctionFactory) {
    //     factory.register_combinator("if", AggregateIfCombinator::combinator_desc());
    //     factory.register_combinator("distinct", AggregateDistinctCombinator::combinator_desc());
    // }

    pub fn register(factory: &mut AggregateFunctionFactory) {
        factory.register("sum", aggregate_sum_function_desc());
        factory.register("count", AggregateCountFunction::desc());
    }

    pub fn register_combinator(_factory: &mut AggregateFunctionFactory) {}
}
