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

use super::aggregate_approx_count_distinct::aggregate_approx_count_distinct_function_desc;
use super::aggregate_arg_min_max::aggregate_arg_max_function_desc;
use super::aggregate_arg_min_max::aggregate_arg_min_function_desc;
use super::aggregate_avg::aggregate_avg_function_desc;
use super::aggregate_bitmap::aggregate_bitmap_and_count_function_desc;
use super::aggregate_bitmap::aggregate_bitmap_intersect_count_function_desc;
use super::aggregate_bitmap::aggregate_bitmap_intersect_function_desc;
use super::aggregate_bitmap::aggregate_bitmap_not_count_function_desc;
use super::aggregate_bitmap::aggregate_bitmap_or_count_function_desc;
use super::aggregate_bitmap::aggregate_bitmap_union_function_desc;
use super::aggregate_bitmap::aggregate_bitmap_xor_count_function_desc;
use super::aggregate_combinator_distinct::aggregate_combinator_distinct_desc;
use super::aggregate_combinator_distinct::aggregate_combinator_uniq_desc;
use super::aggregate_combinator_state::AggregateStateCombinator;
use super::aggregate_covariance::aggregate_covariance_population_desc;
use super::aggregate_covariance::aggregate_covariance_sample_desc;
use super::aggregate_min_max_any::aggregate_any_function_desc;
use super::aggregate_min_max_any::aggregate_max_function_desc;
use super::aggregate_min_max_any::aggregate_min_function_desc;
use super::aggregate_stddev::aggregate_stddev_pop_function_desc;
use super::aggregate_stddev::aggregate_stddev_samp_function_desc;
use super::aggregate_window_funnel::aggregate_window_funnel_function_desc;
use super::AggregateCountFunction;
use super::AggregateFunctionFactory;
use super::AggregateIfCombinator;
use crate::aggregates::aggregate_array_agg::aggregate_array_agg_function_desc;
use crate::aggregates::aggregate_array_moving::aggregate_array_moving_avg_function_desc;
use crate::aggregates::aggregate_array_moving::aggregate_array_moving_sum_function_desc;
use crate::aggregates::aggregate_kurtosis::aggregate_kurtosis_function_desc;
use crate::aggregates::aggregate_quantile_cont::aggregate_median_function_desc;
use crate::aggregates::aggregate_quantile_cont::aggregate_quantile_cont_function_desc;
use crate::aggregates::aggregate_quantile_disc::aggregate_quantile_disc_function_desc;
use crate::aggregates::aggregate_quantile_tdigest::aggregate_median_tdigest_function_desc;
use crate::aggregates::aggregate_quantile_tdigest::aggregate_quantile_tdigest_function_desc;
use crate::aggregates::aggregate_retention::aggregate_retention_function_desc;
use crate::aggregates::aggregate_skewness::aggregate_skewness_function_desc;
use crate::aggregates::aggregate_string_agg::aggregate_string_agg_function_desc;
use crate::aggregates::aggregate_sum::aggregate_sum_function_desc;

pub struct Aggregators;

impl Aggregators {
    pub fn register(factory: &mut AggregateFunctionFactory) {
        // DatabendQuery always uses lowercase function names to get functions.
        factory.register("sum", aggregate_sum_function_desc());
        factory.register("count", AggregateCountFunction::desc());
        factory.register("avg", aggregate_avg_function_desc());
        factory.register("uniq", aggregate_combinator_uniq_desc());

        factory.register("min", aggregate_min_function_desc());
        factory.register("max", aggregate_max_function_desc());
        factory.register("any", aggregate_any_function_desc());
        factory.register("arg_min", aggregate_arg_min_function_desc());
        factory.register("arg_max", aggregate_arg_max_function_desc());

        factory.register("covar_samp", aggregate_covariance_sample_desc());
        factory.register("covar_pop", aggregate_covariance_population_desc());
        factory.register("stddev_samp", aggregate_stddev_samp_function_desc());
        factory.register("stddev_pop", aggregate_stddev_pop_function_desc());
        factory.register("stddev", aggregate_stddev_pop_function_desc());
        factory.register("std", aggregate_stddev_pop_function_desc());
        factory.register("quantile", aggregate_quantile_disc_function_desc());
        factory.register("quantile_disc", aggregate_quantile_disc_function_desc());
        factory.register("quantile_cont", aggregate_quantile_cont_function_desc());
        factory.register(
            "quantile_tdigest",
            aggregate_quantile_tdigest_function_desc(),
        );
        factory.register("median", aggregate_median_function_desc());
        factory.register("median_tdigest", aggregate_median_tdigest_function_desc());
        factory.register("window_funnel", aggregate_window_funnel_function_desc());
        factory.register(
            "approx_count_distinct",
            aggregate_approx_count_distinct_function_desc(),
        );
        factory.register("retention", aggregate_retention_function_desc());
        factory.register("array_agg", aggregate_array_agg_function_desc());
        factory.register("list", aggregate_array_agg_function_desc());
        factory.register(
            "group_array_moving_avg",
            aggregate_array_moving_avg_function_desc(),
        );
        factory.register(
            "group_array_moving_sum",
            aggregate_array_moving_sum_function_desc(),
        );
        factory.register("kurtosis", aggregate_kurtosis_function_desc());
        factory.register("skewness", aggregate_skewness_function_desc());
        factory.register("string_agg", aggregate_string_agg_function_desc());

        factory.register(
            "bitmap_and_count",
            aggregate_bitmap_and_count_function_desc(),
        );
        factory.register(
            "bitmap_not_count",
            aggregate_bitmap_not_count_function_desc(),
        );
        factory.register("bitmap_or_count", aggregate_bitmap_or_count_function_desc());
        factory.register(
            "bitmap_xor_count",
            aggregate_bitmap_xor_count_function_desc(),
        );
        factory.register("bitmap_union", aggregate_bitmap_union_function_desc());
        factory.register(
            "bitmap_intersect",
            aggregate_bitmap_intersect_function_desc(),
        );
        factory.register(
            "intersect_count",
            aggregate_bitmap_intersect_count_function_desc(),
        );
    }

    pub fn register_combinator(factory: &mut AggregateFunctionFactory) {
        factory.register_combinator("_if", AggregateIfCombinator::combinator_desc());
        factory.register_combinator("_distinct", aggregate_combinator_distinct_desc());
        factory.register_combinator("_state", AggregateStateCombinator::combinator_desc());
    }
}
