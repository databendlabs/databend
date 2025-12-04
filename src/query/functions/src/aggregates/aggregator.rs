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

use super::adaptors::*;
use super::aggregate_approx_count_distinct::aggregate_approx_count_distinct_function_desc;
use super::aggregate_arg_min_max::aggregate_arg_max_function_desc;
use super::aggregate_arg_min_max::aggregate_arg_min_function_desc;
use super::aggregate_array_agg::aggregate_array_agg_function_desc;
use super::aggregate_array_moving::aggregate_array_moving_avg_function_desc;
use super::aggregate_array_moving::aggregate_array_moving_sum_function_desc;
use super::aggregate_avg::aggregate_avg_function_desc;
use super::aggregate_bitmap::aggregate_bitmap_and_count_function_desc;
use super::aggregate_bitmap::aggregate_bitmap_construct_agg_function_desc;
use super::aggregate_bitmap::aggregate_bitmap_intersect_count_function_desc;
use super::aggregate_bitmap::aggregate_bitmap_intersect_function_desc;
use super::aggregate_bitmap::aggregate_bitmap_not_count_function_desc;
use super::aggregate_bitmap::aggregate_bitmap_or_count_function_desc;
use super::aggregate_bitmap::aggregate_bitmap_union_function_desc;
use super::aggregate_bitmap::aggregate_bitmap_xor_count_function_desc;
use super::aggregate_boolean::aggregate_boolean_function_desc;
use super::aggregate_covariance::aggregate_covariance_population_desc;
use super::aggregate_covariance::aggregate_covariance_sample_desc;
use super::aggregate_histogram::aggregate_histogram_function_desc;
use super::aggregate_json_array_agg::aggregate_json_array_agg_function_desc;
use super::aggregate_json_object_agg::aggregate_json_object_agg_function_desc;
use super::aggregate_kurtosis::aggregate_kurtosis_function_desc;
use super::aggregate_markov_tarin::aggregate_markov_train_function_desc;
use super::aggregate_min_max_any::aggregate_any_function_desc;
use super::aggregate_min_max_any::aggregate_max_function_desc;
use super::aggregate_min_max_any::aggregate_min_function_desc;
use super::aggregate_mode::aggregate_mode_function_desc;
use super::aggregate_quantile_cont::aggregate_median_function_desc;
use super::aggregate_quantile_cont::aggregate_quantile_cont_function_desc;
use super::aggregate_quantile_disc::aggregate_quantile_disc_function_desc;
use super::aggregate_quantile_tdigest::aggregate_median_tdigest_function_desc;
use super::aggregate_quantile_tdigest::aggregate_quantile_tdigest_function_desc;
use super::aggregate_quantile_tdigest_weighted::aggregate_median_tdigest_weighted_function_desc;
use super::aggregate_quantile_tdigest_weighted::aggregate_quantile_tdigest_weighted_function_desc;
use super::aggregate_range_bound::aggregate_range_bound_function_desc;
use super::aggregate_retention::aggregate_retention_function_desc;
use super::aggregate_skewness::aggregate_skewness_function_desc;
use super::aggregate_st_collect::aggregate_st_collect_function_desc;
use super::aggregate_stddev::aggregate_stddev_pop_function_desc;
use super::aggregate_stddev::aggregate_stddev_samp_function_desc;
use super::aggregate_string_agg::aggregate_string_agg_function_desc;
use super::aggregate_sum::aggregate_sum_function_desc;
use super::aggregate_sum_zero::AggregateSumZeroFunction;
use super::aggregate_window_funnel::aggregate_window_funnel_function_desc;
use super::AggregateCountFunction;
use super::AggregateFunctionFactory;
use super::AggregateIfCombinator;
use super::AggregateStateCombinator;

pub struct Aggregators;

impl Aggregators {
    pub fn register(factory: &mut AggregateFunctionFactory) {
        // DatabendQuery always uses lowercase function names to get functions.
        factory.register("sum", aggregate_sum_function_desc());
        factory.register("count", AggregateCountFunction::desc());
        factory.register("sum0", AggregateSumZeroFunction::desc());
        factory.register("sum_zero", AggregateSumZeroFunction::desc());
        factory.register("avg", aggregate_avg_function_desc());
        factory.register("uniq", aggregate_uniq_desc());
        factory.register("count_distinct", aggregate_count_distinct_desc());

        factory.register("min", aggregate_min_function_desc());
        factory.register("max", aggregate_max_function_desc());
        factory.register_multi_names(&["any", "any_value"], aggregate_any_function_desc);
        factory.register("arg_min", aggregate_arg_min_function_desc());
        factory.register("arg_max", aggregate_arg_max_function_desc());

        // booleans
        factory.register("bool_and", aggregate_boolean_function_desc::<true>());
        factory.register("bool_or", aggregate_boolean_function_desc::<false>());

        factory.register_multi_names(
            &["covar_samp", "var_samp", "variance_samp"],
            aggregate_covariance_sample_desc,
        );
        factory.register_multi_names(
            &["covar_pop", "var_pop", "variance_pop"],
            aggregate_covariance_population_desc,
        );

        factory.register("stddev_samp", aggregate_stddev_samp_function_desc());
        factory.register("stddev_pop", aggregate_stddev_pop_function_desc());
        factory.register("stddev", aggregate_stddev_samp_function_desc());
        factory.register("std", aggregate_stddev_pop_function_desc());
        factory.register("quantile", aggregate_quantile_disc_function_desc());
        factory.register("quantile_disc", aggregate_quantile_disc_function_desc());
        factory.register("quantile_cont", aggregate_quantile_cont_function_desc());
        factory.register(
            "quantile_tdigest",
            aggregate_quantile_tdigest_function_desc(),
        );
        factory.register(
            "quantile_tdigest_weighted",
            aggregate_quantile_tdigest_weighted_function_desc(),
        );
        factory.register("median", aggregate_median_function_desc());
        factory.register("median_tdigest", aggregate_median_tdigest_function_desc());
        factory.register(
            "median_tdigest_weighted",
            aggregate_median_tdigest_weighted_function_desc(),
        );
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
        factory.register("json_agg", aggregate_json_array_agg_function_desc());
        factory.register("json_array_agg", aggregate_json_array_agg_function_desc());
        factory.register("json_object_agg", aggregate_json_object_agg_function_desc());
        factory.register("kurtosis", aggregate_kurtosis_function_desc());
        factory.register("skewness", aggregate_skewness_function_desc());
        factory.register("string_agg", aggregate_string_agg_function_desc());
        factory.register("listagg", aggregate_string_agg_function_desc());
        factory.register("group_concat", aggregate_string_agg_function_desc());

        factory.register("range_bound", aggregate_range_bound_function_desc());

        factory.register_multi_names(
            &["bitmap_construct_agg", "group_bitmap"],
            aggregate_bitmap_construct_agg_function_desc,
        );
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

        factory.register("histogram", aggregate_histogram_function_desc());

        factory.register("mode", aggregate_mode_function_desc());

        factory.register("st_collect", aggregate_st_collect_function_desc());

        factory.register("markov_train", aggregate_markov_train_function_desc());
    }

    pub fn register_combinator(factory: &mut AggregateFunctionFactory) {
        factory.register_combinator("_if", AggregateIfCombinator::combinator_desc());
        factory.register_combinator("_distinct", aggregate_combinator_distinct_desc());
        factory.register_combinator("_state", AggregateStateCombinator::combinator_desc());
    }
}
