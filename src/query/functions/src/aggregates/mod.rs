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

mod aggregate_count;

mod aggregate_function_factory;

mod adaptors;
mod aggregate_approx_count_distinct;
mod aggregate_arg_min_max;
mod aggregate_array_agg;
mod aggregate_array_moving;
mod aggregate_avg;
mod aggregate_bitmap;
mod aggregate_combinator_distinct;
mod aggregate_combinator_if;
mod aggregate_combinator_state;
mod aggregate_covariance;
mod aggregate_distinct_state;
mod aggregate_histogram;
mod aggregate_json_array_agg;
mod aggregate_kurtosis;
mod aggregate_min_max_any;
mod aggregate_null_result;
mod aggregate_quantile_cont;
mod aggregate_quantile_disc;
mod aggregate_quantile_tdigest;
mod aggregate_quantile_tdigest_weighted;
mod aggregate_retention;
mod aggregate_scalar_state;
mod aggregate_skewness;
mod aggregate_stddev;
mod aggregate_string_agg;
mod aggregate_sum;
mod aggregate_unary;
mod aggregate_window_funnel;
mod aggregator;
mod aggregator_common;

pub use adaptors::*;
pub use aggregate_arg_min_max::AggregateArgMinMaxFunction;
pub use aggregate_array_agg::*;
pub use aggregate_array_moving::*;
pub use aggregate_combinator_distinct::AggregateDistinctCombinator;
pub use aggregate_combinator_if::AggregateIfCombinator;
pub use aggregate_count::AggregateCountFunction;
pub use aggregate_covariance::AggregateCovarianceFunction;
pub use aggregate_function::*;
pub use aggregate_function_factory::AggregateFunctionFactory;
pub use aggregate_histogram::*;
pub use aggregate_json_array_agg::*;
pub use aggregate_kurtosis::*;
pub use aggregate_min_max_any::*;
pub use aggregate_null_result::AggregateNullResultFunction;
pub use aggregate_quantile_cont::*;
pub use aggregate_quantile_disc::*;
pub use aggregate_quantile_tdigest::*;
pub use aggregate_quantile_tdigest_weighted::*;
pub use aggregate_retention::*;
pub use aggregate_skewness::*;
pub use aggregate_string_agg::*;
pub use aggregate_sum::*;
pub use aggregate_unary::*;
pub use aggregator::Aggregators;
pub use aggregator_common::*;
pub use databend_common_expression::aggregate as aggregate_function;
