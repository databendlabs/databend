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

use adaptors::*;
pub use aggregate_count::AggregateCountFunction;
pub use aggregate_function::*;
pub use aggregate_function_factory::*;
use aggregate_null_result::AggregateNullResultFunction;
use aggregate_quantile_cont::QuantileData;
pub use aggregate_sum::*;
use aggregate_unary::*;
use aggregator::Aggregators;
pub use aggregator_common::*;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::aggregate as aggregate_function;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;

trait StateSerde {
    fn serialize_type(_function_data: Option<&dyn FunctionData>) -> Vec<StateSerdeItem>;

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()>;

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()>;
}

impl FunctionData for DataType {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

mod adaptors;
mod aggregate_approx_count_distinct;
mod aggregate_arg_min_max;
mod aggregate_array_agg;
mod aggregate_array_moving;
mod aggregate_avg;
mod aggregate_bitmap;
mod aggregate_boolean;
mod aggregate_count;
mod aggregate_covariance;
mod aggregate_distinct_state;
mod aggregate_function_factory;
mod aggregate_histogram;
mod aggregate_json_array_agg;
mod aggregate_json_object_agg;
mod aggregate_kurtosis;
mod aggregate_markov_tarin;
mod aggregate_min_max_any;
mod aggregate_min_max_any_decimal;
mod aggregate_mode;
mod aggregate_null_result;
mod aggregate_quantile_cont;
mod aggregate_quantile_disc;
mod aggregate_quantile_tdigest;
mod aggregate_quantile_tdigest_weighted;
mod aggregate_range_bound;
mod aggregate_retention;
mod aggregate_scalar_state;
mod aggregate_skewness;
mod aggregate_st_collect;
mod aggregate_stddev;
mod aggregate_string_agg;
mod aggregate_sum;
mod aggregate_sum_zero;
mod aggregate_unary;
mod aggregate_window_funnel;
mod aggregator;
mod aggregator_common;
