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

pub(super) mod adaptors_v2;

macro_rules! unary_aggregate_function_build_input_fns {
    ($build:expr) => {
        v2::UnaryAggregateFunctionBuildInputFns::new(
            $build,
            $build,
            $build,
            v2::UnaryDistinctBuildFn::Combinator(v2::unsupported_unary_distinct),
        )
    };
}

macro_rules! unary_aggregate_function_build_input_fns_with_distinct {
    ($build:expr) => {
        v2::UnaryAggregateFunctionBuildInputFns::new(
            $build,
            $build,
            $build,
            v2::UnaryDistinctBuildFn::Combinator($build),
        )
    };
}

macro_rules! multi_arg_aggregate_function_build_input_fns {
    ($build:expr) => {
        v2::MultiArgAggregateFunctionBuildInputFns::new(
            $build,
            $build,
            $build,
            v2::MultiArgDistinctBuildFn::Combinator(v2::unsupported_multi_arg_distinct),
        )
    };
}

macro_rules! multi_arg_aggregate_function_build_input_fns_with_distinct {
    ($build:expr) => {
        v2::MultiArgAggregateFunctionBuildInputFns::new(
            $build,
            $build,
            $build,
            v2::MultiArgDistinctBuildFn::Combinator($build),
        )
    };
}

macro_rules! direct_aggregate_function_build_input_fns {
    ($build:expr) => {
        v2::DirectAggregateFunctionBuildInputFns::new(
            $build,
            $build,
            $build,
            v2::DirectDistinctBuildFn::Combinator(v2::unsupported_direct_distinct),
        )
    };
}

mod aggregate_function_definition;
mod approx_count_distinct;
mod arg_min_max;
mod array_agg;
mod array_moving;
mod avg;
mod bitmap;
mod boolean;
mod count;
mod covariance;
mod geographic;
mod histogram;
mod json_array_agg;
mod json_object_agg;
mod markov_train;
mod min_max_any;
mod mode;
mod moments;
mod quantile_cont;
mod quantile_disc;
mod quantile_tdigest;
mod quantile_tdigest_weighted;
mod range_bound;
mod retention;
mod stddev;
mod string_agg;
mod sum;
mod uniq;
mod window_funnel;

use databend_common_expression::BlockEntry;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::DataType;

use self::adaptors_v2 as v2;
use self::aggregate_function_definition::AggregateFunctionDefinition;

fn aggregate_function_signature(
    request: v2::AggregateFunctionRequest<'_>,
    return_type: DataType,
) -> v2::AggregateFunctionSignature {
    v2::AggregateFunctionSignature {
        name: request.name.to_string(),
        params: request.params.to_vec(),
        args_type: request.args_type.to_vec(),
        distinct: request.distinct,
        order_by: request.order_by.to_vec(),
        return_type,
    }
}

struct AggregateFunctionV2Factory {
    register: fn(&mut v2::AggregateFunctionRegistry),
}

inventory::collect!(AggregateFunctionV2Factory);

pub(super) fn register_functions(registry: &mut v2::AggregateFunctionRegistry) {
    for factory in inventory::iter::<AggregateFunctionV2Factory> {
        (factory.register)(registry);
    }
}

fn serialized_scalar_at(state: &BlockEntry, row: usize, field: usize) -> ScalarRef<'_> {
    let scalar = state.index(row).unwrap();
    match scalar {
        ScalarRef::Tuple(fields) => fields[field].clone(),
        _ => {
            debug_assert_eq!(field, 0);
            scalar
        }
    }
}
