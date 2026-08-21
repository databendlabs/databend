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

mod adaptors;
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
use databend_common_expression::aggregate_function::AggregateFunctionRegistry;

struct FunctionFactory {
    register: fn(&mut AggregateFunctionRegistry),
}

inventory::collect!(FunctionFactory);

pub(super) fn register_functions(registry: &mut AggregateFunctionRegistry) {
    for factory in inventory::iter::<FunctionFactory> {
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
