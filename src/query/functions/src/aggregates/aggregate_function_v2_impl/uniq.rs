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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::AggregateFunctionDefinition;
use super::AggregateFunctionV2Factory;
use super::adaptors_v2 as v2;
use super::count::create_distinct_count_function;

struct UniqBuilder;

impl UniqBuilder {
    fn register(registry: &mut v2::AggregateFunctionRegistry) {
        let uniq = AggregateFunctionDefinition::new(
            "uniq",
            UniqBuilder::uniq_arguments(),
            UniqBuilder::UNIQ_FEATURES,
            UniqBuilder::try_create,
        );
        uniq.register_with_combinators(registry, false);
    }
}

inventory::submit! {
    AggregateFunctionV2Factory {
        register: UniqBuilder::register,
    }
}

impl UniqBuilder {
    fn uniq_arguments() -> v2::AggregateArgumentsPattern {
        v2::AggregateArgumentsPattern::variadic(
            vec![],
            v2::AggregateArgumentPattern::any(),
            1,
            Some(32),
        )
    }

    const UNIQ_FEATURES: v2::FunctionFeatures = v2::FunctionFeatures {
        is_decomposable: true,
        sort_policy: v2::SortPolicy::Unsupported,
        distinct_policy: v2::DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "counts distinct non-null input rows",
        definition: "uniq(expr[, ...])",
        example: "select uniq(number) from numbers(10)",
    };
}

impl UniqBuilder {
    fn try_create(request: v2::AggregateFunctionRequest<'_>) -> Result<v2::AggregateFunctionRef> {
        if !request.params.is_empty() {
            return Err(ErrorCode::BadArguments(format!(
                "{} expects no parameters",
                request.name
            )));
        }

        v2::build_default_name_route_with_direct_input(
            request,
            &["uniq"],
            Self::UNIQ_FEATURES,
            true,
            direct_aggregate_function_build_input_fns!(|build| {
                create_distinct_count_function(build, false)
            }),
        )
    }
}
