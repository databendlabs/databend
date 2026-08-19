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

use super::AggregateFunctionV2Factory;
use super::adaptors_v2 as v2;
use super::count::create_distinct_count_function;

struct UniqBuilder;

impl UniqBuilder {
    fn register(registry: &mut v2::AggregateFunctionRegistry) {
        Self::route().register(registry);
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
    fn route() -> v2::DirectNameRoute {
        let arguments = Self::uniq_arguments();
        let features = Self::UNIQ_FEATURES;
        v2::DirectNameRoute::new(
            &["uniq"],
            arguments.clone(),
            features.clone(),
            v2::NullPolicy::ReturnsDefaultWhenOnlyNull,
        )
        .with_validator(Self::validate_request)
        .then(v2::MergeRoute::new(false, UniqBuilder::create))
        .then(v2::MergeRoute::new(true, UniqBuilder::create))
        .then(v2::PlainRoute::new(UniqBuilder::create))
        .then(v2::IfRoute::new(UniqBuilder::create))
        .then(v2::StateRoute::new(UniqBuilder::create))
    }

    fn validate_request(request: &v2::AggregateFunctionRequest<'_>) -> Result<()> {
        if request.params.is_empty() {
            Ok(())
        } else {
            Err(ErrorCode::BadArguments(format!(
                "{} expects no parameters",
                request.name
            )))
        }
    }

    fn create(
        build: v2::DirectBuildContext<'_, impl v2::CombinatorImpl>,
    ) -> Result<v2::AggregateFunctionRef> {
        create_distinct_count_function(build, false)
    }
}
