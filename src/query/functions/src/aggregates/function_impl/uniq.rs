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

use super::AggregateRegistration;
use super::adaptors::*;
use super::count::create_distinct_count_function;

struct UniqBuilder;

impl UniqBuilder {
    fn register(registry: &mut AggregateRegistry) {
        Self::route().register(registry);
    }
}

inventory::submit! {
    AggregateRegistration {
        register: UniqBuilder::register,
    }
}

impl UniqBuilder {
    fn uniq_arguments() -> ArgumentsPattern {
        ArgumentsPattern::variadic(vec![], ArgumentPattern::any(), 1, Some(32))
    }

    const UNIQ_FEATURES: AggregateFeatures = AggregateFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "counts distinct non-null input rows",
        definition: "uniq(expr[, ...])",
        example: "select uniq(number) from numbers(10)",
    };
}

impl UniqBuilder {
    fn route() -> NameRoute {
        let arguments = Self::uniq_arguments();
        let features = Self::UNIQ_FEATURES;
        NameRoute::new(
            &["uniq"],
            arguments.clone(),
            features.clone(),
            NullPolicy::ReturnsDefaultWhenOnlyNull,
        )
        .with_validator(Self::validate_request)
        .then(MergeRoute::new(false, UniqBuilder::create))
        .then(MergeRoute::new(true, UniqBuilder::create))
        .then(PlainRoute::new(UniqBuilder::create))
        .then(IfRoute::direct(UniqBuilder::create))
        .then(StateRoute::direct(UniqBuilder::create))
    }

    fn validate_request(request: &RawAggregateCall<'_>) -> Result<()> {
        if request.params.is_empty() {
            Ok(())
        } else {
            Err(ErrorCode::BadArguments(format!(
                "{} expects no parameters",
                request.name
            )))
        }
    }

    fn create(build: DirectBuildContext<'_, impl Combinator>) -> Result<AggregateCallRef> {
        create_distinct_count_function(build, false)
    }
}
