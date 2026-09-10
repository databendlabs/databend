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

use databend_common_expression::Scalar;
use databend_common_expression::aggregate_function::AggregateFeatures;
use databend_common_expression::aggregate_function::DistinctPolicy;
use databend_common_expression::aggregate_function::EagerAggregation;
use databend_common_expression::aggregate_function::SortPolicy;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;

/// Intrinsic metadata used to construct a base aggregate or a concrete variant.
/// FILTER support and DISTINCT resolution are derived from the registered routes,
/// so they cannot be specified here.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct AggregateMetadata {
    pub eager_aggregation: EagerAggregation,
    pub sort_policy: SortPolicy,
    /// Result for the pure NULL argument shortcut, not an empty-input policy.
    pub null_argument_result: NullArgumentResult,
    pub documentation: AggregateDocumentation,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct AggregateDocumentation {
    pub category: &'static str,
    pub description: &'static str,
    pub definition: &'static str,
    pub example: &'static str,
}

impl AggregateMetadata {
    /// Publish intrinsic metadata at the external descriptor or call boundary.
    pub(in super::super) fn into_features(self) -> AggregateFeatures {
        AggregateFeatures {
            eager_aggregation: self.eager_aggregation,
            sort_policy: self.sort_policy,
            category: self.documentation.category,
            description: self.documentation.description,
            definition: self.documentation.definition,
            example: self.documentation.example,
            supports_filter: false,
            supports_state: false,
            distinct_policy: DistinctPolicy::Unsupported,
        }
    }
}

/// The declared scalar result when a pure NULL argument makes the aggregate constant.
/// Native input implementations may need to build normally instead of taking this shortcut.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum NullArgumentResult {
    #[default]
    Null,
    UInt64Zero,
}

impl NullArgumentResult {
    pub(in super::super) fn value(self) -> (DataType, Scalar) {
        match self {
            Self::Null => (DataType::Null, Scalar::Null),
            Self::UInt64Zero => (
                DataType::Number(NumberDataType::UInt64),
                Scalar::Number(NumberScalar::UInt64(0)),
            ),
        }
    }
}
