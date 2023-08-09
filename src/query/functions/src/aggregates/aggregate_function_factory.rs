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

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::Scalar;
use once_cell::sync::Lazy;

use super::AggregateFunctionCombinatorNull;
use super::AggregateFunctionOrNullAdaptor;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::Aggregators;

pub type AggregateFunctionCreator =
    Box<dyn Fn(&str, Vec<Scalar>, Vec<DataType>) -> Result<AggregateFunctionRef> + Sync + Send>;

pub type AggregateFunctionCombinatorCreator = Box<
    dyn Fn(
            &str,
            Vec<Scalar>,
            Vec<DataType>,
            &AggregateFunctionCreator,
        ) -> Result<AggregateFunctionRef>
        + Sync
        + Send,
>;

static FACTORY: Lazy<Arc<AggregateFunctionFactory>> = Lazy::new(|| {
    let mut factory = AggregateFunctionFactory::create();
    Aggregators::register(&mut factory);
    Aggregators::register_combinator(&mut factory);
    Arc::new(factory)
});

pub struct AggregateFunctionDescription {
    pub(crate) aggregate_function_creator: AggregateFunctionCreator,
    pub(crate) features: AggregateFunctionFeatures,
}

#[derive(Debug, Clone, Default)]
pub struct AggregateFunctionFeatures {
    /// When the function is wrapped with Null combinator,
    /// should we return Nullable type with NULL when no values were aggregated
    /// or we should return non-Nullable type with default value (example: count, count_distinct, approx_count_distinct)
    pub(crate) returns_default_when_only_null: bool,

    /// An aggregation function F is decomposable if there exist aggregation functions F1 and F2
    /// such that F(S1 ∪ S2) = F2(F1(S1), F1(S2)), where S1 and S2 are two sets of values.
    /// MAX and MIN are always decomposable:
    ///   MAX(S1 ∪ S2) = MAX(MAX(S1), MAX(S2))
    ///   MIN(S1 ∪ S2) = MIN(MIN(S1), MIN(S2))
    /// SUM and COUNT are decomposable when they contain no DISTINCT:
    ///   SUM(S1 ∪ S2) = SUM(SUM(S1), SUM(S2))
    ///   COUNT(S1 ∪ S2) = SUM(COUNT(S1), COUNT(S2))
    /// AVG(C) can be handled as SUM(C) and COUNT(C) and thus is decomposable.
    ///   AVG(C) = SUM(C) / COUNT(C)
    pub(crate) is_decomposable: bool,

    // Function Category
    pub category: &'static str,
    // Introduce the function in brief.
    pub description: &'static str,
    // The definition of the function.
    pub definition: &'static str,
    // Example SQL of the function that can be run directly in query.
    pub example: &'static str,
}

impl AggregateFunctionDescription {
    pub fn creator(creator: AggregateFunctionCreator) -> AggregateFunctionDescription {
        AggregateFunctionDescription {
            aggregate_function_creator: creator,
            features: AggregateFunctionFeatures {
                returns_default_when_only_null: false,
                is_decomposable: false,
                ..Default::default()
            },
        }
    }

    pub fn creator_with_features(
        creator: AggregateFunctionCreator,
        features: AggregateFunctionFeatures,
    ) -> AggregateFunctionDescription {
        AggregateFunctionDescription {
            aggregate_function_creator: creator,
            features,
        }
    }
}

pub struct CombinatorDescription {
    creator: AggregateFunctionCombinatorCreator,
    // TODO(Winter): function document, this is very interesting.
    // TODO(Winter): We can support the SHOW FUNCTION DOCUMENT `function_name` or MAN FUNCTION `function_name` query syntax.
}

impl CombinatorDescription {
    pub fn creator(creator: AggregateFunctionCombinatorCreator) -> CombinatorDescription {
        CombinatorDescription { creator }
    }
}

pub struct AggregateFunctionFactory {
    case_insensitive_desc: HashMap<String, AggregateFunctionDescription>,
    case_insensitive_combinator_desc: Vec<(String, CombinatorDescription)>,
}

impl AggregateFunctionFactory {
    pub(in crate::aggregates::aggregate_function_factory) fn create() -> AggregateFunctionFactory {
        AggregateFunctionFactory {
            case_insensitive_desc: Default::default(),
            case_insensitive_combinator_desc: Default::default(),
        }
    }

    pub fn instance() -> &'static AggregateFunctionFactory {
        FACTORY.as_ref()
    }

    pub fn register(&mut self, name: &str, desc: AggregateFunctionDescription) {
        let case_insensitive_desc = &mut self.case_insensitive_desc;
        case_insensitive_desc.insert(name.to_lowercase(), desc);
    }

    pub fn register_combinator(&mut self, suffix: &str, desc: CombinatorDescription) {
        for (exists_suffix, _) in &self.case_insensitive_combinator_desc {
            if exists_suffix.eq_ignore_ascii_case(suffix) {
                panic!(
                    "Logical error: {} combinator suffix already exists.",
                    suffix
                );
            }
        }

        let case_insensitive_combinator_desc = &mut self.case_insensitive_combinator_desc;
        case_insensitive_combinator_desc.push((suffix.to_lowercase(), desc));
    }

    pub fn get(
        &self,
        name: impl AsRef<str>,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
    ) -> Result<AggregateFunctionRef> {
        self.get_or_null(name, params, arguments, true)
    }

    pub fn get_or_null(
        &self,
        name: impl AsRef<str>,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
        or_null: bool,
    ) -> Result<AggregateFunctionRef> {
        let name = name.as_ref();
        let mut features = AggregateFunctionFeatures::default();
        // The NULL value in the array_agg function needs to be added to the returned array column,
        // so handled separately.
        if name == "array_agg"
            || name == "list"
            || name == "group_array_moving_avg"
            || name == "group_array_moving_sum"
        {
            let agg = self.get_impl(name, params, arguments, &mut features)?;
            return Ok(agg);
        }

        if !arguments.is_empty() && arguments.iter().any(|f| f.is_nullable_or_null()) {
            let new_params = AggregateFunctionCombinatorNull::transform_params(&params)?;
            let new_arguments = AggregateFunctionCombinatorNull::transform_arguments(&arguments)?;

            let nested = self.get_impl(name, new_params, new_arguments, &mut features)?;
            let agg = AggregateFunctionCombinatorNull::try_create(
                name,
                params,
                arguments,
                nested,
                features.clone(),
            )?;
            if or_null {
                return AggregateFunctionOrNullAdaptor::create(agg, features);
            } else {
                return Ok(agg);
            }
        }

        let agg = self.get_impl(name, params, arguments, &mut features)?;
        if or_null {
            AggregateFunctionOrNullAdaptor::create(agg, features)
        } else {
            Ok(agg)
        }
    }

    fn get_impl(
        &self,
        name: &str,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
        features: &mut AggregateFunctionFeatures,
    ) -> Result<AggregateFunctionRef> {
        let lowercase_name = name.to_lowercase();
        let aggregate_functions_map = &self.case_insensitive_desc;
        if let Some(desc) = aggregate_functions_map.get(&lowercase_name) {
            *features = desc.features.clone();
            return (desc.aggregate_function_creator)(name, params, arguments);
        }

        // find suffix
        for (suffix, desc) in &self.case_insensitive_combinator_desc {
            if let Some(nested_name) = lowercase_name.strip_suffix(suffix) {
                let aggregate_functions_map = &self.case_insensitive_desc;

                match aggregate_functions_map.get(nested_name) {
                    None => {
                        break;
                    }
                    Some(nested_desc) => {
                        *features = nested_desc.features.clone();
                        if suffix == "_state" {
                            features.returns_default_when_only_null = true;
                        }
                        return (desc.creator)(
                            nested_name,
                            params,
                            arguments,
                            &nested_desc.aggregate_function_creator,
                        );
                    }
                }
            }
        }

        Err(ErrorCode::UnknownAggregateFunction(format!(
            "Unsupported AggregateFunction: {}",
            name
        )))
    }

    pub fn contains(&self, func_name: impl AsRef<str>) -> bool {
        let origin = func_name.as_ref();
        let lowercase_name = origin.to_lowercase();

        if self.case_insensitive_desc.contains_key(&lowercase_name) {
            return true;
        }

        // find suffix
        for (suffix, _) in &self.case_insensitive_combinator_desc {
            if let Some(nested_name) = lowercase_name.strip_suffix(suffix) {
                if self.case_insensitive_desc.contains_key(nested_name) {
                    return true;
                }
            }
        }

        false
    }

    pub fn is_decomposable(&self, func_name: impl AsRef<str>) -> bool {
        let origin = func_name.as_ref();
        let lowercase_name = origin.to_lowercase();

        self.case_insensitive_desc
            .get(&lowercase_name)
            .is_some_and(|desc| desc.features.is_decomposable)
    }

    pub fn registered_names(&self) -> Vec<String> {
        self.case_insensitive_desc.keys().cloned().collect()
    }

    pub fn registered_features(&self) -> Vec<AggregateFunctionFeatures> {
        self.case_insensitive_desc
            .values()
            .map(|v| &v.features)
            .cloned()
            .collect::<Vec<_>>()
    }
}
