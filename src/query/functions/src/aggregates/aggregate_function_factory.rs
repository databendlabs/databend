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
use std::sync::LazyLock;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;

use super::AggregateFunctionCombinatorNull;
use super::AggregateFunctionOrNullAdaptor;
use super::AggregateFunctionRef;
use super::AggregateFunctionSortAdaptor;
use super::Aggregators;

const STATE_SUFFIX: &str = "_state";

pub type AggregateFunctionCreator = Box<
    dyn Fn(
            &str,
            Vec<Scalar>,
            Vec<DataType>,
            Vec<AggregateFunctionSortDesc>,
        ) -> Result<AggregateFunctionRef>
        + Sync
        + Send,
>;

pub type AggregateFunctionCombinatorCreator = Box<
    dyn Fn(
            &str,
            Vec<Scalar>,
            Vec<DataType>,
            Vec<AggregateFunctionSortDesc>,
            &AggregateFunctionCreator,
        ) -> Result<AggregateFunctionRef>
        + Sync
        + Send,
>;

static FACTORY: LazyLock<Arc<AggregateFunctionFactory>> = LazyLock::new(|| {
    let mut factory = AggregateFunctionFactory::create();
    Aggregators::register(&mut factory);
    Aggregators::register_combinator(&mut factory);
    Arc::new(factory)
});

pub struct AggregateFunctionDescription {
    pub(crate) creator: AggregateFunctionCreator,
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

    pub(crate) allow_sort: bool,

    // The NULL value in the those function needs to be handled separately.
    pub(crate) keep_nullable: bool,

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
            creator,
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
        AggregateFunctionDescription { creator, features }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug, serde::Serialize, serde::Deserialize)]
pub struct AggregateFunctionSortDesc {
    pub index: usize,
    pub is_reuse_index: bool,
    pub data_type: DataType,
    pub nulls_first: bool,
    pub asc: bool,
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
    pub(in super::aggregate_function_factory) fn create() -> AggregateFunctionFactory {
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

    pub fn register_multi_names(
        &mut self,
        names: &[&str],
        desc_c: impl Fn() -> AggregateFunctionDescription + Clone,
    ) {
        let case_insensitive_desc = &mut self.case_insensitive_desc;
        for name in names {
            case_insensitive_desc.insert(name.to_lowercase(), desc_c());
        }
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
        sort_descs: Vec<AggregateFunctionSortDesc>,
    ) -> Result<AggregateFunctionRef> {
        self.get_or_null(name, params, arguments, sort_descs, true)
    }

    pub fn get_or_null(
        &self,
        name: impl AsRef<str>,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
        sort_descs: Vec<AggregateFunctionSortDesc>,
        or_null: bool,
    ) -> Result<AggregateFunctionRef> {
        let name = name.as_ref().to_lowercase();

        if let Some(desc) = self.case_insensitive_desc.get(&name) {
            if desc.features.keep_nullable {
                let agg = (desc.creator)(&name, params, arguments, sort_descs.clone())?;
                return if desc.features.allow_sort {
                    AggregateFunctionSortAdaptor::create(agg, sort_descs)
                } else {
                    Ok(agg)
                };
            }

            if arguments.iter().all(|f| !f.is_nullable_or_null()) {
                let mut agg = (desc.creator)(&name, params, arguments, sort_descs.clone())?;
                if desc.features.allow_sort {
                    agg = AggregateFunctionSortAdaptor::create(agg, sort_descs)?
                }
                return if or_null && !desc.features.returns_default_when_only_null {
                    AggregateFunctionOrNullAdaptor::create(agg)
                } else {
                    Ok(agg)
                };
            }

            let new_arguments = AggregateFunctionCombinatorNull::transform_arguments(&arguments)?;

            let nested = (desc.creator)(&name, params.clone(), new_arguments, sort_descs.clone())?;
            let mut agg = AggregateFunctionCombinatorNull::try_create(
                params,
                arguments,
                nested,
                desc.features.returns_default_when_only_null,
            )?;
            if desc.features.allow_sort {
                agg = AggregateFunctionSortAdaptor::create(agg, sort_descs)?
            }
            return if or_null && !desc.features.returns_default_when_only_null {
                AggregateFunctionOrNullAdaptor::create(agg)
            } else {
                Ok(agg)
            };
        }

        // find suffix
        let Some((nested_name, suffix, nested_desc, desc)) = self
            .case_insensitive_combinator_desc
            .iter()
            .find_map(|(suffix, desc)| {
                name.strip_suffix(suffix)
                    .map(|nested_name| (nested_name, suffix, desc))
            })
            .and_then(|(nested_name, suffix, desc)| {
                self.case_insensitive_desc
                    .get(nested_name)
                    .map(|nested_desc| (nested_name, suffix, nested_desc, desc))
            })
        else {
            return Err(ErrorCode::UnknownAggregateFunction(format!(
                "Unsupported AggregateFunction: {name}",
            )));
        };

        let (nested, features) = if suffix == STATE_SUFFIX {
            let nested = (*desc.creator)(
                nested_name,
                params.clone(),
                arguments.clone(),
                sort_descs.clone(),
                &nested_desc.creator,
            )?;
            let mut features = nested_desc.features.clone();
            features.returns_default_when_only_null = true;
            (nested, features)
        } else {
            let new_arguments = AggregateFunctionCombinatorNull::transform_arguments(&arguments)?;

            let nested = (*desc.creator)(
                nested_name,
                params.clone(),
                new_arguments,
                sort_descs.clone(),
                &nested_desc.creator,
            )?;
            (nested, nested_desc.features.clone())
        };

        let mut agg = AggregateFunctionCombinatorNull::try_create(
            params,
            arguments,
            nested,
            features.returns_default_when_only_null,
        )?;
        if features.allow_sort {
            agg = AggregateFunctionSortAdaptor::create(agg, sort_descs)?
        }
        if or_null && !features.returns_default_when_only_null {
            AggregateFunctionOrNullAdaptor::create(agg)
        } else {
            Ok(agg)
        }
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
