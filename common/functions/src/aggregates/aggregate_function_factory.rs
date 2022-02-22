// Copyright 2021 Datafuse Labs.
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

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use once_cell::sync::Lazy;

use super::AggregateFunctionBasicAdaptor;
use super::AggregateFunctionCombinatorNull;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::Aggregators;

pub type AggregateFunctionCreator =
    Box<dyn Fn(&str, Vec<DataValue>, Vec<DataField>) -> Result<AggregateFunctionRef> + Sync + Send>;

pub type AggregateFunctionCombinatorCreator = Box<
    dyn Fn(
            &str,
            Vec<DataValue>,
            Vec<DataField>,
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
    pub(crate) properties: AggregateFunctionProperties,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct AggregateFunctionProperties {
    /** When the function is wrapped with Null combinator,
     * should we return Nullable type with NULL when no values were aggregated
     * or we should return non-Nullable type with default value (example: count, countDistinct).
     */
    pub(crate) returns_default_when_only_null: bool,
}

impl AggregateFunctionDescription {
    pub fn creator(creator: AggregateFunctionCreator) -> AggregateFunctionDescription {
        AggregateFunctionDescription {
            aggregate_function_creator: creator,
            properties: AggregateFunctionProperties {
                returns_default_when_only_null: false,
            },
        }
    }

    pub fn creator_with_properties(
        creator: AggregateFunctionCreator,
        properties: AggregateFunctionProperties,
    ) -> AggregateFunctionDescription {
        AggregateFunctionDescription {
            aggregate_function_creator: creator,
            properties,
        }
    }
}

#[allow(dead_code)]
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
        params: Vec<DataValue>,
        arguments: Vec<DataField>,
    ) -> Result<AggregateFunctionRef> {
        let name = name.as_ref();
        let mut properties = AggregateFunctionProperties::default();

        if !arguments.is_empty()
            && arguments
                .iter()
                .any(|f| f.is_nullable() || f.data_type().data_type_id() == TypeID::Null)
        {
            let new_params = AggregateFunctionCombinatorNull::transform_params(&params)?;
            let new_arguments = AggregateFunctionCombinatorNull::transform_arguments(&arguments)?;

            let nested = self.get_impl(name, new_params, new_arguments, &mut properties)?;
            let agg = AggregateFunctionCombinatorNull::try_create(
                name, params, arguments, nested, properties,
            )?;
            return Ok(AggregateFunctionBasicAdaptor::create(agg));
        }

        let agg = self.get_impl(name, params, arguments, &mut properties)?;
        Ok(AggregateFunctionBasicAdaptor::create(agg))
    }

    fn get_impl(
        &self,
        name: &str,
        params: Vec<DataValue>,
        arguments: Vec<DataField>,
        properties: &mut AggregateFunctionProperties,
    ) -> Result<AggregateFunctionRef> {
        let lowercase_name = name.to_lowercase();
        let aggregate_functions_map = &self.case_insensitive_desc;
        if let Some(desc) = aggregate_functions_map.get(&lowercase_name) {
            *properties = desc.properties;
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
                        *properties = nested_desc.properties;
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

    pub fn check(&self, name: impl AsRef<str>) -> bool {
        let origin = name.as_ref();
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

    pub fn registered_names(&self) -> Vec<String> {
        self.case_insensitive_desc.keys().cloned().collect()
    }
}
