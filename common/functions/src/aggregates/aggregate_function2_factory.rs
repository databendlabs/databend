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

use common_datavalues2::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use once_cell::sync::Lazy;

use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::Aggregators;

pub type AggregateFunction2Creator =
    Box<dyn Fn(&str, Vec<DataValue>, Vec<DataField>) -> Result<AggregateFunctionRef> + Sync + Send>;

pub type AggregateFunction2CombinatorCreator = Box<
    dyn Fn(
            &str,
            Vec<DataValue>,
            Vec<DataField>,
            &AggregateFunction2Creator,
        ) -> Result<AggregateFunctionRef>
        + Sync
        + Send,
>;

static FACTORY: Lazy<Arc<AggregateFunction2Factory>> = Lazy::new(|| {
    let mut factory = AggregateFunction2Factory::create();
    Aggregators::register2(&mut factory);
    Aggregators::register_combinator2(&mut factory);
    Arc::new(factory)
});

pub struct AggregateFunction2Description {
    aggregate_function_creator: AggregateFunction2Creator,
    // TODO(Winter): function document, this is very interesting.
    // TODO(Winter): We can support the SHOW FUNCTION DOCUMENT `function_name` or MAN FUNCTION `function_name` query syntax.
}

impl AggregateFunction2Description {
    pub fn creator(creator: AggregateFunction2Creator) -> AggregateFunction2Description {
        AggregateFunction2Description {
            aggregate_function_creator: creator,
        }
    }
}

pub struct Combinator2Description {
    creator: AggregateFunction2CombinatorCreator,
    // TODO(Winter): function document, this is very interesting.
    // TODO(Winter): We can support the SHOW FUNCTION DOCUMENT `function_name` or MAN FUNCTION `function_name` query syntax.
}

impl Combinator2Description {
    pub fn creator(creator: AggregateFunction2CombinatorCreator) -> Combinator2Description {
        Combinator2Description { creator }
    }
}

pub struct AggregateFunction2Factory {
    case_insensitive_desc: HashMap<String, AggregateFunction2Description>,
    case_insensitive_combinator_desc: Vec<(String, Combinator2Description)>,
}

impl AggregateFunction2Factory {
    pub(in crate::aggregates::aggregate_function2_factory) fn create() -> AggregateFunction2Factory
    {
        AggregateFunction2Factory {
            case_insensitive_desc: Default::default(),
            case_insensitive_combinator_desc: Default::default(),
        }
    }

    pub fn instance() -> &'static AggregateFunction2Factory {
        FACTORY.as_ref()
    }

    pub fn register(&mut self, name: &str, desc: AggregateFunction2Description) {
        let case_insensitive_desc = &mut self.case_insensitive_desc;
        case_insensitive_desc.insert(name.to_lowercase(), desc);
    }

    pub fn register_combinator(&mut self, suffix: &str, desc: Combinator2Description) {
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
        let origin = name.as_ref();
        let lowercase_name = origin.to_lowercase();

        let aggregate_functions_map = &self.case_insensitive_desc;
        if let Some(desc) = aggregate_functions_map.get(&lowercase_name) {
            return (desc.aggregate_function_creator)(origin, params, arguments);
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
            origin
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
