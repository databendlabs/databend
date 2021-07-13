// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataField;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::RwLock;
use indexmap::IndexMap;
use lazy_static::lazy_static;

use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::Aggregators;

pub struct AggregateFunctionFactory;
pub type FactoryFunc = fn(name: &str, arguments: Vec<DataField>) -> Result<AggregateFunctionRef>;

pub type FactoryCombinatorFunc = fn(
    name: &str,
    arguments: Vec<DataField>,
    nested_func: FactoryFunc,
) -> Result<AggregateFunctionRef>;

pub type FactoryFuncRef = Arc<RwLock<IndexMap<&'static str, FactoryFunc>>>;
pub type FactoryCombinatorFuncRef = Arc<RwLock<IndexMap<&'static str, FactoryCombinatorFunc>>>;

lazy_static! {
    static ref FACTORY: FactoryFuncRef = {
        let map: FactoryFuncRef = Arc::new(RwLock::new(IndexMap::new()));
        Aggregators::register(map.clone()).unwrap();

        map
    };
    static ref COMBINATOR_FACTORY: FactoryCombinatorFuncRef = {
        let map: FactoryCombinatorFuncRef = Arc::new(RwLock::new(IndexMap::new()));
        Aggregators::register_combinator(map.clone()).unwrap();
        map
    };
}

impl AggregateFunctionFactory {
    pub fn get(name: &str, arguments: Vec<DataField>) -> Result<AggregateFunctionRef> {
        let not_found_error = || -> ErrorCode {
            ErrorCode::UnknownAggregateFunction(format!("Unsupported AggregateFunction: {}", name))
        };

        let lower_name = name.to_lowercase();

        let map = FACTORY.read();
        match map.get(lower_name.as_str()) {
            Some(creator) => (creator)(name, arguments),
            None => {
                // find suffix
                let combinator = COMBINATOR_FACTORY.read();
                if let Some((&k, &combinator_creator)) =
                    combinator.iter().find(|(&k, _)| lower_name.ends_with(k))
                {
                    let nested_name = lower_name.strip_suffix(k).ok_or_else(not_found_error)?;

                    return map
                        .get(nested_name)
                        .map(|nested_creator| {
                            combinator_creator(nested_name, arguments, *nested_creator)
                        })
                        .unwrap_or_else(|| Err(not_found_error()));
                }

                Err(not_found_error())
            }
        }
    }

    pub fn check(name: &str) -> bool {
        let map = FACTORY.read();

        let lower_name = name.to_lowercase();
        if map.contains_key(lower_name.as_str()) {
            return true;
        }

        // find suffix
        let combinator = COMBINATOR_FACTORY.read();

        for (k, _) in combinator.iter() {
            if let Some(nested_name) = lower_name.strip_suffix(k) {
                if map.contains_key(nested_name) {
                    return true;
                }
            }
        }
        false
    }

    pub fn registered_names() -> Vec<String> {
        let map = FACTORY.read();
        map.keys().into_iter().map(|x| x.to_string()).collect()
    }
}
