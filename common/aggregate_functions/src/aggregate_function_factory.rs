// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::ErrorCodes;
use common_exception::Result;
use common_infallible::RwLock;
use indexmap::IndexMap;
use lazy_static::lazy_static;

use crate::aggregator::AggregatorFunction;
use crate::{IAggreagteFunction, AggregateFunctionCtx};

pub struct AggregateFunctionFactory;
pub type FactoryFunc = fn(name: &str, ctx: Arc<dyn AggregateFunctionCtx>) -> Result<Box<dyn IAggreagteFunction>>;

pub type FactoryFuncRef = Arc<RwLock<IndexMap<&'static str, FactoryFunc>>>;

lazy_static! {
    static ref FACTORY: FactoryFuncRef = {
        let map: FactoryFuncRef = Arc::new(RwLock::new(IndexMap::new()));
        AggregatorFunction::register(map.clone()).unwrap();

        map
    };
}

impl AggregateFunctionFactory {
    pub fn get(name: &str, ctx: Arc<dyn AggregateFunctionCtx>) -> Result<Box<dyn IAggreagteFunction>> {
        let map = FACTORY.read();
        let creator = map.get(&*name.to_lowercase()).ok_or_else(|| {
            ErrorCodes::UnknownFunction(format!("Unsupported AggregateFunction: {}", name))
        })?;
        (creator)(name, ctx)
    }

    pub fn check(name: &str) -> bool {
        let map = FACTORY.read();
        map.contains_key(&*name.to_lowercase())
    }

    pub fn registered_names() -> Vec<String> {
        let map = FACTORY.read();
        map.keys().into_iter().map(|x| x.to_string()).collect()
    }
}
