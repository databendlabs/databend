// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::anyhow;
use anyhow::Result;
use common_infallible::RwLock;
use indexmap::IndexMap;
use lazy_static::lazy_static;

use crate::aggregators::AggregatorFunction;
use crate::arithmetics::ArithmeticFunction;
use crate::comparisons::ComparisonFunction;
use crate::logics::LogicFunction;
use crate::udfs::UdfFunction;
use crate::IFunction;

pub struct FunctionFactory;
pub type FactoryFunc = fn(args: &[Box<dyn IFunction>]) -> Result<Box<dyn IFunction>>;
pub type FactoryFuncRef = Arc<RwLock<IndexMap<&'static str, FactoryFunc>>>;

lazy_static! {
    static ref FACTORY: FactoryFuncRef = {
        let map: FactoryFuncRef = Arc::new(RwLock::new(IndexMap::new()));
        AggregatorFunction::register(map.clone()).unwrap();
        ArithmeticFunction::register(map.clone()).unwrap();
        ComparisonFunction::register(map.clone()).unwrap();
        LogicFunction::register(map.clone()).unwrap();
        UdfFunction::register(map.clone()).unwrap();
        map
    };
}

impl FunctionFactory {
    pub fn get(name: &str, args: &[Box<dyn IFunction>]) -> Result<Box<dyn IFunction>> {
        let map = FACTORY.read();
        let creator = map
            .get(&*name.to_lowercase())
            .ok_or_else(|| anyhow!("Unsupported Function: {}", name))?;
        (creator)(args)
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
