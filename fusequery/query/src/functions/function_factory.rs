// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::{Arc, Mutex};

use indexmap::IndexMap;
use lazy_static::lazy_static;

use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::aggregators::AggregatorFunction;
use crate::functions::arithmetics::ArithmeticFunction;
use crate::functions::comparisons::ComparisonFunction;
use crate::functions::logics::LogicFunction;
use crate::functions::udfs::UdfFunction;
use crate::functions::IFunction;
use crate::sessions::FuseQueryContextRef;

pub struct FunctionFactory;
pub type FactoryFunc = fn(
    ctx: FuseQueryContextRef,
    args: &[Box<dyn IFunction>],
) -> FuseQueryResult<Box<dyn IFunction>>;
pub type FactoryFuncRef = Arc<Mutex<IndexMap<&'static str, FactoryFunc>>>;

lazy_static! {
    static ref FACTORY: FactoryFuncRef = {
        let map: FactoryFuncRef = Arc::new(Mutex::new(IndexMap::new()));
        AggregatorFunction::register(map.clone()).unwrap();
        ArithmeticFunction::register(map.clone()).unwrap();
        ComparisonFunction::register(map.clone()).unwrap();
        LogicFunction::register(map.clone()).unwrap();
        UdfFunction::register(map.clone()).unwrap();
        map
    };
}

impl FunctionFactory {
    pub fn get(
        ctx: FuseQueryContextRef,
        name: &str,
        args: &[Box<dyn IFunction>],
    ) -> FuseQueryResult<Box<dyn IFunction>> {
        let map = FACTORY.as_ref().lock()?;
        let creator = map.get(&*name.to_lowercase()).ok_or_else(|| {
            FuseQueryError::build_internal_error(format!("Unsupported Function: {}", name))
        })?;
        (creator)(ctx, args)
    }

    pub fn registered_names() -> Vec<String> {
        let map = FACTORY.as_ref().lock().unwrap();
        map.keys().into_iter().map(|x| x.to_string()).collect()
    }
}
