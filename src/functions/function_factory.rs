// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use indexmap::IndexMap;
use lazy_static::lazy_static;
use std::sync::{Arc, Mutex};

use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::aggregators::AggregatorFunction;
use crate::functions::arithmetics::ArithmeticFunction;
use crate::functions::comparisons::ComparisonFunction;
use crate::functions::logics::LogicFunction;
use crate::functions::udfs::UDFFunction;
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
        UDFFunction::register(map.clone()).unwrap();
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
        let creator = map
            .get(&*name.to_lowercase())
            .ok_or_else(|| FuseQueryError::Internal(format!("Unsupported Function: {}", name)))?;
        (creator)(ctx, args)
    }

    pub fn registered_names() -> Vec<String> {
        let map = FACTORY.as_ref().lock().unwrap();
        map.keys().into_iter().map(|x| x.to_string()).collect()
    }
}
