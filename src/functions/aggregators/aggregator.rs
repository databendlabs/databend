// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use crate::error::FuseQueryResult;
use crate::functions::aggregators::{
    AggregatorAvgFunction, AggregatorCountFunction, AggregatorMaxFunction, AggregatorMinFunction,
    AggregatorSumFunction,
};
use crate::functions::FactoryFuncRef;

pub struct AggregatorFunction;

impl AggregatorFunction {
    pub fn register(map: FactoryFuncRef) -> FuseQueryResult<()> {
        let mut map = map.as_ref().lock()?;
        map.insert("count", AggregatorCountFunction::try_create);
        map.insert("min", AggregatorMinFunction::try_create);
        map.insert("max", AggregatorMaxFunction::try_create);
        map.insert("sum", AggregatorSumFunction::try_create);
        map.insert("avg", AggregatorAvgFunction::try_create);
        Ok(())
    }
}
