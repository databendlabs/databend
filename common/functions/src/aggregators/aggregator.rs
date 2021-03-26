// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::aggregators::{
    AggregatorAvgFunction, AggregatorCountFunction, AggregatorMaxFunction, AggregatorMinFunction,
    AggregatorSumFunction,
};
use crate::{FactoryFuncRef, FunctionResult};

pub struct AggregatorFunction;

impl AggregatorFunction {
    pub fn register(map: FactoryFuncRef) -> FunctionResult<()> {
        let mut map = map.as_ref().lock()?;
        map.insert("count", AggregatorCountFunction::try_create);
        map.insert("min", AggregatorMinFunction::try_create);
        map.insert("max", AggregatorMaxFunction::try_create);
        map.insert("sum", AggregatorSumFunction::try_create);
        map.insert("avg", AggregatorAvgFunction::try_create);
        Ok(())
    }
}
