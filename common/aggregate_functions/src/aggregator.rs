// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::{AggregateFunctionCtx, IAggreagteFunction, AggregateCountFunction, AggregateMaxFunction, AggregateMinFunction, AggregateSumFunction, AggregateAvgFunction};

use std::sync::Arc;
use indexmap::map::IndexMap;
use common_infallible::RwLock;
use crate::aggregate_function_factory::FactoryFuncRef;

pub struct AggregatorFunction;

impl AggregatorFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("count", AggregateCountFunction::try_create);
        map.insert("min", AggregateMinFunction::try_create);
        map.insert("max", AggregateMaxFunction::try_create);
        map.insert("sum", AggregateSumFunction::try_create);
        map.insert("avg", AggregateAvgFunction::try_create);
        Ok(())
    }
}
