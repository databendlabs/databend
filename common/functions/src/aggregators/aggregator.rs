// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;

use crate::aggregators::AggregatorAvgFunction;
use crate::aggregators::AggregatorCountFunction;
use crate::aggregators::AggregatorMaxFunction;
use crate::aggregators::AggregatorMinFunction;
use crate::aggregators::AggregatorSumFunction;
use crate::FactoryFuncRef;

pub struct AggregatorFunction;

impl AggregatorFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("count", AggregatorCountFunction::try_create);
        map.insert("min", AggregatorMinFunction::try_create);
        map.insert("max", AggregatorMaxFunction::try_create);
        map.insert("sum", AggregatorSumFunction::try_create);
        map.insert("avg", AggregatorAvgFunction::try_create);
        Ok(())
    }
}
