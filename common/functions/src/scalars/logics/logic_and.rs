// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataValueLogicOperator;
use common_exception::Result;

use crate::scalars::Function;
use crate::scalars::LogicFunction;

pub struct LogicAndFunction;

impl LogicAndFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        LogicFunction::try_create_func(DataValueLogicOperator::And)
    }
}
