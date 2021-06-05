// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataValueLogicOperator;
use common_exception::Result;

use crate::logics::LogicFunction;
use crate::Function;

pub struct LogicNotFunction;

impl LogicNotFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        LogicFunction::try_create_func(DataValueLogicOperator::Not)
    }
}
