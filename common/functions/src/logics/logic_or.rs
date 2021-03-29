// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::Result;
use common_datavalues::DataValueLogicOperator;

use crate::logics::LogicFunction;
use crate::IFunction;

pub struct LogicOrFunction;

impl LogicOrFunction {
    pub fn try_create_func(args: &[Box<dyn IFunction>]) -> Result<Box<dyn IFunction>> {
        LogicFunction::try_create_func(DataValueLogicOperator::Or, args)
    }
}
