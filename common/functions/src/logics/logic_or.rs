// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::datavalues::DataValueLogicOperator;
use crate::logics::LogicFunction;
use crate::{FunctionResult, IFunction};

pub struct LogicOrFunction;

impl LogicOrFunction {
    pub fn try_create_func(args: &[Box<dyn IFunction>]) -> FunctionResult<Box<dyn IFunction>> {
        LogicFunction::try_create_func(DataValueLogicOperator::Or, args)
    }
}
