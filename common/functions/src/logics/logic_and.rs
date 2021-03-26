// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataValueLogicOperator;

use crate::logics::LogicFunction;
use crate::{FunctionResult, IFunction};

pub struct LogicAndFunction;

impl LogicAndFunction {
    pub fn try_create_func(args: &[Box<dyn IFunction>]) -> FunctionResult<Box<dyn IFunction>> {
        LogicFunction::try_create_func(DataValueLogicOperator::And, args)
    }
}
