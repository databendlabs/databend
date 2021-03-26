// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::arithmetics::ArithmeticFunction;
use crate::common_datavalues::DataValueArithmeticOperator;
use crate::{FunctionResult, IFunction};

pub struct ArithmeticMulFunction;

impl ArithmeticMulFunction {
    pub fn try_create_func(args: &[Box<dyn IFunction>]) -> FunctionResult<Box<dyn IFunction>> {
        ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Mul, args)
    }
}
