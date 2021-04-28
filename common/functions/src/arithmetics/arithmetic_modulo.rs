// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use common_datavalues::DataValueArithmeticOperator;

use crate::arithmetics::ArithmeticFunction;
use crate::IFunction;

pub struct ArithmeticModuloFunction;

impl ArithmeticModuloFunction {
    pub fn try_create_func(args: &[Box<dyn IFunction>]) -> Result<Box<dyn IFunction>> {
        ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Modulo, args)
    }
}
