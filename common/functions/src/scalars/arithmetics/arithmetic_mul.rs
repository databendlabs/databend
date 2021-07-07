// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataValueArithmeticOperator;
use common_exception::Result;

use crate::scalars::ArithmeticFunction;
use crate::scalars::Function;

pub struct ArithmeticMulFunction;

impl ArithmeticMulFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Mul)
    }
}
