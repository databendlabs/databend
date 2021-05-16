// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataValueArithmeticOperator;
use common_exception::Result;

use crate::arithmetics::ArithmeticFunction;
use crate::{IFunction, FunctionCtx};
use std::sync::Arc;

pub struct ArithmeticMinusFunction;

impl ArithmeticMinusFunction {
    pub fn try_create_func(
        _display_name: &str,
        ctx: Arc<dyn FunctionCtx>
    ) -> Result<Box<dyn IFunction>> {
        ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Minus, ctx)
    }
}
