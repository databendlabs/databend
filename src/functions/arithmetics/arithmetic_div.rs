// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use crate::datavalues::DataValueArithmeticOperator;
use crate::error::FuseQueryResult;
use crate::functions::arithmetics::ArithmeticFunction;
use crate::functions::IFunction;
use crate::sessions::FuseQueryContextRef;

pub struct ArithmeticDivFunction;

impl ArithmeticDivFunction {
    pub fn try_create_func(
        _ctx: FuseQueryContextRef,
        args: &[Box<dyn IFunction>],
    ) -> FuseQueryResult<Box<dyn IFunction>> {
        ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Div, args)
    }
}
