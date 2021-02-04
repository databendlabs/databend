// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::contexts::FuseQueryContextRef;
use crate::datavalues::DataValueArithmeticOperator;
use crate::error::FuseQueryResult;
use crate::functions::arithmetics::ArithmeticFunction;
use crate::functions::IFunction;

pub struct ArithmeticSubFunction;

impl ArithmeticSubFunction {
    pub fn try_create_func(
        _ctx: FuseQueryContextRef,
        args: &[Box<dyn IFunction>],
    ) -> FuseQueryResult<Box<dyn IFunction>> {
        ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Sub, args)
    }
}
