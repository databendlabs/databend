// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::contexts::FuseQueryContextRef;
use crate::datavalues::DataValueLogicOperator;
use crate::error::FuseQueryResult;
use crate::functions::logics::LogicFunction;
use crate::functions::IFunction;

pub struct LogicOrFunction;

impl LogicOrFunction {
    pub fn try_create_func(
        _ctx: FuseQueryContextRef,
        args: &[Box<dyn IFunction>],
    ) -> FuseQueryResult<Box<dyn IFunction>> {
        LogicFunction::try_create_func(DataValueLogicOperator::Or, args)
    }
}
