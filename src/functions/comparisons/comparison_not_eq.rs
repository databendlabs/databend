// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use crate::contexts::FuseQueryContextRef;
use crate::datavalues::DataValueComparisonOperator;
use crate::error::FuseQueryResult;
use crate::functions::comparisons::ComparisonFunction;
use crate::functions::IFunction;

pub struct ComparisonNotEqFunction;

impl ComparisonNotEqFunction {
    pub fn try_create_func(
        _ctx: FuseQueryContextRef,
        args: &[Box<dyn IFunction>],
    ) -> FuseQueryResult<Box<dyn IFunction>> {
        ComparisonFunction::try_create_func(DataValueComparisonOperator::NotEq, args)
    }
}
