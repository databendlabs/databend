// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataValueComparisonOperator;

use crate::comparisons::ComparisonFunction;
use crate::{FunctionResult, IFunction};

pub struct ComparisonGtEqFunction;

impl ComparisonGtEqFunction {
    pub fn try_create_func(args: &[Box<dyn IFunction>]) -> FunctionResult<Box<dyn IFunction>> {
        ComparisonFunction::try_create_func(DataValueComparisonOperator::GtEq, args)
    }
}
