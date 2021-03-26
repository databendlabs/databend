// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataValueComparisonOperator;

use crate::comparisons::ComparisonFunction;
use crate::{FunctionResult, IFunction};

pub struct ComparisonEqFunction;

impl ComparisonEqFunction {
    pub fn try_create_func(args: &[Box<dyn IFunction>]) -> FunctionResult<Box<dyn IFunction>> {
        ComparisonFunction::try_create_func(DataValueComparisonOperator::Eq, args)
    }
}
