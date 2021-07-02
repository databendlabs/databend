// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataValueComparisonOperator;
use common_exception::Result;

use crate::scalars::ComparisonFunction;
use crate::scalars::Function;

pub struct ComparisonEqFunction;

impl ComparisonEqFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        ComparisonFunction::try_create_func(DataValueComparisonOperator::Eq)
    }
}
