// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataValueComparisonOperator;
use common_exception::Result;

use crate::comparisons::ComparisonFunction;
use crate::IFunction;

pub struct ComparisonEqFunction;

impl ComparisonEqFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn IFunction>> {
        ComparisonFunction::try_create_func(DataValueComparisonOperator::Eq)
    }
}
