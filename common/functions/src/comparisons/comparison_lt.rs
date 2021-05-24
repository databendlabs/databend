// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataValueComparisonOperator;
use common_exception::Result;

use crate::comparisons::ComparisonFunction;
use crate::IFunction;

pub struct ComparisonLtFunction;

impl ComparisonLtFunction {
    pub fn try_create_func(
        _display_name: &str,
        args: &[Box<dyn IFunction>],
    ) -> Result<Box<dyn IFunction>> {
        ComparisonFunction::try_create_func(DataValueComparisonOperator::Lt, args)
    }
}
