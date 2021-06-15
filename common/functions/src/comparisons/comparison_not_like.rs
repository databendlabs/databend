// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataValueComparisonOperator;
use common_exception::Result;

use crate::comparisons::ComparisonFunction;
use crate::Function;

pub struct ComparisonNotLikeFunction;

impl ComparisonNotLikeFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        ComparisonFunction::try_create_func(DataValueComparisonOperator::NotLike)
    }
}
