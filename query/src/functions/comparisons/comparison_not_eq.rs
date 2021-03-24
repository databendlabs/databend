// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use fuse_query_datavalues::DataValueComparisonOperator;

use crate::error::FuseQueryResult;
use crate::functions::comparisons::ComparisonFunction;
use crate::functions::IFunction;
use crate::sessions::FuseQueryContextRef;

pub struct ComparisonNotEqFunction;

impl ComparisonNotEqFunction {
    pub fn try_create_func(
        _ctx: FuseQueryContextRef,
        args: &[Box<dyn IFunction>],
    ) -> FuseQueryResult<Box<dyn IFunction>> {
        ComparisonFunction::try_create_func(DataValueComparisonOperator::NotEq, args)
    }
}
