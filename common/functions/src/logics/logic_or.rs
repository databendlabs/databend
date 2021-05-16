// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataValueLogicOperator;
use common_exception::Result;

use crate::logics::LogicFunction;
use crate::{IFunction, FunctionCtx};
use std::sync::Arc;

pub struct LogicOrFunction;

impl LogicOrFunction {
    pub fn try_create_func(
        _display_name: &str,
        ctx: Arc<dyn FunctionCtx>
    ) -> Result<Box<dyn IFunction>> {
        LogicFunction::try_create_func(DataValueLogicOperator::Or, ctx)
    }
}
