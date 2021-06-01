// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_aggregate_functions::AggregateFunctionFactory;
use common_datavalues::DataValue;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_functions::FunctionFactory;
use common_planners::Expression;

use crate::sessions::FuseQueryContextRef;

pub struct ContextFunction;

impl ContextFunction {
    // Some function args need from context
    // such as `SELECT database()`, the arg is ctx.get_default_db()
    pub fn build_args_from_ctx(name: &str, ctx: FuseQueryContextRef) -> Result<Vec<Expression>> {
        // Check the function is supported in common functions.
        if !FunctionFactory::check(name) && !AggregateFunctionFactory::check(name) {
            return Result::Err(ErrorCodes::UnknownFunction(format!(
                "Unsupported function: {:?}",
                name
            )));
        }

        Ok(match name.to_lowercase().as_str() {
            "database" => vec![Expression::Literal(DataValue::Utf8(Some(
                ctx.get_current_database(),
            )))],
            _ => vec![],
        })
    }
}
