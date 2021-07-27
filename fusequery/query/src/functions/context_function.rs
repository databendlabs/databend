// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::AggregateFunctionFactory;
use common_functions::scalars::FunctionFactory;
use common_planners::Expression;

use crate::sessions::FuseQueryContextRef;

pub struct ContextFunction;

impl ContextFunction {
    // Some function args need from context
    // such as `SELECT database()`, the arg is ctx.get_default_db()
    pub fn build_args_from_ctx(name: &str, ctx: FuseQueryContextRef) -> Result<Vec<Expression>> {
        // Check the function is supported in common functions.
        if !FunctionFactory::check(name) && !AggregateFunctionFactory::check(name) {
            return Result::Err(ErrorCode::UnknownFunction(format!(
                "Unsupported function: {:?}",
                name
            )));
        }

        Ok(match name.to_lowercase().as_str() {
            "database" => vec![Expression::create_literal(DataValue::Utf8(Some(
                ctx.get_current_database(),
            )))],
            "version" => vec![Expression::create_literal(DataValue::Utf8(Some(
                ctx.get_fuse_version(),
            )))],
            _ => vec![],
        })
    }
}
