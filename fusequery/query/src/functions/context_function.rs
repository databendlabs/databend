// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use anyhow::bail;
use anyhow::Result;
use common_datavalues::DataValue;
use common_functions::FunctionFactory;
use common_planners::ExpressionPlan;

use crate::sessions::FuseQueryContextRef;

pub struct ContextFunction;

impl ContextFunction {
    // Some function args need from context
    // such as `SELECT database()`, the arg is ctx.get_default_db()
    pub fn build_args_from_ctx(
        name: &str,
        ctx: FuseQueryContextRef
    ) -> Result<Vec<ExpressionPlan>> {
        // Check the function is supported in common functions.
        if !FunctionFactory::check(name) {
            bail!("Unsupported function: {:?}", name);
        }

        Ok(match name.to_lowercase().as_str() {
            "database" => vec![ExpressionPlan::Literal(DataValue::Utf8(Some(
                ctx.get_default_db()?
            )))],
            _ => vec![]
        })
    }
}
