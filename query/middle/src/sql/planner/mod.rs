// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use common_ast::parser::Parser;
use common_exception::ErrorCode;
use common_exception::Result;
pub use metadata::*;
use query_sql::optimizer::optimize;
use query_sql::optimizer::OptimizeContext;
pub use query_sql::planner::plan::*;
pub use query_sql::planner::scalar::*;

use crate::pipelines::processors::Pipeline;
use crate::sessions::QueryContext;
use crate::sql::exec::Executor;
use crate::sql::planner::binder::Binder;

mod binder;
mod metadata;

pub struct Planner {
    context: Arc<QueryContext>,
}

impl Planner {
    pub fn new(context: Arc<QueryContext>) -> Self {
        Planner { context }
    }

    pub async fn plan_sql(&mut self, sql: &str) -> Result<Pipeline> {
        // Step 1: parse SQL text into AST
        let parser = Parser {};
        let stmts = parser.parse_with_sqlparser(sql)?;
        if stmts.len() > 1 {
            return Err(ErrorCode::UnImplement("unsupported multiple statements"));
        }

        // Step 2: bind AST with catalog, and generate a pure logical SExpr
        let binder = Binder::new(self.context.get_catalog(), self.context.clone());
        let bind_result = binder.bind(&stmts[0]).await?;

        // Step 3: optimize the SExpr with optimizers, and generate optimized physical SExpr
        let optimize_context = OptimizeContext::create_with_bind_context(&bind_result.bind_context);
        let optimized_expr = optimize(bind_result.s_expr().clone(), optimize_context)?;

        // Step 4: build executable Pipeline with SExpr
        let exec = Executor::create(self.context.clone(), bind_result.metadata);
        let pipeline = exec.build_pipeline(&optimized_expr).await?;

        Ok(pipeline)
    }
}
