// Copyright 2022 Datafuse Labs.
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

use common_ast::parser::parse_sql;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::sessions::QueryContext;
use crate::sql::exec::PipelineBuilder;
use crate::sql::optimizer::optimize;
use crate::sql::optimizer::OptimizeContext;
pub use crate::sql::planner::binder::BindContext;
use crate::sql::planner::binder::Binder;

mod binder;
mod metadata;
pub mod plans;

pub use metadata::ColumnEntry;
pub use metadata::Metadata;
pub use metadata::TableEntry;

use crate::pipelines::new::NewPipeline;

pub struct Planner {
    context: Arc<QueryContext>,
}

impl Planner {
    pub fn new(context: Arc<QueryContext>) -> Self {
        Planner { context }
    }

    pub async fn plan_sql(&mut self, sql: &str) -> Result<NewPipeline> {
        // Step 1: parse SQL text into AST
        let stmts = parse_sql(sql)?;
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
        let result_columns = bind_result.bind_context.result_columns();
        let pb = PipelineBuilder::new(
            self.context.clone(),
            result_columns,
            bind_result.metadata,
            optimized_expr,
        );
        let pipeline = pb.spawn()?;

        Ok(pipeline)
    }
}
