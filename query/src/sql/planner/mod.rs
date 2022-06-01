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

use common_ast::parser::error::Backtrace;
use common_ast::parser::parse_sql;
use common_ast::parser::tokenize_sql;
use common_base::infallible::RwLock;
use common_exception::ErrorCode;
use common_exception::Result;
pub use plans::ScalarExpr;

use crate::sessions::QueryContext;
use crate::sql::exec::PipelineBuilder;
use crate::sql::optimizer::optimize;
use crate::sql::optimizer::SExpr;
pub use crate::sql::planner::binder::BindContext;
use crate::sql::planner::binder::Binder;

pub(crate) mod binder;
mod format;
mod metadata;
pub mod plans;
mod semantic;

pub use binder::ColumnBinding;
pub use format::FormatTreeNode;
pub use metadata::ColumnEntry;
pub use metadata::Metadata;
pub use metadata::MetadataRef;
pub use metadata::TableEntry;

use self::plans::Plan;
use crate::pipelines::new::NewPipeline;

pub struct Planner {
    ctx: Arc<QueryContext>,
}

impl Planner {
    pub fn new(ctx: Arc<QueryContext>) -> Self {
        Planner { ctx }
    }

    // pub async fn plan_sql(&mut self, sql: &str) -> Result<(NewPipeline, Vec<NewPipeline>)> {
    //     let (optimized_expr, bind_context, metadata) = self.build_sexpr(sql).await?;
    //     self.build_pipeline(optimized_expr, bind_context, metadata)
    //         .await
    // }

    pub async fn plan_sql(&mut self, sql: &str) -> Result<(Plan, MetadataRef)> {
        // Step 1: parse SQL text into AST
        let tokens = tokenize_sql(sql)?;

        let backtrace = Backtrace::new();
        let stmts = parse_sql(&tokens, &backtrace)?;
        if stmts.len() > 1 {
            return Err(ErrorCode::UnImplement("unsupported multiple statements"));
        }

        // Step 2: bind AST with catalog, and generate a pure logical SExpr
        let metadata = Arc::new(RwLock::new(Metadata::create()));
        let binder = Binder::new(self.ctx.clone(), self.ctx.get_catalogs(), metadata.clone());
        let plan = binder.bind(&stmts[0]).await?;

        // Step 3: optimize the SExpr with optimizers, and generate optimized physical SExpr
        // let optimize_context = OptimizeContext::create_with_bind_context(&bind_result.bind_context);
        let optimized_plan = optimize(plan)?;

        Ok((optimized_plan, metadata.clone()))
    }

    pub async fn build_pipeline(
        &mut self,
        optimized_expr: SExpr,
        bind_context: BindContext,
        metadata: MetadataRef,
    ) -> Result<(NewPipeline, Vec<NewPipeline>)> {
        // Step 4: build executable Pipeline with SExpr
        let result_columns = bind_context.result_columns();
        let pb = PipelineBuilder::new(self.ctx.clone(), result_columns, metadata, optimized_expr);
        pb.spawn()
    }
}
