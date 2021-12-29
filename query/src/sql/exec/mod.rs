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

mod data_schema_helper;
mod expression_builder;
mod util;

use std::sync::Arc;

use async_recursion::async_recursion;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;
use common_planners::ReadDataSourcePlan;

use crate::pipelines::processors::Pipeline;
use crate::pipelines::transforms::ProjectionTransform;
use crate::pipelines::transforms::SourceTransform;
use crate::sessions::QueryContext;
use crate::sql::exec::data_schema_helper::DataSchemaHelper;
use crate::sql::exec::expression_builder::ExpressionBuilder;
use crate::sql::exec::util::check_physical;
use crate::sql::optimizer::SExpr;
use crate::sql::Metadata;
use crate::sql::PhysicalProject;
use crate::sql::PhysicalScan;
use crate::sql::Plan;

/// Helper to build a `Pipeline` from `SExpr`
pub struct Executor {
    ctx: Arc<QueryContext>,
    metadata: Metadata,
}

impl Executor {
    pub fn create(ctx: Arc<QueryContext>, metadata: Metadata) -> Self {
        Executor { ctx, metadata }
    }

    #[async_recursion(? Send)]
    pub async fn build_pipeline(&self, expression: &SExpr) -> Result<Pipeline> {
        if !check_physical(expression) {
            return Err(ErrorCode::LogicalError(format!(
                "Invalid physical plan: {:?}",
                expression
            )));
        }

        match expression.plan().as_ref() {
            Plan::PhysicalScan(scan) => self.build_table_scan(scan).await,
            Plan::PhysicalProject(project) => {
                self.build_project(project, expression.children().as_slice())
                    .await
            }
            _ => Err(ErrorCode::LogicalError(format!(
                "Invalid physical plan: {:?}",
                expression
            ))),
        }
    }

    async fn build_project(
        &self,
        project: &PhysicalProject,
        children: &[SExpr],
    ) -> Result<Pipeline> {
        let child = &children[0];
        let input_schema = Arc::new(DataSchemaHelper::build(child, &self.metadata)?);
        let output_schema = Arc::new(DataSchemaHelper::build_project(
            project,
            child,
            &self.metadata,
        )?);

        let mut exprs = vec![];
        for item in project.items.iter() {
            let alias = self.metadata.column(item.index).name.clone();
            let builder = ExpressionBuilder::create(&self.metadata);
            let expr = builder.build(&item.expr)?;
            let expr = Expression::Alias(alias, Box::new(expr));
            exprs.push(expr);
        }

        let mut pipeline = self.build_pipeline(child).await?;
        pipeline.add_simple_transform(|| {
            Ok(Box::new(ProjectionTransform::try_create(
                input_schema.clone(),
                output_schema.clone(),
                exprs.clone(),
            )?))
        })?;
        Ok(pipeline)
    }

    async fn build_table_scan(&self, scan: &PhysicalScan) -> Result<Pipeline> {
        let table = self.metadata.table(scan.table_index).table.clone();
        let (statistics, parts) = table.read_partitions(self.ctx.clone(), None).await?;
        let plan = ReadDataSourcePlan {
            table_info: table.get_table_info().clone(),
            scan_fields: None,
            parts,
            statistics,
            description: "".to_string(),
            tbl_args: table.table_args(),
            push_downs: None,
        };

        // Bind plan partitions to context.
        self.ctx.try_set_partitions(plan.parts.clone())?;

        let mut pipeline = Pipeline::create(self.ctx.clone());
        let max_threads = self.ctx.get_settings().get_max_threads()? as usize;
        let max_threads = std::cmp::min(max_threads, plan.parts.len());
        let workers = std::cmp::max(max_threads, 1);

        for _ in 0..workers {
            let source = SourceTransform::try_create(self.ctx.clone(), plan.clone())?;
            pipeline.add_source(Arc::new(source))?;
        }
        Ok(pipeline)
    }
}
