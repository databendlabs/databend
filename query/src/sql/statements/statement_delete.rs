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

use common_datavalues::DataSchema;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::DeletePlan;
use common_planners::Expression;
use common_planners::PlanNode;
use common_tracing::tracing;
use sqlparser::ast::Expr;
use sqlparser::ast::ObjectName;

use crate::sessions::QueryContext;
use crate::sql::statements::resolve_table;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::statements::QueryRelation;
use crate::storages::view::view_table::VIEW_ENGINE;

#[derive(Debug, Clone, PartialEq)]
pub struct DfDeleteStatement {
    pub name: ObjectName,
    pub selection: Option<Expr>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfDeleteStatement {
    #[tracing::instrument(level = "debug", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let (catalog_name, database_name, table_name) =
            resolve_table(&ctx, &self.name, "DELETE from TABLE")?;

        let table = ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await?;
        let tbl_info = table.get_table_info();
        if tbl_info.engine() == VIEW_ENGINE {
            return Err(ErrorCode::SemanticError("Delete from view not allowed"));
        }

        let table_id = tbl_info.ident.clone();

        // Parallel / Distributed execution of deletion not supported before
        // the new parser, new planner and new pipeline are settled down.

        Ok(AnalyzedResult::SimpleQuery(Box::new(PlanNode::Delete(
            DeletePlan {
                catalog_name,
                database_name,
                table_name,
                table_id,
            },
        ))))
    }
}

#[derive(Clone, Default)]
pub struct DeleteAnalyzeState {
    pub filter: Option<Expression>,
    pub relation: QueryRelation,
    pub schema: Arc<DataSchema>,
}
