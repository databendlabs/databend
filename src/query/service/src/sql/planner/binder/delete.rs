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

use common_ast::ast::Expr;
use common_ast::ast::TableReference;
use common_exception::ErrorCode;
use common_exception::Result;
use common_legacy_planners::DeletePlan;
use common_legacy_planners::Projection;

use crate::sql::binder::Binder;
use crate::sql::binder::ScalarBinder;
use crate::sql::plans::Plan;
use crate::sql::BindContext;
use crate::sql::ScalarExpr;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_delete(
        &mut self,
        bind_context: &BindContext,
        table_reference: &'a TableReference<'a>,
        selection: &'a Option<Expr<'a>>,
    ) -> Result<Plan> {
        let (catalog_name, database_name, table_name) = if let TableReference::Table {
            catalog,
            database,
            table,
            ..
        } = table_reference
        {
            (
                catalog
                    .as_ref()
                    .map_or_else(|| self.ctx.get_current_catalog(), |i| i.name.clone()),
                database
                    .as_ref()
                    .map_or_else(|| self.ctx.get_current_database(), |i| i.name.clone()),
                table.name.clone(),
            )
        } else {
            // we do not support USING clause yet
            return Err(ErrorCode::LogicalError(
                "should not happen, parser should have report error already",
            ));
        };

        let (_, context) = self
            .bind_table_reference(bind_context, table_reference)
            .await?;

        let mut scalar_binder = ScalarBinder::new(
            &context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
        );

        let table = self
            .ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await?;

        let tbl_info = table.get_table_info();
        let table_id = tbl_info.ident;

        // @todo wait delete migrate to new planner
        let col_indices: Vec<usize> = if let Some(expr) = selection {
            let (scalar, _) = scalar_binder.bind(expr).await?;
            scalar.used_columns().into_iter().collect()
        } else {
            vec![]
        };
        let selection = selection.as_ref().map(|expr| format!("({})", expr));
        let projection = Projection::Columns(col_indices);

        let plan = DeletePlan {
            catalog_name,
            database_name,
            table_name,
            table_id,
            selection,
            projection,
        };
        Ok(Plan::Delete(Box::new(plan)))
    }
}
