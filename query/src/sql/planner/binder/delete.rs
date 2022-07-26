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

use std::collections::HashSet;

use common_ast::ast::Expr;
use common_ast::ast::TableReference;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::DeletePlan;
use common_planners::Expression;

use crate::sessions::TableContext;
use crate::sql::binder::Binder;
use crate::sql::binder::ScalarBinder;
use crate::sql::executor::ExpressionBuilderWithoutRenaming;
use crate::sql::plans::Plan;
use crate::sql::statements::query::QueryASTIRVisitor;
use crate::sql::BindContext;

pub struct DeleteCollectPushDowns {}
impl QueryASTIRVisitor<HashSet<String>> for DeleteCollectPushDowns {
    fn visit_expr(expr: &mut Expression, require_columns: &mut HashSet<String>) -> Result<()> {
        if let Expression::Column(name) = expr {
            if !require_columns.contains(name) {
                require_columns.insert(name.clone());
            }
        }

        Ok(())
    }

    fn visit_filter(predicate: &mut Expression, data: &mut HashSet<String>) -> Result<()> {
        Self::visit_recursive_expr(predicate, data)
    }
}

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

        let mut scalar_binder =
            ScalarBinder::new(&context, self.ctx.clone(), self.metadata.clone());

        let mut expression = None;
        let mut require_columns: HashSet<String> = HashSet::new();
        if let Some(expr) = selection {
            let (scalar, _) = scalar_binder.bind(expr).await?;
            let eb = ExpressionBuilderWithoutRenaming::create(self.metadata.clone());
            let mut pred_expr = eb.build(&scalar)?;
            DeleteCollectPushDowns::visit_filter(&mut pred_expr, &mut require_columns)?;
            expression = Some(pred_expr);
        }

        let table = self
            .ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await?;

        let tbl_info = table.get_table_info();
        let table_id = tbl_info.ident.clone();
        let mut projection = vec![];
        let schema = tbl_info.meta.schema.as_ref();
        for col_name in require_columns {
            if let Some((idx, _)) = schema.column_with_name(col_name.as_str()) {
                projection.push(idx);
            } else {
                return Err(ErrorCode::UnknownColumn(format!(
                    "Column [{}] not found",
                    col_name
                )));
            }
        }

        let plan = DeletePlan {
            catalog_name,
            database_name,
            table_name,
            table_id,
            selection: expression,
            projection,
        };
        Ok(Plan::Delete(Box::new(plan)))
    }
}
