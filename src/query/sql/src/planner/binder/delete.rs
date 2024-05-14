// Copyright 2021 Datafuse Labs
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
use std::sync::Arc;

use databend_common_ast::ast::DeleteStmt;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::TableReference;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ROW_ID_COL_NAME;

use crate::binder::Binder;
use crate::binder::ScalarBinder;
use crate::binder::INTERNAL_COLUMN_FACTORY;
use crate::optimizer::SExpr;
use crate::optimizer::SubqueryRewriter;
use crate::plans::DeletePlan;
use crate::plans::Filter;
use crate::plans::Operator;
use crate::plans::Plan;
use crate::plans::RelOp;
use crate::plans::RelOperator::Scan;
use crate::plans::SubqueryDesc;
use crate::plans::SubqueryExpr;
use crate::plans::Visitor;
use crate::BindContext;
use crate::ColumnEntry;
use crate::ScalarExpr;

impl<'a> Binder {
    pub(in crate::planner::binder) async fn process_selection(
        &self,
        filter: &'a Option<Expr>,
        table_expr: SExpr,
        scalar_binder: &mut ScalarBinder<'_>,
    ) -> Result<(Option<ScalarExpr>, Option<SubqueryDesc>)> {
        Ok(if let Some(expr) = filter {
            let (scalar, _) = scalar_binder.bind(expr).await?;
            if !self.has_subquery_in_selection(&scalar)? {
                return Ok((Some(scalar), None));
            }
            let subquery_desc = self.process_subquery(scalar.clone(), table_expr).await?;
            (Some(scalar), Some(subquery_desc))
        } else {
            (None, None)
        })
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_delete(
        &mut self,
        bind_context: &mut BindContext,
        stamt: &DeleteStmt,
    ) -> Result<Plan> {
        let DeleteStmt {
            table,
            selection,
            with,
            ..
        } = stamt;

        if let Some(with) = &with {
            self.add_cte(with, bind_context)?;
        }

        let (catalog_name, database_name, table_name) = if let TableReference::Table {
            catalog,
            database,
            table,
            ..
        } = table
        {
            self.normalize_object_identifier_triple(catalog, database, table)
        } else {
            // we do not support USING clause yet
            return Err(ErrorCode::Internal(
                "should not happen, parser should have report error already",
            ));
        };

        let (table_expr, mut context) = self.bind_single_table(bind_context, table).await?;

        context.allow_internal_columns(false);
        let mut scalar_binder = ScalarBinder::new(
            &mut context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            self.m_cte_bound_ctx.clone(),
            self.ctes_map.clone(),
        );

        let (selection, subquery_desc) = self
            .process_selection(selection, table_expr, &mut scalar_binder)
            .await?;

        if let Some(selection) = &selection {
            if !self.check_allowed_scalar_expr_with_subquery(selection)? {
                return Err(ErrorCode::SemanticError(
                    "selection in delete statement can't contain window|aggregate|udf functions"
                        .to_string(),
                )
                .set_span(selection.span()));
            }
        }

        let plan = DeletePlan {
            catalog_name,
            database_name,
            table_name,
            metadata: self.metadata.clone(),
            bind_context: Box::new(context.clone()),
            selection,
            subquery_desc,
        };
        Ok(Plan::Delete(Box::new(plan)))
    }
}

impl Binder {
    // The method will find all subquery in filter
    fn has_subquery_in_selection(&self, scalar: &ScalarExpr) -> Result<bool> {
        struct FindSubqueryVisitor {
            found_subquery: bool,
        }

        impl<'a> Visitor<'a> for FindSubqueryVisitor {
            fn visit_subquery(&mut self, _subquery: &'a SubqueryExpr) -> Result<()> {
                self.found_subquery = true;
                Ok(())
            }
        }

        let mut find_subquery = FindSubqueryVisitor {
            found_subquery: false,
        };
        find_subquery.visit(scalar)?;

        Ok(find_subquery.found_subquery)
    }

    #[async_backtrace::framed]
    async fn process_subquery(
        &self,
        predicate: ScalarExpr,
        mut table_expr: SExpr,
    ) -> Result<SubqueryDesc> {
        let mut outer_columns: HashSet<usize> = Default::default();

        let filter = Filter {
            predicates: vec![predicate],
        };
        debug_assert_eq!(table_expr.plan.rel_op(), RelOp::Scan);
        let mut scan = match &*table_expr.plan {
            Scan(scan) => scan.clone(),
            _ => unreachable!(),
        };
        // Check if metadata contains row_id column
        let mut row_id_index = None;
        for col in self
            .metadata
            .read()
            .columns_by_table_index(scan.table_index)
            .iter()
        {
            if col.name() == ROW_ID_COL_NAME {
                row_id_index = Some(col.index());
                break;
            }
        }
        if row_id_index.is_none() {
            // Add row_id column to metadata
            let internal_column = INTERNAL_COLUMN_FACTORY
                .get_internal_column(ROW_ID_COL_NAME)
                .unwrap();
            row_id_index = Some(
                self.metadata
                    .write()
                    .add_internal_column(scan.table_index, internal_column),
            );
        }

        // add all table columns into outer columns
        let metadata = self.metadata.read();
        let columns = metadata.columns_by_table_index(scan.table_index);
        for column in columns {
            if let ColumnEntry::BaseTableColumn(column) = &column {
                outer_columns.insert(column.column_index);
            }
        }

        // Add row_id column to scan's column set
        scan.columns.insert(row_id_index.unwrap());
        table_expr.plan = Arc::new(Scan(scan));
        let filter_expr = SExpr::create_unary(Arc::new(filter.into()), Arc::new(table_expr));
        let mut rewriter = SubqueryRewriter::new(self.ctx.clone(), self.metadata.clone());
        let filter_expr = rewriter.rewrite(&filter_expr)?;

        Ok(SubqueryDesc {
            input_expr: filter_expr,
            index: row_id_index.unwrap(),
            outer_columns,
        })
    }
}
