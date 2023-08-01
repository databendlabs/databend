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

use std::sync::Arc;

use common_ast::ast::Expr;
use common_ast::ast::TableReference;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::ROW_ID_COL_NAME;

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
use crate::BindContext;
use crate::ScalarExpr;

impl<'a> Binder {
    pub(in crate::planner::binder) async fn process_selection(
        &self,
        filter: &'a Option<Expr>,
        table_expr: SExpr,
        scalar_binder: &mut ScalarBinder<'_>,
    ) -> Result<(Option<ScalarExpr>, Vec<SubqueryDesc>)> {
        Ok(if let Some(expr) = filter {
            let (scalar, _) = scalar_binder.bind(expr).await?;
            let mut subquery_desc = vec![];
            self.subquery_desc(&scalar, table_expr, &mut subquery_desc)
                .await?;
            (Some(scalar), subquery_desc)
        } else {
            (None, vec![])
        })
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_delete(
        &mut self,
        bind_context: &mut BindContext,
        table_reference: &'a TableReference,
        filter: &'a Option<Expr>,
    ) -> Result<Plan> {
        let (catalog_name, database_name, table_name) = if let TableReference::Table {
            catalog,
            database,
            table,
            ..
        } = table_reference
        {
            self.normalize_object_identifier_triple(catalog, database, table)
        } else {
            // we do not support USING clause yet
            return Err(ErrorCode::Internal(
                "should not happen, parser should have report error already",
            ));
        };

        let (table_expr, mut context) = self
            .bind_table_reference(bind_context, table_reference)
            .await?;

        let mut scalar_binder = ScalarBinder::new(
            &mut context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            self.m_cte_bound_ctx.clone(),
        );

        let (selection, subquery_desc) = self
            .process_selection(filter, table_expr, &mut scalar_binder)
            .await?;

        let plan = DeletePlan {
            catalog_name,
            database_name,
            table_name,
            metadata: self.metadata.clone(),
            selection,
            subquery_desc,
        };
        Ok(Plan::Delete(Box::new(plan)))
    }
}

impl Binder {
    #[async_backtrace::framed]
    async fn process_subquery(
        &self,
        scalar: &ScalarExpr,
        subquery_expr: &SubqueryExpr,
        mut table_expr: SExpr,
    ) -> Result<SubqueryDesc> {
        if subquery_expr.data_type() != DataType::Nullable(Box::new(DataType::Boolean)) {
            return Err(ErrorCode::from_string(
                "subquery data type in delete statement should be boolean".to_string(),
            ));
        }
        let mut outer_columns = Default::default();
        if let Some(child_expr) = &subquery_expr.child_expr {
            outer_columns = child_expr.used_columns();
        };
        outer_columns.extend(subquery_expr.outer_columns.iter());

        let filter = Filter {
            predicates: vec![scalar.clone()],
            is_having: false,
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
        // Add row_id column to scan's column set
        scan.columns.insert(row_id_index.unwrap());
        table_expr.plan = Arc::new(Scan(scan));
        let filter_expr = SExpr::create_unary(Arc::new(filter.into()), Arc::new(table_expr));
        let mut rewriter = SubqueryRewriter::new(self.metadata.clone());
        let filter_expr = rewriter.rewrite(&filter_expr)?;

        Ok(SubqueryDesc {
            input_expr: filter_expr,
            index: row_id_index.unwrap(),
            outer_columns,
        })
    }

    // The method will find all subquery in filter
    #[async_recursion::async_recursion]
    #[async_backtrace::framed]
    async fn subquery_desc(
        &self,
        scalar: &ScalarExpr,
        table_expr: SExpr,
        subquery_desc: &mut Vec<SubqueryDesc>,
    ) -> Result<()> {
        match scalar {
            ScalarExpr::FunctionCall(scalar) => {
                for arg in scalar.arguments.iter() {
                    self.subquery_desc(arg, table_expr.clone(), subquery_desc)
                        .await?;
                }
            }
            ScalarExpr::CastExpr(scalar) => {
                self.subquery_desc(scalar.argument.as_ref(), table_expr, subquery_desc)
                    .await?;
            }
            ScalarExpr::SubqueryExpr(subquery) => {
                let desc = self
                    .process_subquery(scalar, subquery, table_expr.clone())
                    .await?;
                subquery_desc.push(desc);
            }
            ScalarExpr::BoundColumnRef(_)
            | ScalarExpr::ConstantExpr(_)
            | ScalarExpr::WindowFunction(_)
            | ScalarExpr::AggregateFunction(_)
            | ScalarExpr::LambdaFunction(_) => {}
        }
        Ok(())
    }
}
